#!/usr/bin/env python3
"""Prometheus exporter for GitLab Runner job containers (Docker executor).

Subscribes to Docker events for containers created by GitLab Runner, streams
their resource stats while they run, and keeps each container's final numbers
for a while after it exits so short jobs are still scraped.

Works against Windows hosts (docker-windows executor) and Linux hosts.
"""

import argparse
import logging
import re
import signal
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone

import docker
from docker.errors import NotFound
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily

LABEL_PREFIX = "com.gitlab.gitlab-runner."
JOB_ID_LABEL = LABEL_PREFIX + "job.id"

# Prometheus label name -> GitLab Runner container label (without prefix)
RUNNER_LABELS = {
    "job_id": "job.id",
    "job_name": "job.name",
    "job_stage": "job.stage",
    "pipeline_id": "pipeline.id",
    "project_id": "project.id",
    "runner_id": "runner.id",
    "container_type": "type",
}
LABEL_NAMES = [*RUNNER_LABELS, "container_id", "container_name"]

log = logging.getLogger("gitlab-job-exporter")


@dataclass
class ContainerState:
    id: str
    name: str
    image: str
    job_url: str
    label_values: list
    started_at: float
    finished_at: float | None = None
    exit_code: int | None = None
    cpu_seconds: float = 0.0
    memory_bytes: float = 0.0
    memory_peak_bytes: float = 0.0
    memory_commit_peak_bytes: float = 0.0
    disk_read_bytes: float = 0.0
    disk_write_bytes: float = 0.0
    net_rx_bytes: float = 0.0
    net_tx_bytes: float = 0.0


def parse_docker_time(value):
    """Parse Docker's RFC3339Nano UTC timestamps (Python can't do 9-digit fractions)."""
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?", value or "")
    if not m:
        return time.time()
    base = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return base.timestamp() + float(m.group(2) or 0)


def apply_stats(state, s, windows):
    """Update state from one /containers/{id}/stats sample."""
    cpu = (s.get("cpu_stats") or {}).get("cpu_usage", {}).get("total_usage")
    if cpu:
        # Windows reports 100ns units, Linux reports nanoseconds.
        state.cpu_seconds = cpu * (1e-7 if windows else 1e-9)

    mem = s.get("memory_stats") or {}
    if windows:
        current = mem.get("privateworkingset", 0)
        state.memory_commit_peak_bytes = mem.get("commitpeakbytes", 0)
        storage = s.get("storage_stats") or {}
        state.disk_read_bytes = storage.get("read_size_bytes", 0)
        state.disk_write_bytes = storage.get("write_size_bytes", 0)
    else:
        # Working set, same as `docker stats` / kubelet: usage minus page cache.
        extra = mem.get("stats") or {}
        inactive = extra.get("inactive_file", extra.get("total_inactive_file", 0))
        current = max(mem.get("usage", 0) - inactive, 0)
        read = write = 0
        for entry in (s.get("blkio_stats") or {}).get("io_service_bytes_recursive") or []:
            op = entry.get("op", "").lower()
            if op == "read":
                read += entry.get("value", 0)
            elif op == "write":
                write += entry.get("value", 0)
        state.disk_read_bytes, state.disk_write_bytes = read, write

    state.memory_bytes = current
    state.memory_peak_bytes = max(state.memory_peak_bytes, current)

    networks = (s.get("networks") or {}).values()
    state.net_rx_bytes = sum(n.get("rx_bytes", 0) for n in networks)
    state.net_tx_bytes = sum(n.get("tx_bytes", 0) for n in networks)


class JobContainerExporter:
    def __init__(self, client, summary_ttl):
        self.client = client
        self.summary_ttl = summary_ttl
        self.windows = client.info().get("OSType") == "windows"
        self.lock = threading.Lock()
        self.containers = {}
        self.docker_up = 0
        self.events_total = 0
        self.reconnects_total = 0
        self.stats_errors_total = 0

    # --- container lifecycle -------------------------------------------------

    def track(self, cid, name, image, labels, started_at):
        name = name.lstrip("/")
        with self.lock:
            if cid in self.containers:
                return
            state = ContainerState(
                id=cid,
                name=name,
                image=image,
                job_url=labels.get(LABEL_PREFIX + "job.url", ""),
                label_values=[labels.get(LABEL_PREFIX + k, "") for k in RUNNER_LABELS.values()]
                + [cid[:12], name],
                started_at=started_at,
            )
            self.containers[cid] = state
        log.info("tracking %s (job %s)", name, labels.get(JOB_ID_LABEL))
        threading.Thread(
            target=self._stream_stats, args=(state,), daemon=True, name=f"stats-{cid[:12]}"
        ).start()

    def finish(self, cid, finished_at, exit_code=None):
        with self.lock:
            state = self.containers.get(cid)
            if state is None or state.finished_at is not None:
                return
            state.finished_at = finished_at
            state.exit_code = exit_code
        log.info("finished %s exit=%s duration=%.1fs peak_mem=%.0fMiB",
                 state.name, exit_code, finished_at - state.started_at,
                 state.memory_peak_bytes / 2**20)

    def _stream_stats(self, state):
        while True:
            try:
                for s in self.client.api.stats(state.id, stream=True, decode=True):
                    # A stopped container yields zeroed samples with a zero timestamp.
                    if not s.get("read") or s["read"].startswith("0001-01-01"):
                        break
                    with self.lock:
                        apply_stats(state, s, self.windows)
            except NotFound:
                return
            except Exception as e:
                log.warning("stats stream for %s failed: %s", state.name, e)
                with self.lock:
                    self.stats_errors_total += 1

            # The stream ended. Reopen it only if the container is still running;
            # otherwise the die event (or the next reconcile) records the finish.
            with self.lock:
                if state.finished_at is not None:
                    return
            try:
                if not self.client.api.inspect_container(state.id)["State"]["Running"]:
                    return
            except NotFound:
                return
            except Exception:
                pass
            time.sleep(1)

    # --- docker events ----------------------------------------------------------

    def handle_event(self, ev):
        action = ev.get("Action") or ev.get("status")
        actor = ev.get("Actor") or {}
        cid = actor.get("ID") or ev.get("id")
        attrs = actor.get("Attributes") or {}
        ts = ev["timeNano"] / 1e9 if ev.get("timeNano") else ev.get("time", time.time())

        if action == "start":
            # Event attributes carry the container's labels plus name and image.
            self.track(cid, attrs.get("name", cid[:12]), attrs.get("image", ""), attrs, ts)
        elif action == "die":
            code = attrs.get("exitCode")
            self.finish(cid, ts, int(code) if code is not None else None)
        elif action == "destroy":
            self.finish(cid, ts)

    def reconcile(self):
        """Pick up running job containers and close out ones that stopped while we weren't listening."""
        running = set()
        for c in self.client.containers.list(filters={"label": JOB_ID_LABEL}):
            running.add(c.id)
            self.track(c.id, c.name, c.attrs["Config"].get("Image", ""), c.labels,
                       parse_docker_time(c.attrs["State"]["StartedAt"]))

        with self.lock:
            stale = [cid for cid, s in self.containers.items()
                     if s.finished_at is None and cid not in running]
        for cid in stale:
            try:
                st = self.client.api.inspect_container(cid)["State"]
                self.finish(cid, parse_docker_time(st.get("FinishedAt")), st.get("ExitCode"))
            except NotFound:
                self.finish(cid, time.time())

    def run_events(self, stop):
        # Open the event stream before reconciling so nothing slips through the
        # gap; replayed duplicates are harmless because track/finish are idempotent.
        since = int(time.time()) - 1
        backoff = 1
        while not stop.is_set():
            try:
                stream = self.client.api.events(
                    since=since,
                    decode=True,
                    filters={"type": "container",
                             "event": ["start", "die", "destroy"],
                             "label": JOB_ID_LABEL},
                )
                self.reconcile()
                self.docker_up = 1
                backoff = 1
                for ev in stream:
                    since = int(ev.get("time", since))
                    self.events_total += 1
                    self.handle_event(ev)
                log.warning("docker event stream closed")
            except Exception as e:
                log.warning("docker event stream error: %s", e)
            self.docker_up = 0
            self.reconnects_total += 1
            stop.wait(backoff)
            backoff = min(backoff * 2, 30)

    def prune(self):
        cutoff = time.time() - self.summary_ttl
        with self.lock:
            for cid in [cid for cid, s in self.containers.items()
                        if s.finished_at is not None and s.finished_at < cutoff]:
                del self.containers[cid]

    # --- prometheus collector -----------------------------------------------------

    def collect(self):
        now = time.time()
        with self.lock:
            states = [replace(s) for s in self.containers.values()]

        def gauge(name, doc, extra=()):
            return GaugeMetricFamily(name, doc, labels=LABEL_NAMES + list(extra))

        def counter(name, doc):
            return CounterMetricFamily(name, doc, labels=LABEL_NAMES)

        p = "gitlab_job_container_"
        info = gauge(p + "info", "Job container metadata.", ["image", "job_url"])
        running = gauge(p + "running", "1 while the container runs, 0 after it exits.")
        start = gauge(p + "start_time_seconds", "Container start time, unix seconds.")
        duration = gauge(p + "duration_seconds", "Run time so far, or total run time once exited.")
        exit_code = gauge(p + "exit_code", "Exit code (only after the container exits).")
        cpu = counter(p + "cpu_seconds", "Total CPU time consumed.")
        mem = gauge(p + "memory_bytes", "Current memory (Windows: private working set; Linux: working set).")
        mem_peak = gauge(p + "memory_peak_bytes", "Highest memory_bytes seen (sampled ~1s).")
        commit_peak = gauge(p + "memory_commit_peak_bytes", "Peak commit reported by Windows.")
        disk_r = counter(p + "disk_read_bytes", "Bytes read from storage.")
        disk_w = counter(p + "disk_write_bytes", "Bytes written to storage.")
        net_rx = counter(p + "network_receive_bytes", "Bytes received.")
        net_tx = counter(p + "network_transmit_bytes", "Bytes transmitted.")

        for s in states:
            lv = s.label_values
            info.add_metric(lv + [s.image, s.job_url], 1)
            running.add_metric(lv, 0 if s.finished_at else 1)
            start.add_metric(lv, s.started_at)
            duration.add_metric(lv, (s.finished_at or now) - s.started_at)
            if s.exit_code is not None:
                exit_code.add_metric(lv, s.exit_code)
            cpu.add_metric(lv, s.cpu_seconds)
            mem.add_metric(lv, s.memory_bytes)
            mem_peak.add_metric(lv, s.memory_peak_bytes)
            if self.windows:
                commit_peak.add_metric(lv, s.memory_commit_peak_bytes)
            disk_r.add_metric(lv, s.disk_read_bytes)
            disk_w.add_metric(lv, s.disk_write_bytes)
            net_rx.add_metric(lv, s.net_rx_bytes)
            net_tx.add_metric(lv, s.net_tx_bytes)

        yield from (info, running, start, duration, exit_code, cpu, mem, mem_peak,
                    commit_peak, disk_r, disk_w, net_rx, net_tx)

        e = "gitlab_job_exporter_"
        yield GaugeMetricFamily(e + "docker_up", "1 if the Docker event stream is connected.", value=self.docker_up)
        yield GaugeMetricFamily(e + "tracked_containers", "Containers currently held in memory.", value=len(states))
        yield CounterMetricFamily(e + "docker_events", "Docker events processed.", value=self.events_total)
        yield CounterMetricFamily(e + "reconnects", "Docker event stream reconnects.", value=self.reconnects_total)
        yield CounterMetricFamily(e + "stats_errors", "Stats stream failures.", value=self.stats_errors_total)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--listen-address", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9417)
    parser.add_argument("--summary-ttl", type=int, default=600,
                        help="seconds to keep a container's metrics after it exits (default 600)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")

    # DOCKER_HOST is honoured; default is npipe:////./pipe/docker_engine on Windows.
    # Each stats stream holds its own connection, so allow more than the default pool of 10.
    client = docker.from_env(max_pool_size=64)
    exporter = JobContainerExporter(client, args.summary_ttl)
    REGISTRY.register(exporter)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    start_http_server(args.port, addr=args.listen_address)
    log.info("listening on %s:%d (docker os: %s)", args.listen_address, args.port,
             "windows" if exporter.windows else "linux")

    threading.Thread(target=exporter.run_events, args=(stop,), daemon=True, name="events").start()
    while not stop.wait(30):
        exporter.prune()


if __name__ == "__main__":
    main()

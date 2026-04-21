import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from http.client import HTTPResponse
from urllib.parse import quote


DOCKER_SOCK = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
LISTEN_HOST = os.environ.get("EXPORTER_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("EXPORTER_PORT", "9888"))
SCRAPE_TIMEOUT = float(os.environ.get("SCRAPE_TIMEOUT_SECONDS", "20"))


class UnixHTTPConnection:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path

    def request_json(self, path: str):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(SCRAPE_TIMEOUT)
        sock.connect(self.socket_path)
        request = (
            f"GET {path} HTTP/1.1\r\n"
            "Host: docker\r\n"
            "User-Agent: docker-stats-exporter\r\n"
            "Accept: application/json\r\n"
            "Connection: close\r\n\r\n"
        )
        sock.sendall(request.encode("ascii"))
        response = HTTPResponse(sock)
        response.begin()
        body = response.read()
        sock.close()
        return json.loads(body.decode("utf-8"))


def unix_time(value: str) -> float:
    if not value or value.startswith("0001-01-01"):
        return 0.0
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")


def metric_line(name: str, value: float, labels: dict[str, str]) -> str:
    rendered = ",".join(f'{key}="{escape_label(val)}"' for key, val in sorted(labels.items()))
    return f"{name}{{{rendered}}} {value}"


def collect_metrics() -> str:
    client = UnixHTTPConnection(DOCKER_SOCK)
    now = time.time()
    containers = client.request_json("/containers/json?all=1")
    lines = [
        "# HELP docker_container_last_seen Last scrape timestamp for the container.",
        "# TYPE docker_container_last_seen gauge",
        "# HELP docker_container_memory_working_set_bytes Container memory usage in bytes.",
        "# TYPE docker_container_memory_working_set_bytes gauge",
        "# HELP docker_container_cpu_usage_seconds_total Total container CPU time consumed in seconds.",
        "# TYPE docker_container_cpu_usage_seconds_total counter",
        "# HELP docker_container_start_time_seconds Container start time as unix timestamp.",
        "# TYPE docker_container_start_time_seconds gauge",
        "# HELP docker_container_oom_killed Container oom-killed flag from Docker inspect.",
        "# TYPE docker_container_oom_killed gauge",
        "# HELP docker_container_running Container running state from Docker inspect.",
        "# TYPE docker_container_running gauge",
    ]

    def collect_container_metrics(container: dict) -> list[str]:
        worker = UnixHTTPConnection(DOCKER_SOCK)
        container_id = container["Id"]
        inspect = worker.request_json(f"/containers/{quote(container_id)}/json")

        labels = inspect.get("Config", {}).get("Labels", {}) or {}
        service = labels.get("com.docker.compose.service", "")
        name = inspect.get("Name", "").lstrip("/") or (container.get("Names") or [""])[0].lstrip("/")
        image = inspect.get("Config", {}).get("Image", "") or container.get("Image", "")
        state = inspect.get("State", {}) or {}
        running = 1.0 if state.get("Running") else 0.0

        cpu_total = 0.0
        mem_usage = 0.0
        if running:
          stats = worker.request_json(f"/containers/{quote(container_id)}/stats?stream=false")
          cpu_total = (stats.get("cpu_stats", {}) or {}).get("cpu_usage", {}).get("total_usage", 0) / 1_000_000_000
          mem_usage = (stats.get("memory_stats", {}) or {}).get("usage", 0)

        metric_labels = {
            "id": f"/docker/{container_id}",
            "image": image,
            "name": name,
            "service": service,
        }

        start_time = unix_time(state.get("StartedAt", ""))
        oom_killed = 1.0 if state.get("OOMKilled") else 0.0
        return [
            metric_line("docker_container_last_seen", now, metric_labels),
            metric_line("docker_container_memory_working_set_bytes", mem_usage, metric_labels),
            metric_line("docker_container_cpu_usage_seconds_total", cpu_total, metric_labels),
            metric_line("docker_container_start_time_seconds", start_time, metric_labels),
            metric_line("docker_container_oom_killed", oom_killed, metric_labels),
            metric_line("docker_container_running", running, metric_labels),
        ]

    with ThreadPoolExecutor(max_workers=8) as pool:
        for container_lines in pool.map(collect_container_metrics, containers):
            lines.extend(container_lines)

    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok\n")
            return

        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        try:
            body = collect_metrics().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:  # noqa: BLE001
            body = f"exporter_error {json.dumps(str(exc))}\n".encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    server = HTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    server.serve_forever()

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from payments.consumer import configure_logging
from payments.healthcheck import check


def test_local_health_probe_bypasses_environment_proxy(monkeypatch):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            received.append(self.headers.get("X-API-Key"))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        def log_message(self, *_):
            pass

    for variable in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        monkeypatch.setenv(variable, "http://127.0.0.1:9")
    for variable in ("NO_PROXY", "no_proxy"):
        monkeypatch.setenv(variable, "")
    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            check(f"http://127.0.0.1:{server.server_port}/health")
        finally:
            server.shutdown()
            thread.join(timeout=5)
    assert received == [os.environ["API_KEY"]]


def test_http_dependency_logs_do_not_expose_callback_tokens(settings, monkeypatch, caplog):
    for name in ("httpx", "httpcore"):
        monkeypatch.setattr(logging.getLogger(name), "disabled", False)
    configure_logging(settings)
    with caplog.at_level(logging.DEBUG):
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).info("POST http://webhook/hook?token=synthetic-private-token")
    assert "synthetic-private-token" not in caplog.text

#!/usr/bin/env python3
"""Local TLS termination for real FE/BE HTTP fixture requests, not a load mock."""
import argparse
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import ssl
import threading
from urllib.parse import urlsplit, urlunsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--fe", required=True)
    parser.add_argument("--be", required=True)
    parser.add_argument("--mtls", action="store_true")
    parser.add_argument("--private-location", action="store_true")
    parser.add_argument("--downgrade", action="store_true")
    args = parser.parse_args()
    root = Path(os.environ["SQLX_DORIS_BUILD_ROOT"]).resolve()
    if not args.events.resolve().is_relative_to(root / "artifacts"):
        raise SystemExit("TLS proxy evidence must stay on the external artifact volume")
    certs = root / "fixtures/tls"
    upstreams = {
        name: urlsplit(value) for name, value in [("fe", args.fe), ("be", args.be)]
    }
    if any(
        u.scheme != "http" or u.hostname not in ("localhost", "127.0.0.1")
        for u in upstreams.values()
    ):
        raise SystemExit("only the owned loopback HTTP fixtures are supported")
    args.events.parent.mkdir(parents=True, exist_ok=True)
    args.events.write_text("")
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *unused):
            pass

        def do_PUT(self):
            length = int(self.headers["Content-Length"])
            body = self.rfile.read(length)
            if len(body) != length:
                self.send_error(400, "incomplete fixture upload")
                return
            service = self.server.service
            upstream = upstreams[service]
            headers = {
                k: v
                for k, v in self.headers.items()
                if k.lower() not in ("host", "connection")
            }
            connection = HTTPConnection(upstream.hostname, upstream.port, timeout=15)
            try:
                connection.request("PUT", self.path, body, headers)
                response = connection.getresponse()
                payload = response.read()
                location = response.getheader("Location")
                if service == "fe" and location and response.status in (307, 308):
                    parsed = urlsplit(location)
                    authority = (
                        "private-be.invalid:8040"
                        if args.private_location
                        else f"localhost:{be.server_port}"
                    )
                    location = urlunsplit(
                        (
                            "http" if args.downgrade else "https",
                            authority,
                            parsed.path,
                            parsed.query,
                            "",
                        )
                    )
                event = {
                    "service": service,
                    "path": self.path,
                    "upstream_status": response.status,
                    "body_bytes": length,
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                    "basic_auth_present": self.headers.get(
                        "Authorization", ""
                    ).startswith("Basic "),
                    "expect_continue_present": self.headers.get("Expect", "").lower()
                    == "100-continue",
                    "client_certificate_present": bool(self.connection.getpeercert()),
                }
                # Record booleans and hashes, never passwords or auth headers.
                with lock, args.events.open("a") as output:
                    output.write(json.dumps(event) + "\n")
                self.send_response(response.status)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header(
                    "Content-Type",
                    response.getheader("Content-Type", "application/json"),
                )
                self.send_header("Connection", "close")
                if location:
                    self.send_header("Location", location)
                self.end_headers()
                self.wfile.write(payload)
                self.close_connection = True
            finally:
                connection.close()

    def server(service):
        instance = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        instance.daemon_threads = True
        instance.service = service
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(certs / "server-cert.pem", certs / "server-key.pem")
        if args.mtls:
            context.load_verify_locations(certs / "ca.pem")
            context.verify_mode = ssl.CERT_REQUIRED
        instance.socket = context.wrap_socket(instance.socket, server_side=True)
        return instance

    be = server("be")
    fe = server("fe")
    threading.Thread(target=be.serve_forever, daemon=True).start()
    print(
        json.dumps(
            {
                "fe": f"https://localhost:{fe.server_port}",
                "be": f"https://localhost:{be.server_port}",
            }
        ),
        flush=True,
    )
    fe.serve_forever()


if __name__ == "__main__":
    main()

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import algo
import mt5_exec as exe
import threading

PORT = 8080


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", len(data))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/status":
            self._send(200, json.dumps(algo.get_state()))
        elif self.path in ("/", "/index.html"):
            with open("index.html", "rb") as f:
                self._send(200, f.read(), "text/html")
        else:
            self._send(404, '{"error":"not found"}')

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length) or b"{}")
        if self.path == "/threshold":
            algo.set_thresholds(
                speed_mult=body.get("speed_mult"),
                imb_thresh=body.get("imb_thresh"),
            )
            self._send(200, '{"ok":true}')
        elif self.path == "/manual":
            result = exe.fire(body.get("action","buy"), "manual")
            self._send(200, json.dumps(result))
        else:
            self._send(404, '{"error":"not found"}')

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


def start():
    srv = HTTPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"Server on port {PORT}", flush=True)

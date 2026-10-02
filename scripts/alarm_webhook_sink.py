# -*- coding: utf-8 -*-
"""最小 webhook 接收端：把收到的告警 POST 追加到文件，便于端到端验证。

用法：
    python scripts/alarm_webhook_sink.py --port 9000 --out F:\\cems-backup\\alarm_sink.jsonl
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main() -> int:
    parser = argparse.ArgumentParser(description="告警 Webhook 接收端（演示/验证用）")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--host", default="0.0.0.0",
                        help="默认 0.0.0.0（容器要能访问）；仅本机用可设 127.0.0.1")
    parser.add_argument("--out", default="alarm_sink.jsonl")
    args = parser.parse_args()

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            line = json.dumps(
                {"received_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "path": self.path,
                 "payload": json.loads(body.decode("utf-8")) if body else None},
                ensure_ascii=False,
            )
            with out.open("a", encoding="utf-8") as fp:
                fp.write(line + "\n")
            print(f"[告警] {line[:160]}", flush=True)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, *a: object) -> None:
            return

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Webhook 接收端: http://{args.host}:{args.port}/  ->  {out}", flush=True)
    print("[注意] 这是验证/演示用的最小接收端，不是生产服务。Ctrl+C 退出。", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止", flush=True)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

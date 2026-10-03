"""HTTP 接口：值班员用 HTTP 建预约、确认取冰、查结欠。

仅使用标准库 http.server + json，不依赖第三方包。
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

from .service import IceDockService, NotFound, BadRequest, Conflict


class Handler(BaseHTTPRequestHandler):
    def _service(self):
        return self.server.service  # type: ignore[attr-defined]

    # -- helpers ----------------------------------------------------------
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _query(self):
        q = parse_qs(urlparse(self.path).query)
        return {k: v[0] for k, v in q.items()}

    def log_message(self, fmt, *args):
        pass  # 静默默认日志

    # -- GET --------------------------------------------------------------
    def do_GET(self):
        try:
            path = unquote(urlparse(self.path).path)
            s = self._service()
            if path == "/health":
                self._send(200, s.health())
            elif path == "/slots":
                self._send(200, {"slots": s.list_slots()})
            elif path == "/appointments":
                q = self._query()
                self._send(200, {"appointments": s.list_appointments(
                    boat_owner=q.get("boat_owner"), slot_id=q.get("slot_id"),
                    status=q.get("status"))})
            elif path.startswith("/appointments/"):
                aid = path.rsplit("/", 1)[-1]
                self._send(200, s.get_appointment(aid))
            elif path == "/confirmations":
                q = self._query()
                self._send(200, {"confirmations": s.list_confirmations(
                    boat_owner=q.get("boat_owner"))})
            elif path == "/balances":
                self._send(200, {"balances": s.list_balances()})
            elif path.startswith("/balances/"):
                boat = path.rsplit("/", 1)[-1]
                self._send(200, s.get_balance(boat))
            elif path == "/queue":
                self._send(200, {"queue": s.list_queue()})
            elif path == "/ice-maker":
                self._send(200, s.ice_maker_status())
            elif path == "/network":
                self._send(200, s.network_status())
            elif path == "/outbox":
                q = self._query()
                self._send(200, {"outbox": s.list_outbox(status=q.get("status"))})
            else:
                self._send(404, {"error": "not found", "path": path})
        except NotFound as e:
            self._send(404, {"error": str(e)})
        except (BadRequest, Conflict) as e:
            self._send(400 if isinstance(e, BadRequest) else 409, {"error": str(e)})
        except Exception as e:
            self._send(500, {"error": str(e)})

    # -- POST -------------------------------------------------------------
    def do_POST(self):
        try:
            path = unquote(urlparse(self.path).path)
            body = self._read_json()
            s = self._service()
            if path == "/slots":
                slot = s.create_slot(
                    body["slot_id"], body.get("slot_time", body["slot_id"]),
                    float(body["capacity"]))
                self._send(201, {"slot": slot})
            elif path == "/appointments":
                appt, dup = s.create_appointment(
                    body["boat_owner"], body["slot_id"], float(body["amount"]),
                    body["idempotency_key"], body.get("voyage_id"))
                self._send(200 if dup else 201,
                           {"appointment": appt, "duplicated": dup})
            elif path == "/confirmations/start":
                appt = s.start_confirmation(body["appointment_id"])
                self._send(200, {"appointment": appt})
            elif path == "/confirmations/complete":
                conf, dup = s.complete_confirmation(
                    body["appointment_id"], float(body["actual_amount"]),
                    body["idempotency_key"], body.get("actual_slot_id"))
                self._send(200 if dup else 201,
                           {"confirmation": conf, "duplicated": dup})
            elif path == "/confirmations":
                conf, dup = s.confirm_pickup(
                    body["appointment_id"], float(body["actual_amount"]),
                    body["idempotency_key"], body.get("actual_slot_id"))
                self._send(200 if dup else 201,
                           {"confirmation": conf, "duplicated": dup})
            elif path == "/ice-maker/start":
                self._send(200, s.ice_maker_start())
            elif path == "/ice-maker/stop":
                self._send(200, s.ice_maker_stop())
            elif path == "/network/up":
                self._send(200, s.network_up())
            elif path == "/network/down":
                self._send(200, s.network_down())
            elif path == "/outbox/replay":
                self._send(200, {"replayed": s.replay_outbox()})
            else:
                self._send(404, {"error": "not found", "path": path})
        except KeyError as e:
            self._send(400, {"error": f"缺少字段: {e}"})
        except NotFound as e:
            self._send(404, {"error": str(e)})
        except (BadRequest, Conflict) as e:
            self._send(400 if isinstance(e, BadRequest) else 409, {"error": str(e)})
        except Exception as e:
            self._send(500, {"error": str(e)})


def serve(db_path, host="127.0.0.1", port=8765):
    from .worker import OutboxWorker
    service = IceDockService(db_path)
    server = ThreadingHTTPServer((host, port), Handler)
    server.service = service  # type: ignore[attr-defined]
    worker = OutboxWorker(db_path, service)
    worker.start()
    print(f"冰码头服务已启动: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        worker.stop()
        server.shutdown()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
冰港取冰预约服务（零第三方依赖，仅用 Python 3 标准库）。

业务规则
--------
1. 船主按「到港时段(slot)」预约取冰，预约占用该时段制冰余量；余量不足则进入该时段队列 FIFO 排队。
2. 确认取冰时按当前余量「重查」：
   - 可在原时段确认，也可带 actual_slot_id 表示船早到/晚到、实际落在别的时段；
   - 实拿过磅重量超过当前可承受余量 -> 409，不允许确认。
3. 确认接口幂等：码头员必须带 confirm_key；两个码头员同时提交同一条确认，只记一次，第二次原样返回。
4. 少拿差额：本航次自己的预约量未被满足的部分，记为船东欠冰（shortfall），结到下一航次；
   下一航次预约量须覆盖结欠量，过磅后优先满足本航次需求，多出的部分偿还结欠（repayment）。
5. 码头断网：值班员的请求先落本地追加日志（fsync）再应答；网络恢复后用同一 client_key /
   confirm_key 重发，服务按幂等键去重，不会重复占容量。
6. 容量释放（取消 / 确认完成）后自动从队首放排队预约入场，队首不满足则其后全部阻塞（严格 FIFO）。
7. 制冰机停机：该时段所有「已占容量、处理中(reserved)」的预约整批退回队列；
   开机后重新按 FIFO 入场。

存储：单文件 JSONL 追加日志（WAL），启动时回放重建全部状态；全程一把锁串行化写操作。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

EPS = 1e-9


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def qnum(value, name):
    """重量/容量必须是非负数字。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise APIError(400, f"{name} 必须是数字")
    v = float(value)
    if v < -EPS:
        raise APIError(400, f"{name} 不能为负")
    return max(0.0, v)


def require(body, field):
    if not isinstance(body, dict) or field not in body:
        raise APIError(400, f"缺少字段: {field}")
    return body[field]


class APIError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class Store:
    """内存状态 + 追加日志。所有公开方法都在 self.lock 下串行执行。"""

    def __init__(self, log_path: str):
        self.log_path = log_path
        self.lock = threading.RLock()
        self.slots: dict[str, dict] = {}
        self.bookings: dict[str, dict] = {}
        # 每个时段的排队顺序（booking_id 列表）
        self.queue: dict[str, list[str]] = {}
        # 船东 -> 台账条目列表
        self.ledger: dict[str, list[dict]] = {}
        self.booking_keys: dict[str, str] = {}   # client_key -> booking_id
        self.confirm_keys: dict[str, str] = {}   # confirm_key -> booking_id
        self._ledger_seq = 0

        os.makedirs(os.path.dirname(os.path.abspath(log_path)), exist_ok=True)
        # 回放；'a+' 保证文件不存在时创建
        self._log = open(log_path, "a+", encoding="utf-8")
        self._log.seek(0)
        for line in self._log:
            line = line.strip()
            if line:
                self._apply(json.loads(line))
        self._log.seek(0, os.SEEK_END)

    def close(self):
        with self.lock:
            if not self._log.closed:
                self._log.close()

    # ---------- 日志：先落盘(fsync)，再改内存，保证崩溃不丢 ----------

    def _emit(self, event: dict):
        event.setdefault("ts", now_iso())
        self._log.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._log.flush()
        os.fsync(self._log.fileno())
        self._apply(event)

    def _apply(self, e: dict):
        kind = e["event"]
        if kind == "slot_created":
            self.slots[e["slot_id"]] = {
                "id": e["slot_id"],
                "capacity": float(e["capacity"]),
                "running": True,
            }
            self.queue.setdefault(e["slot_id"], [])
        elif kind == "machine_set":
            self.slots[e["slot_id"]]["running"] = bool(e["running"])
        elif kind == "booking_created":
            b = {
                "id": e["booking_id"],
                "slot_id": e["slot_id"],
                "ship": e["ship"],
                "voyage": e["voyage"],
                "qty": float(e["qty"]),
                "status": e["status"],          # queued | reserved
                "arrear_carried": float(e.get("arrear_carried", 0.0)),
                "own_need": float(e.get("qty", 0.0)) - float(e.get("arrear_carried", 0.0)),
                "client_key": e.get("client_key"),
                "created_at": e.get("ts"),
                "reserved_at": None,
                "actual_slot_id": None,
                "delivered_qty": None,
                "new_shortfall": None,
                "repaid": None,
                "confirmed_key": None,
                "confirmed_at": None,
                "cancelled_at": None,
            }
            self.bookings[b["id"]] = b
            if b["status"] == "queued":
                self.queue.setdefault(b["slot_id"], []).append(b["id"])
            if b["client_key"]:
                self.booking_keys[b["client_key"]] = b["id"]
        elif kind == "booking_admitted":
            b = self.bookings[e["booking_id"]]
            q = self.queue[b["slot_id"]]
            if b["id"] in q:
                q.remove(b["id"])
            b["status"] = "reserved"
            b["reserved_at"] = e.get("ts")
        elif kind == "booking_returned":
            b = self.bookings[e["booking_id"]]
            b["status"] = "queued"
            b["reserved_at"] = None
            q = self.queue.setdefault(b["slot_id"], [])
            if b["id"] not in q:
                q.insert(e.get("pos", len(q)), b["id"])
        elif kind == "booking_confirmed":
            b = self.bookings[e["booking_id"]]
            if b["id"] in self.queue.get(b["slot_id"], []):
                self.queue[b["slot_id"]].remove(b["id"])
            b["status"] = "confirmed"
            b["actual_slot_id"] = e["actual_slot_id"]
            b["delivered_qty"] = float(e["delivered_qty"])
            b["new_shortfall"] = float(e.get("new_shortfall", 0.0))
            b["repaid"] = float(e.get("repaid", 0.0))
            b["confirmed_key"] = e.get("confirm_key")
            b["confirmed_at"] = e.get("ts")
            b["reserved_at"] = b.get("reserved_at")
            if e.get("confirm_key"):
                self.confirm_keys[e["confirm_key"]] = b["id"]
        elif kind == "booking_cancelled":
            b = self.bookings[e["booking_id"]]
            for q in self.queue.values():
                if b["id"] in q:
                    q.remove(b["id"])
            b["status"] = "cancelled"
            b["cancelled_at"] = e.get("ts")
        elif kind == "ledger_posted":
            entry = {
                "id": e["entry_id"],
                "ship": e["ship"],
                "voyage": e["voyage"],
                "kind": e["kind"],            # shortfall(新增结欠) | repayment(偿还)
                "qty": float(e["qty"]),
                "ts": e.get("ts"),
            }
            self.ledger.setdefault(e["ship"], []).append(entry)
            n = int(e["entry_id"].lstrip("L"))
            self._ledger_seq = max(self._ledger_seq, n)

    # ---------- 视图与派生量（确认时永远现场重算，不吃老快照） ----------

    def reserved_qty(self, slot_id: str) -> float:
        return sum(
            b["qty"] for b in self.bookings.values()
            if b["slot_id"] == slot_id and b["status"] == "reserved"
        )

    def delivered_qty(self, slot_id: str) -> float:
        return sum(
            b["delivered_qty"] for b in self.bookings.values()
            if b["actual_slot_id"] == slot_id and b["status"] == "confirmed"
        )

    def remaining(self, slot_id: str) -> float:
        s = self.slots[slot_id]
        return max(0.0, s["capacity"] - self.reserved_qty(slot_id)
                   - self.delivered_qty(slot_id))

    def ship_balance(self, ship: str) -> float:
        bal = 0.0
        for e in self.ledger.get(ship, []):
            bal += e["qty"] if e["kind"] == "shortfall" else -e["qty"]
        return round(bal, 6)

    def slot_view(self, slot_id: str) -> dict:
        s = self.slots[slot_id]
        return {
            "slot_id": slot_id,
            "capacity": s["capacity"],
            "running": s["running"],
            "reserved": round(self.reserved_qty(slot_id), 6),
            "delivered": round(self.delivered_qty(slot_id), 6),
            "remaining": round(self.remaining(slot_id), 6),
            "queue": list(self.queue.get(slot_id, [])),
        }

    def booking_view(self, b: dict) -> dict:
        return {
            "booking_id": b["id"],
            "ship": b["ship"],
            "voyage": b["voyage"],
            "slot_id": b["slot_id"],
            "qty": b["qty"],
            "status": b["status"],
            "arrear_carried": b["arrear_carried"],
            "own_need": round(b["own_need"], 6),
            "client_key": b["client_key"],
            "created_at": b["created_at"],
            "reserved_at": b["reserved_at"],
            "actual_slot_id": b["actual_slot_id"],
            "delivered_qty": b["delivered_qty"],
            "new_shortfall": b["new_shortfall"],
            "repaid": b["repaid"],
            "confirmed_key": b["confirmed_key"],
            "confirmed_at": b["confirmed_at"],
            "cancelled_at": b["cancelled_at"],
            "ship_balance": self.ship_balance(b["ship"]),
        }

    # ---------- 队列放号：严格 FIFO，队首放不下就阻塞后面 ----------

    def _drain_locked(self, slot_id: str):
        s = self.slots.get(slot_id)
        if not s or not s["running"]:
            return
        q = self.queue.setdefault(slot_id, [])
        while q:
            b = self.bookings[q[0]]
            if self.remaining(slot_id) + EPS >= b["qty"]:
                self._emit({"event": "booking_admitted", "booking_id": b["id"]})
            else:
                break

    # ---------- 业务操作 ----------

    def create_slot(self, body: dict) -> tuple[int, dict]:
        slot_id = body.get("slot_id") or f"S-{uuid.uuid4().hex[:8]}"
        cap = qnum(require(body, "capacity"), "capacity")
        with self.lock:
            if slot_id in self.slots:
                raise APIError(409, f"时段 {slot_id} 已存在")
            self._emit({"event": "slot_created", "slot_id": slot_id, "capacity": cap})
            return 201, self.slot_view(slot_id)

    def set_machine(self, slot_id: str, body: dict) -> tuple[int, dict]:
        running = require(body, "running")
        if not isinstance(running, bool):
            raise APIError(400, "running 必须是 true/false")
        with self.lock:
            if slot_id not in self.slots:
                raise APIError(404, f"时段 {slot_id} 不存在")
            if self.slots[slot_id]["running"] == running:
                return 200, self.slot_view(slot_id)
            self._emit({"event": "machine_set", "slot_id": slot_id, "running": running})
            if not running:
                # 停机：处理中(reserved)的预约整批退回队列，成块插到队首（创建顺序）。
                # 逐条带递增 pos 发事件，使日志回放后顺序一致。
                reserved = [b for b in self.bookings.values()
                            if b["slot_id"] == slot_id and b["status"] == "reserved"]
                for i, b in enumerate(reserved):
                    self._emit({"event": "booking_returned",
                                "booking_id": b["id"], "pos": i})
            else:
                self._drain_locked(slot_id)
            return 200, self.slot_view(slot_id)

    def create_booking(self, body: dict) -> tuple[int, dict]:
        ship = require(body, "ship")
        voyage = require(body, "voyage")
        slot_id = require(body, "slot_id")
        qty = qnum(require(body, "qty"), "qty")
        client_key = body.get("client_key") or f"auto-{uuid.uuid4().hex}"
        with self.lock:
            # 断网重发：同一 client_key 直接回放第一次的结果，绝不重复占容量
            if client_key in self.booking_keys:
                b = self.bookings[self.booking_keys[client_key]]
                return 200, {"deduplicated": True, **self.booking_view(b)}
            if slot_id not in self.slots:
                raise APIError(404, f"时段 {slot_id} 不存在")
            if qty <= EPS:
                raise APIError(400, "qty 必须大于 0")

            balance = self.ship_balance(ship)
            # 结欠结到下一航次：下一航次的预约量须覆盖尚未偿还的结欠
            if qty + EPS < balance:
                raise APIError(
                    409,
                    f"船东 {ship} 尚有结欠 {balance}，下一航次预约量须覆盖该结欠")

            booking_id = f"B-{uuid.uuid4().hex[:12]}"
            s = self.slots[slot_id]
            # 严格 FIFO：该时段已有排队者时，新预约一律入队，不许插队
            admit = (s["running"] and not self.queue.get(slot_id)
                     and self.remaining(slot_id) + EPS >= qty)
            self._emit({
                "event": "booking_created",
                "booking_id": booking_id,
                "slot_id": slot_id,
                "ship": ship,
                "voyage": voyage,
                "qty": qty,
                "status": "reserved" if admit else "queued",
                "arrear_carried": balance,
                "client_key": client_key,
            })
            b = self.bookings[booking_id]
            if admit:
                # created 事件已直接置为 reserved
                return 201, {"deduplicated": False, **self.booking_view(b)}
            return 202, {"deduplicated": False,
                         "message": "时段余量不足或制冰机停机，已进入队列",
                         **self.booking_view(b)}

    def confirm_booking(self, booking_id: str, body: dict) -> tuple[int, dict]:
        confirm_key = body.get("confirm_key")
        if not confirm_key:
            raise APIError(400, "confirm_key 必填（防重复确认的幂等键，建议终端单号）")
        d = qnum(require(body, "delivered_qty"), "delivered_qty")
        with self.lock:
            # 两个码头员同时提交同一条确认：后到者拿到第一次的结果，只记一次
            if confirm_key in self.confirm_keys:
                existed = self.bookings[self.confirm_keys[confirm_key]]
                if existed["id"] != booking_id:
                    raise APIError(409, "该 confirm_key 已用于别的预约")
                return 200, {"deduplicated": True,
                             "confirmation": self._confirm_payload(existed)}
            b = self.bookings.get(booking_id)
            if b is None:
                raise APIError(404, f"预约 {booking_id} 不存在")
            if b["confirmed_key"]:
                raise APIError(409, f"预约已确认，原确认单号 {b['confirmed_key']}")
            if b["status"] == "cancelled":
                raise APIError(409, "预约已取消，不能确认")

            actual_id = body.get("actual_slot_id") or b["slot_id"]
            if actual_id not in self.slots:
                raise APIError(404, f"实际到港时段 {actual_id} 不存在")
            if d > b["qty"] + EPS:
                raise APIError(400, f"过磅量 {d} 超过预约量 {b['qty']}")

            actual = self.slots[actual_id]
            if not actual["running"]:
                raise APIError(409, f"实际到港时段 {actual_id} 制冰机停机，不能确认")

            if actual_id == b["slot_id"]:
                # 原时段：若还在排队（含停机退回），先尝试 FIFO 放号
                if b["status"] == "queued":
                    self._drain_locked(b["slot_id"])
                if b["status"] == "queued":
                    raise APIError(409, "预约仍在队列中（容量不足或制冰机停机），暂不能确认")
                # 确认时重查余量：本预约自己占的 qty 会释放为实际交付，
                # 故当前可承受量 = remaining + qty
                capacity_now = self.remaining(actual_id) + b["qty"]
            else:
                # 早到/晚到跨时段：不占原时段入场名额，直接从原队列移出，
                # 重查实际到港时段的真实余量
                if b["status"] == "cancelled":
                    raise APIError(409, "预约已取消，不能确认")
                q = self.queue.get(b["slot_id"], [])
                if b["id"] in q:
                    q.remove(b["id"])
                capacity_now = self.remaining(actual_id)
            if d > capacity_now + EPS:
                raise APIError(409,
                    f"重查余量不足：实拿 {d}，当前可承受 {round(capacity_now, 6)}")

            # 结欠结算：先满足本航次自身需求，多出的冰偿还历史结欠
            arrear = b["arrear_carried"]
            own_need = b["own_need"]
            new_shortfall = max(0.0, own_need - d)
            repaid = max(0.0, min(arrear, d - own_need))

            self._emit({
                "event": "booking_confirmed",
                "booking_id": b["id"],
                "actual_slot_id": actual_id,
                "delivered_qty": d,
                "new_shortfall": round(new_shortfall, 6),
                "repaid": round(repaid, 6),
                "confirm_key": confirm_key,
            })
            if new_shortfall > EPS:
                self._post_ledger(b, "shortfall", new_shortfall)
            if repaid > EPS:
                self._post_ledger(b, "repayment", repaid)

            # 容量状态变化，两个时段都尝试放排队的船入场
            self._drain_locked(b["slot_id"])
            if actual_id != b["slot_id"]:
                self._drain_locked(actual_id)
            return 200, {"deduplicated": False, "confirmation": self._confirm_payload(b)}

    def _post_ledger(self, b: dict, kind: str, qty: float):
        self._ledger_seq += 1
        self._emit({
            "event": "ledger_posted",
            "entry_id": f"L{self._ledger_seq:06d}",
            "ship": b["ship"],
            "voyage": b["voyage"],
            "kind": kind,
            "qty": round(qty, 6),
        })

    def _confirm_payload(self, b: dict) -> dict:
        return {
            "booking_id": b["id"],
            "ship": b["ship"],
            "voyage": b["voyage"],
            "booked_slot": b["slot_id"],
            "actual_slot": b["actual_slot_id"],
            "booked_qty": b["qty"],
            "delivered_qty": b["delivered_qty"],
            "shortfall_qty": b["qty"] - (b["delivered_qty"] or 0.0),
            "new_arrear": b["new_shortfall"],
            "arrear_repaid": b["repaid"],
            "arrear_carried_in": b["arrear_carried"],
            "ship_balance": self.ship_balance(b["ship"]),
            "confirm_key": b["confirmed_key"],
            "confirmed_at": b["confirmed_at"],
        }

    def cancel_booking(self, booking_id: str) -> tuple[int, dict]:
        with self.lock:
            b = self.bookings.get(booking_id)
            if b is None:
                raise APIError(404, f"预约 {booking_id} 不存在")
            if b["status"] == "cancelled":
                return 200, {"deduplicated": True, **self.booking_view(b)}
            if b["status"] == "confirmed":
                raise APIError(409, "预约已确认，不能取消")
            self._emit({"event": "booking_cancelled", "booking_id": booking_id})
            self._drain_locked(b["slot_id"])
            return 200, {"deduplicated": False, **self.booking_view(b)}

    def ship_view(self, ship: str) -> dict:
        return {
            "ship": ship,
            "balance": self.ship_balance(ship),
            "entries": list(self.ledger.get(ship, [])),
        }


# ---------------- HTTP 层 ----------------

class Handler(BaseHTTPRequestHandler):
    server_version = "IceService/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

    def _send(self, status: int, payload):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise APIError(400, "请求体必须是合法 JSON")
        if not isinstance(data, dict):
            raise APIError(400, "请求体必须是 JSON 对象")
        return data

    def do_GET(self):
        try:
            path = unquote(urlparse(self.path).path.rstrip("/")) or "/"
            store = self.server.store
            with store.lock:
                if path == "/":
                    self._send(200, {"service": "ice-service", "status": "ok"})
                elif path == "/slots":
                    self._send(200, {"slots": [store.slot_view(sid)
                                               for sid in sorted(store.slots)]})
                elif path.startswith("/slots/"):
                    sid = path.split("/")[2]
                    if sid not in store.slots:
                        raise APIError(404, f"时段 {sid} 不存在")
                    self._send(200, store.slot_view(sid))
                elif path.startswith("/bookings/"):
                    bid = path.split("/")[2]
                    if bid not in store.bookings:
                        raise APIError(404, f"预约 {bid} 不存在")
                    self._send(200, store.booking_view(store.bookings[bid]))
                elif path.startswith("/ships/"):
                    ship = path.split("/")[2]
                    self._send(200, store.ship_view(ship))
                else:
                    raise APIError(404, "未知路径")
        except APIError as e:
            self._send(e.status, {"error": e.message})

    def do_POST(self):
        try:
            path = unquote(urlparse(self.path).path.rstrip("/")) or "/"
            body = self._body()
            store = self.server.store
            with store.lock:
                if path == "/slots":
                    status, payload = store.create_slot(body)
                elif path.startswith("/slots/") and path.endswith("/machine"):
                    sid = path.split("/")[2]
                    status, payload = store.set_machine(sid, body)
                elif path == "/bookings":
                    status, payload = store.create_booking(body)
                elif path.startswith("/bookings/") and path.endswith("/confirm"):
                    bid = path.split("/")[2]
                    status, payload = store.confirm_booking(bid, body)
                elif path.startswith("/bookings/") and path.endswith("/cancel"):
                    bid = path.split("/")[2]
                    status, payload = store.cancel_booking(bid)
                else:
                    raise APIError(404, "未知路径")
            self._send(status, payload)
        except APIError as e:
            self._send(e.status, {"error": e.message})


def build_server(host: str, port: int, log_path: str) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), Handler)
    server.store = Store(log_path)
    return server


def main(argv=None):
    ap = argparse.ArgumentParser(description="冰港取冰预约服务（零依赖）")
    ap.add_argument("--host", default=os.environ.get("ICE_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("ICE_PORT", "8080")))
    ap.add_argument("--log", default=os.environ.get("ICE_LOG", "ice_journal.log"),
                    help="JSONL 追加日志路径（断点恢复用）")
    args = ap.parse_args(argv)

    server = build_server(args.host, args.port, args.log)
    host, port = server.server_address[:2]
    print(f"ice-service 监听 http://{host}:{port}，日志 {args.log}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("正在关闭…", flush=True)
    finally:
        server.server_close()
        server.store.close()


if __name__ == "__main__":
    main()

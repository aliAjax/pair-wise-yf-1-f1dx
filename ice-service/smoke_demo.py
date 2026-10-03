#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
端到端冒烟脚本：用标准库 http.client 把题目场景在一个真实服务上走一遍。

用法：
  python3 smoke_demo.py            # 自动拉起临时服务（随机端口）
  python3 smoke_demo.py 18080      # 打一个已经在跑的服务（数据会写进它的日志）

它不依赖任何第三方库。
"""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from urllib.parse import quote


class Client:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, body=None, expect=None):
        path = "/".join(quote(p) for p in path.split("/"))
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as r:
                status, payload = r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            status, payload = e.code, json.loads(e.read().decode())
        ok = "OK " if (expect is None or status == expect) else "BAD"
        summary = payload.get("error") or _summary(payload)
        print(f"[{ok}] {status} {method} {path} -> {summary}")
        if expect is not None and status != expect:
            raise SystemExit(f"期望 {expect}，实际 {status}: {payload}")
        return status, payload


def _summary(d):
    keys = ("status", "remaining", "queue", "deduplicated", "message",
            "delivered_qty", "new_arrear", "arrear_repaid", "ship_balance",
            "actual_slot", "balance")
    return ", ".join(f"{k}={d[k]}" for k in keys if isinstance(d, dict) and k in d)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else None
    proc = None
    if port is None:
        port = 18099
        proc = subprocess.Popen(
            [sys.executable, "ice_service.py", "--port", str(port),
             "--log", "/tmp/ice-smoke.log"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.8)
    c = Client(f"http://127.0.0.1:{port}")
    try:
        print("--- 建时段 AM/PM，容量各 100 ---")
        c.call("POST", "/slots", {"slot_id": "AM", "capacity": 100}, 201)
        c.call("POST", "/slots", {"slot_id": "PM", "capacity": 100}, 201)

        print("\n--- 预约：A(60)、B(30) 入场；C(20)、D(5) 容量满/队首阻塞排队 ---")
        _, a = c.call("POST", "/bookings",
                      {"ship": "沪渔1", "voyage": "V1", "slot_id": "AM",
                       "qty": 60, "client_key": "P-A"}, 201)
        c.call("POST", "/bookings",
               {"ship": "沪渔2", "voyage": "V1", "slot_id": "AM",
                "qty": 30, "client_key": "P-B"}, 201)
        _, cc = c.call("POST", "/bookings",
                       {"ship": "沪渔3", "voyage": "V1", "slot_id": "AM",
                        "qty": 20, "client_key": "P-C"}, 202)
        _, dd = c.call("POST", "/bookings",
                       {"ship": "沪渔4", "voyage": "V1", "slot_id": "AM",
                        "qty": 5, "client_key": "P-D"}, 202)
        c.call("GET", "/slots/AM", expect=200)

        print("\n--- 断网重发：P-A 再发一次，幂等不重复占容量 ---")
        c.call("POST", "/bookings",
               {"ship": "沪渔1", "voyage": "V1", "slot_id": "AM",
                "qty": 60, "client_key": "P-A"}, 200)

        print("\n--- 两个码头员并发提交同一条确认 TICKET-1（过磅 55）---")
        import concurrent.futures

        def confirm_a():
            return c.call("POST", f"/bookings/{a['booking_id']}/confirm",
                          {"delivered_qty": 55, "confirm_key": "TICKET-1"})
        with concurrent.futures.ThreadPoolExecutor(2) as ex:
            rs = list(ex.map(lambda _: confirm_a(), range(2)))
        dedups = sorted(r[1]["deduplicated"] for r in rs)
        assert dedups == [False, True], dedups
        print("     => 恰好一次真实确认，另一次 deduplicated=true")
        # 确认释放容量：队列放号，C(20) 入场（余量 45），D(5) 随后入场
        c.call("GET", "/slots/AM", expect=200)
        c.call("GET", f"/bookings/{cc['booking_id']}", expect=200)
        c.call("GET", f"/bookings/{dd['booking_id']}", expect=200)

        print("\n--- A 少拿 5（约60/实55），查结欠 5，结到下一航次 ---")
        c.call("GET", "/ships/沪渔1", expect=200)

        print("\n--- A 下一航次必须覆盖结欠：只约 3 -> 409；约 25（5结欠+20本航次）-> 201 ---")
        c.call("POST", "/bookings",
               {"ship": "沪渔1", "voyage": "V2", "slot_id": "PM",
                "qty": 3, "client_key": "P-A2"}, 409)
        _, a2 = c.call("POST", "/bookings",
                       {"ship": "沪渔1", "voyage": "V2", "slot_id": "PM",
                        "qty": 25, "client_key": "P-A2"}, 201)
        print("--- 实拿 22：本航次要 20，多出 2 偿还结欠，剩结欠 3 ---")
        c.call("POST", f"/bookings/{a2['booking_id']}/confirm",
               {"delivered_qty": 22, "confirm_key": "TICKET-2"}, 200)
        c.call("GET", "/ships/沪渔1", expect=200)

        print("\n--- 船早到/晚到：给 C 确认到 PM，实拿 20 超过 PM 当前余量(75? 否) ---")
        # PM 已被 A2 占 25，余 75，C 拿 20 没问题
        c.call("POST", f"/bookings/{cc['booking_id']}/confirm",
               {"delivered_qty": 20, "confirm_key": "TICKET-3",
                "actual_slot_id": "PM"}, 200)
        c.call("GET", "/slots/AM", expect=200)
        c.call("GET", "/slots/PM", expect=200)

        print("\n--- 制冰机停机：AM 处理中的 B、D 整批退回队列 ---")
        c.call("POST", "/slots/AM/machine", {"running": False}, 200)
        c.call("POST", f"/bookings/{dd['booking_id']}/confirm",
               {"delivered_qty": 5, "confirm_key": "TICKET-4"}, 409)
        print("--- 开机：按序重新入场 ---")
        c.call("POST", "/slots/AM/machine", {"running": True}, 200)
        c.call("POST", f"/bookings/{dd['booking_id']}/confirm",
               {"delivered_qty": 5, "confirm_key": "TICKET-4"}, 200)

        print("\n全部冒烟断言通过 ✅")
    finally:
        if proc is not None:
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == "__main__":
    main()

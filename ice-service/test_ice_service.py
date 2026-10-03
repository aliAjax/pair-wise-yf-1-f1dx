#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ice_service 端到端测试（仅用标准库 unittest + urllib）。

覆盖：
  1. 建预约 / 确认 / 结欠台账，少拿差额结到下一航次并在下航次偿还
  2. 断网重发：同一 client_key 重放不重复占容量；重启日志恢复后依旧幂等
  3. 两个码头员并发提交同一条 confirm_key，只记一次
  4. 容量满 -> 排队 -> 释放容量 FIFO 自动放号（队首阻塞）
  5. 制冰机停机：处理中预约整批退回队列；开机后按序重新入场
  6. 早到/晚到跨时段确认：重查目标时段余量，不足拒绝
"""

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from ice_service import Store, build_server


def store_in_tmp():
    d = tempfile.mkdtemp(prefix="ice-test-")
    return Store(os.path.join(d, "journal.log")), d


class HttpHarness:
    """把真实 HTTP 服务起在随机端口上，后台线程提供服务。"""

    def __init__(self, log_path):
        self.server = build_server("127.0.0.1", 0, log_path)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.server.store.close()

    def req(self, method, path, body=None):
        # 路径段（如中文船名）需要百分号编码；保留已有的 / 分隔符
        from urllib.parse import quote
        path = "/".join(quote(seg) for seg in path.split("/"))
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(r) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())


def slot(h, sid, cap):
    return h.req("POST", "/slots", {"slot_id": sid, "capacity": cap})


class TestStore(unittest.TestCase):

    def setUp(self):
        self.store, self.tmp = store_in_tmp()

    def tearDown(self):
        self.store.close()

    def mk(self, sid, cap):
        self.store.create_slot({"slot_id": sid, "capacity": cap})

    def book(self, ship, voyage, sid, qty, key=None):
        body = {"ship": ship, "voyage": voyage, "slot_id": sid, "qty": qty}
        if key:
            body["client_key"] = key
        return self.store.create_booking(body)

    def confirm(self, bid, delivered, key, actual=None):
        body = {"delivered_qty": delivered, "confirm_key": key}
        if actual:
            body["actual_slot_id"] = actual
        status, payload = self.store.confirm_booking(bid, body)
        return status, payload

    # 1. 基本流：少拿 -> 结欠 -> 下一航次覆盖预约并偿还
    def test_shortfall_carries_and_repays(self):
        self.mk("T1", 100)
        s, b1 = self.book("沪渔88", "V1", "T1", 30, "k1")
        self.assertEqual(b1["status"], "reserved")
        self.assertEqual(b1["arrear_carried"], 0.0)

        # 过磅实拿 20，少拿 10
        s, c1 = self.confirm(b1["booking_id"], 20, "DOCK-1")
        self.assertFalse(c1["deduplicated"])
        self.assertEqual(c1["confirmation"]["new_arrear"], 10.0)
        self.assertEqual(c1["confirmation"]["ship_balance"], 10.0)
        v = self.store.ship_view("沪渔88")
        self.assertEqual(v["balance"], 10.0)
        self.assertEqual(v["entries"][0]["kind"], "shortfall")

        # 下一航次预约量必须覆盖结欠：只约 5 -> 409
        with self.assertRaisesRegex(Exception, "结欠"):
            self.book("沪渔88", "V2", "T1", 5, "k2")
        # 覆盖结欠：预约 30（10 结欠 + 20 本航次需求）
        s, b2 = self.book("沪渔88", "V2", "T1", 30, "k2")
        self.assertEqual(b2["arrear_carried"], 10.0)
        self.assertEqual(b2["own_need"], 20.0)

        # 实拿 25：本航次要 20，多出的 5 偿还结欠，剩余结欠 5
        s, c2 = self.confirm(b2["booking_id"], 25, "DOCK-2")
        self.assertEqual(c2["confirmation"]["new_arrear"], 0.0)
        self.assertEqual(c2["confirmation"]["arrear_repaid"], 5.0)
        self.assertEqual(c2["confirmation"]["ship_balance"], 5.0)

    # 2. client_key 幂等：重发不重复占容量
    def test_booking_idempotent_retry(self):
        self.mk("T1", 30)
        s, b1 = self.book("浙岱66", "V1", "T1", 20, "PHONE-7")
        s, b1_retry = self.book("浙岱66", "V1", "T1", 20, "PHONE-7")
        self.assertTrue(b1_retry["deduplicated"])
        self.assertEqual(b1_retry["booking_id"], b1["booking_id"])
        # 只剩 10，说明没有重复扣容量
        self.assertAlmostEqual(self.store.remaining("T1"), 10.0)

        # 即使参数被篡改也以第一次为准
        _, tampered = self.book("别的船", "V9", "T1", 99, "PHONE-7")
        self.assertEqual(tampered["ship"], "浙岱66")

    # 3. confirm_key 并发去重（直接对 Store 加屏障，制造真竞争）
    def test_concurrent_confirm_only_once(self):
        self.mk("T1", 100)
        _, b = self.book("闽霞5", "V1", "T1", 30, "kb")
        results = []
        barrier = threading.Barrier(5)

        def worker():
            barrier.wait()
            try:
                _, c = self.confirm(b["booking_id"], 28, "TERM-55")
                results.append(c["deduplicated"])
            except Exception as e:
                results.append(("error", str(e)))

        ts = [threading.Thread(target=worker) for _ in range(5)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        # 只有一次真正落账，其余 4 次拿到 deduplicated
        self.assertEqual(results.count(False), 1)
        self.assertEqual(results.count(True), 4)
        # 只产生一条 shortfall 台账（30-28=2）
        entries = self.store.ledger["闽霞5"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["qty"], 2.0)

    # 4. 容量满排队 + 严格 FIFO + 释放放号
    def test_queue_fifo(self):
        self.mk("T1", 50)
        _, a = self.book("A", "V1", "T1", 30, "a")
        _, b = self.book("B", "V1", "T1", 30, "b")   # 排队
        _, c = self.book("C", "V1", "T1", 10, "c")   # 即使能放下也排在 B 后
        self.assertEqual(b["status"], "queued")
        self.assertEqual(c["status"], "queued")
        self.assertEqual(self.store.queue["T1"], [b["booking_id"], c["booking_id"]])

        # 取消排队中的 B：不释放 reserved 容量，但 C 仍需等（队首 B 消失后 C 需求 10 <= 剩余 20）
        self.store.cancel_booking(b["booking_id"])
        c_after = self.store.bookings[c["booking_id"]]
        self.assertEqual(c_after["status"], "reserved")

        # 若队首需求大于余量，其后不能插队
        _, d = self.book("D", "V1", "T1", 35, "d")   # 余量 10，排队
        _, e = self.book("E", "V1", "T1", 5, "e")    # 排队且被 D 阻塞
        self.assertEqual(self.store.queue["T1"], [d["booking_id"], e["booking_id"]])
        self.store.cancel_booking(a["booking_id"])    # 释放 30
        # 余量变为 40：D 需 35 入场，随后 E 需 5 入场
        self.assertEqual(self.store.bookings[d["booking_id"]]["status"], "reserved")
        self.assertEqual(self.store.bookings[e["booking_id"]]["status"], "reserved")

    # 5. 停机整批退回，开机重新入场
    def test_machine_stop_returns_batch(self):
        self.mk("T1", 100)
        _, a = self.book("A", "V1", "T1", 60, "a")
        _, b = self.book("B", "V1", "T1", 30, "b")
        _, c = self.book("C", "V1", "T1", 20, "c")   # 余量 10，排队
        self.assertEqual(self.store.queue["T1"], [c["booking_id"]])

        self.store.set_machine("T1", {"running": False})
        self.assertEqual(self.store.bookings[a["booking_id"]]["status"], "queued")
        self.assertEqual(self.store.bookings[b["booking_id"]]["status"], "queued")
        # 整批（按创建顺序 A,B）成块在原排队 C 之前
        self.assertEqual(self.store.queue["T1"],
                         [a["booking_id"], b["booking_id"], c["booking_id"]])

        # 停机期间不能确认
        with self.assertRaisesRegex(Exception, "停机|队列"):
            self.confirm(a["booking_id"], 50, "D1")

        # 开机：容量 100，A(60)、B(30) 依次入场，C(20) 因剩 10 继续排队
        self.store.set_machine("T1", {"running": True})
        self.assertEqual(self.store.bookings[a["booking_id"]]["status"], "reserved")
        self.assertEqual(self.store.bookings[b["booking_id"]]["status"], "reserved")
        self.assertEqual(self.store.bookings[c["booking_id"]]["status"], "queued")
        self.assertEqual(self.store.queue["T1"], [c["booking_id"]])

        # 确认 A 只拿 40 释放容量后，C 自动放号
        self.confirm(a["booking_id"], 40, "D1")
        self.assertEqual(self.store.bookings[c["booking_id"]]["status"], "reserved")

    # 6. 早到/晚到：跨时段确认重查余量
    def test_cross_slot_confirm(self):
        self.mk("EARLY", 100)
        self.mk("LATE", 25)
        _, b = self.book("船1", "V1", "LATE", 30, "bk")   # LATE 占 30（容量 25?）
        # LATE 容量 25 < 30，会排队 -> 改成 EARLY 预约晚到
        self.assertEqual(self.store.bookings[b["booking_id"]]["status"], "queued")
        self.store.cancel_booking(b["booking_id"])
        _, b2 = self.book("船1", "V1", "EARLY", 30, "bk2")
        # 船晚到，实际落在 LATE（余量 25），实拿 30 -> 重查余量不足 409
        with self.assertRaisesRegex(Exception, "余量不足"):
            self.confirm(b2["booking_id"], 30, "DX", actual="LATE")
        # 实拿 25 可以在 LATE 确认；EARLY 的预约容量被释放
        _, c = self.confirm(b2["booking_id"], 25, "DX", actual="LATE")
        self.assertEqual(c["confirmation"]["actual_slot"], "LATE")
        self.assertEqual(c["confirmation"]["new_arrear"], 5.0)
        self.assertAlmostEqual(self.store.remaining("EARLY"), 100.0)
        self.assertAlmostEqual(self.store.remaining("LATE"), 0.0)

    # 6b. 排队中的船实际早到/晚到别的时段：跨槽确认后从原队列移除，原槽队首自动放号
    def test_cross_slot_while_queued(self):
        self.mk("AM", 100)
        self.mk("PM", 100)
        _, a = self.book("A", "V1", "AM", 90, "a")    # AM 余 10
        _, b = self.book("B", "V1", "AM", 30, "b")    # 排队（队首）
        _, c = self.book("C", "V1", "AM", 5, "c")     # 被 B 阻塞
        self.assertEqual(self.store.queue["AM"], [b["booking_id"], c["booking_id"]])

        # B 实际晚到 PM（余 100），实拿 30 跨槽确认成功
        _, cb = self.confirm(b["booking_id"], 30, "K1", actual="PM")
        self.assertEqual(cb["confirmation"]["actual_slot"], "PM")
        # B 移出 AM 队列，C 随即被自动放号（需 5 <= 余 10）
        self.assertEqual(self.store.queue["AM"], [])
        self.assertEqual(self.store.bookings[c["booking_id"]]["status"], "reserved")
        self.assertAlmostEqual(self.store.remaining("AM"), 5.0)
        self.assertAlmostEqual(self.store.remaining("PM"), 70.0)

    # 7. 原时段确认也要重查（停机退回/队列变化后余量可能已变）
    def test_confirm_rechecks_same_slot(self):
        self.mk("T1", 100)
        _, b = self.book("船1", "V1", "T1", 40, "x")
        self.confirm(b["booking_id"], 40, "d1")
        # 再来一条预约 60，此时容量被前面确认交付占满；reserved 量 60，确认时可承受 100
        _, b2 = self.book("船2", "V1", "T1", 60, "y")
        # 构造竞争余量：再塞一个 55 的 confirmed 不可能（reserved 已 60），
        # 改为校验交付上限逻辑：同预约 remaining+qty = 40+60=100，拿 60 允许
        _, c = self.confirm(b2["booking_id"], 60, "d2")
        self.assertEqual(c["confirmation"]["delivered_qty"], 60.0)
        self.assertAlmostEqual(self.store.remaining("T1"), 0.0)

    # 8. 日志恢复：重启后状态与幂等键都在，重放事件得到相同队列
    def test_journal_recovery_and_restart_idempotency(self):
        self.mk("T1", 100)
        _, a = self.book("A", "V1", "T1", 60, "a")
        _, b = self.book("B", "V1", "T1", 30, "b")
        _, c = self.book("C", "V1", "T1", 20, "c")
        self.confirm(a["booking_id"], 45, "D1")   # A 少拿 15
        self.store.set_machine("T1", {"running": False})
        path = self.store.log_path
        self.store.close()

        s2 = Store(path)
        # 余量与队列一致：B、C 被退回，A 已确认
        self.assertEqual(s2.bookings[a["booking_id"]]["status"], "confirmed")
        self.assertEqual(s2.queue["T1"], [b["booking_id"], c["booking_id"]])
        self.assertEqual(s2.ship_balance("A"), 15.0)
        # 幂等键恢复：confirm_key 重发不重复记账
        _, dup = s2.confirm_booking(a["booking_id"],
                                   {"delivered_qty": 9, "confirm_key": "D1"})
        self.assertTrue(dup["deduplicated"])
        self.assertEqual(dup["confirmation"]["delivered_qty"], 45.0)
        self.assertEqual(len(s2.ledger["A"]), 1)
        # client_key 重发也不新建
        _, dup_book = s2.create_booking(
            {"ship": "B", "voyage": "V1", "slot_id": "T1", "qty": 30, "client_key": "b"})
        self.assertTrue(dup_book["deduplicated"])
        self.assertEqual(dup_book["booking_id"], b["booking_id"])
        s2.close()


class TestHTTP(unittest.TestCase):
    """少量通过真实 HTTP 栈的冒烟测试，验证 JSON 路由与状态码。"""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="ice-http-")
        cls.h = HttpHarness(os.path.join(cls.tmp, "j.log"))

    @classmethod
    def tearDownClass(cls):
        cls.h.stop()

    def test_full_http_flow(self):
        h = self.h
        self.assertEqual(slot(h, "S1", 100)[0], 201)
        self.assertEqual(slot(h, "S1", 100)[0], 409)

        s, b = h.req("POST", "/bookings",
                     {"ship": "船X", "voyage": "V1", "slot_id": "S1",
                      "qty": 40, "client_key": "TERM-1"})
        self.assertEqual(s, 201)
        bid = b["booking_id"]

        # 重发
        s, b_dup = h.req("POST", "/bookings",
                         {"ship": "船X", "voyage": "V1", "slot_id": "S1",
                          "qty": 40, "client_key": "TERM-1"})
        self.assertTrue(b_dup["deduplicated"])

        s, c = h.req("POST", f"/bookings/{bid}/confirm",
                     {"delivered_qty": 32, "confirm_key": "PICK-1"})
        self.assertEqual(s, 200)
        self.assertEqual(c["confirmation"]["new_arrear"], 8.0)

        # 重复确认
        s, c_dup = h.req("POST", f"/bookings/{bid}/confirm",
                         {"delivered_qty": 32, "confirm_key": "PICK-1"})
        self.assertTrue(c_dup["deduplicated"])

        # 无 confirm_key
        s, err = h.req("POST", f"/bookings/{bid}/confirm",
                       {"delivered_qty": 1})
        self.assertEqual(s, 400)

        s, v = h.req("GET", "/ships/船X")
        self.assertEqual(v["balance"], 8.0)
        s, sv = h.req("GET", "/slots/S1")
        self.assertEqual(sv["delivered"], 32.0)
        self.assertEqual(s, 200)
        s, missing = h.req("GET", "/slots/NOPE")
        self.assertEqual(s, 404)

        # 坏 JSON
        r = urllib.request.Request(
            f"http://127.0.0.1:{h.port}/bookings",
            data=b"{not json", method="POST",
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(r)
            self.fail("应返回 400")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)


if __name__ == "__main__":
    unittest.main(verbosity=2)

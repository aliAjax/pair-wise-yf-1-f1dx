"""端到端演示：覆盖排队、幂等确认、结欠、离线重发、制冰机批量回退。

直接用 HTTP 调用，验证服务行为。以子进程方式启动服务。
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import quote

BASE = "http://127.0.0.1:8765"


def q(boat):
    return quote(boat)


def req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = Request(BASE + path, data=data, method=method,
                headers={"Content-Type": "application/json"})
    try:
        with urlopen(r, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_ready(timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with urlopen(BASE + "/health", timeout=1) as resp:
                if resp.status == 200:
                    return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("服务启动超时")


def section(title):
    print(f"\n{'='*60}\n{title}\n{'='*60}")


def main():
    tmp = tempfile.mktemp(suffix=".db")
    proc = subprocess.Popen(
        [sys.executable, "-m", "ice_dock", "--db", tmp, "--port", "8765"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    try:
        wait_ready()

        section("0. 健康检查")
        print(req("GET", "/health"))

        section("1. 建时段（容量 100kg）")
        print(req("POST", "/slots", {"slot_id": "S1", "slot_time": "08:00-10:00", "capacity": 100}))
        print(req("POST", "/slots", {"slot_id": "S2", "slot_time": "10:00-12:00", "capacity": 100}))

        section("2. 建预约：A 约 60kg(占容量)，B 约 60kg(满了排队)")
        print(req("POST", "/appointments", {
            "boat_owner": "船主A", "slot_id": "S1", "amount": 60,
            "idempotency_key": "apt-a-001", "voyage_id": "V1"}))
        print(req("POST", "/appointments", {
            "boat_owner": "船主B", "slot_id": "S1", "amount": 60,
            "idempotency_key": "apt-b-001", "voyage_id": "V1"}))
        print("队列:", req("GET", "/queue")[1])
        print("S1 容量:", req("GET", "/slots")[1]["slots"][0])

        section("3. 船主A 确认取冰：实拿 50kg，少拿 10kg 结欠")
        aid = req("GET", f"/appointments?boat_owner={q('船主A')}")[1]["appointments"][0]["appointment_id"]
        print(req("POST", "/confirmations", {
            "appointment_id": aid, "actual_amount": 50, "idempotency_key": "cnf-a-001"}))
        print("船主A 结欠:", req("GET", f"/balances/{q('船主A')}")[1])
        print("S1 容量(释放后):", req("GET", "/slots")[1]["slots"][0])
        print("队列(补位):", req("GET", "/queue")[1])

        section("4. 幂等：两个码头员同时提交同一条确认，只记一次")
        results = []
        def worker():
            results.append(req("POST", "/confirmations", {
                "appointment_id": aid, "actual_amount": 50,
                "idempotency_key": "cnf-a-001"}))
        threads = [__import__("threading").Thread(target=worker) for _ in range(5)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        print("5 个并发确认结果(应全为 duplicated=True):")
        for code, body in results:
            print(f"  {code} duplicated={body.get('duplicated')}")
        print("确认记录数:", len(req("GET", "/confirmations")[1]["confirmations"]))

        section("5. 离线：断网时预约照常记，恢复后重发不重复占容量")
        print("断网:", req("POST", "/network/down")[1])
        print("断网建预约 C:", req("POST", "/appointments", {
            "boat_owner": "船主C", "slot_id": "S2", "amount": 30,
            "idempotency_key": "apt-c-offline", "voyage_id": "V1"}))
        print("outbox 待同步:", len([o for o in req("GET", "/outbox")[1]["outbox"] if o["status"] == "pending"]))
        print("S2 容量(本地已占):", req("GET", "/slots")[1]["slots"][1])
        print("恢复网络:", req("POST", "/network/up")[1])
        time.sleep(1.5)
        print("outbox 已同步:", len([o for o in req("GET", "/outbox")[1]["outbox"] if o["status"] == "synced"]))
        print("手动重发:", req("POST", "/outbox/replay")[1])
        print("S2 容量(重发后不变):", req("GET", "/slots")[1]["slots"][1])

        section("6. 制冰机停机：正在处理的预约整批退回队列")
        print(req("POST", "/appointments", {
            "boat_owner": "船主D", "slot_id": "S2", "amount": 20,
            "idempotency_key": "apt-d-001", "voyage_id": "V2"}))
        print(req("POST", "/appointments", {
            "boat_owner": "船主E", "slot_id": "S2", "amount": 15,
            "idempotency_key": "apt-e-001", "voyage_id": "V2"}))
        did = req("GET", f"/appointments?boat_owner={q('船主D')}")[1]["appointments"][0]["appointment_id"]
        eid = req("GET", f"/appointments?boat_owner={q('船主E')}")[1]["appointments"][0]["appointment_id"]
        print("开始处理 D:", req("POST", "/confirmations/start", {"appointment_id": did})[1]["appointment"]["status"])
        print("开始处理 E:", req("POST", "/confirmations/start", {"appointment_id": eid})[1]["appointment"]["status"])
        print("停机:", req("POST", "/ice-maker/stop")[1])
        print("D 状态:", req("GET", f"/appointments/{did}")[1]["status"])
        print("E 状态:", req("GET", f"/appointments/{eid}")[1]["status"])
        print("队列:", [a["boat_owner"] for a in req("GET", "/queue")[1]["queue"]])

        section("7. 结欠结转下一航次")
        print("船主A 当前结欠:", req("GET", f"/balances/{q('船主A')}")[1])
        print("（结欠按船主累计，下一航次预约取冰时可抵扣）")

        section("演示完成")
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        if os.path.exists(tmp):
            os.unlink(tmp)


if __name__ == "__main__":
    main()

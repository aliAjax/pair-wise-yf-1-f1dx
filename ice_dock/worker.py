"""outbox 后台同步线程。

断网时暂停重发（本地照常记），恢复后重发。
重发走业务层的幂等逻辑，唯一约束保证不重复占容量。
"""
import threading

from .db import get_db


class OutboxWorker(threading.Thread):
    daemon = True

    def __init__(self, db_path, service, interval=1.0):
        super().__init__(name="outbox-worker")
        self.db_path = db_path
        self.service = service
        self.interval = interval
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            try:
                self._drain()
            except Exception as e:
                print(f"[outbox] drain error: {e}")
            self._stop.wait(self.interval)

    def _drain(self):
        conn = get_db(self.db_path)
        try:
            net = conn.execute("SELECT status FROM network WHERE id = 1").fetchone()
            if not net or net["status"] != "up":
                return  # 断网：暂停重发，本地照常记
        finally:
            conn.close()
        self.service.replay_outbox()

    def stop(self):
        self._stop.set()

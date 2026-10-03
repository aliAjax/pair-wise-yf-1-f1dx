"""业务逻辑层：预约、确认取冰、结欠、排队、制冰机/网络状态。

所有方法都是线程安全的：写操作在 ``BEGIN IMMEDIATE`` 事务内完成，
并依赖 appointments.confirmations 的唯一约束做幂等。
"""
import json
import uuid
from datetime import datetime, timezone

from .db import get_db, ICE_MAKER_ROW_ID, NETWORK_ROW_ID


# ---------------------------------------------------------------------------
# 领域异常
# ---------------------------------------------------------------------------
class NotFound(Exception):
    pass


class BadRequest(Exception):
    pass


class Conflict(Exception):
    pass


def _now():
    return datetime.now(timezone.utc).isoformat()


def _uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


class IceDockService:
    def __init__(self, db_path):
        self.db_path = db_path

    # ------------------------------------------------------------------ slots
    def create_slot(self, slot_id, slot_time, capacity):
        if capacity <= 0:
            raise BadRequest("容量必须为正")
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO slots(slot_id, slot_time, capacity, occupied) VALUES(?, ?, ?, 0)",
                (slot_id, slot_time, capacity),
            )
            conn.commit()
            return self.get_slot(slot_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_slots(self):
        conn = get_db(self.db_path)
        try:
            rows = conn.execute(
                "SELECT *, (capacity - occupied) AS available FROM slots ORDER BY slot_time"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_slot(self, slot_id):
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT *, (capacity - occupied) AS available FROM slots WHERE slot_id = ?",
                (slot_id,),
            ).fetchone()
            if not row:
                raise NotFound(f"时段不存在: {slot_id}")
            return dict(row)
        finally:
            conn.close()

    # ------------------------------------------------------------ appointments
    def create_appointment(self, boat_owner, slot_id, amount, idempotency_key, voyage_id=None):
        """建预约。容量满则先排队(queued)，不占容量。

        幂等：同一 idempotency_key 重发直接返回已有预约，不重复占容量。
        """
        if amount <= 0:
            raise BadRequest("预约量必须为正")
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            # 幂等：同 key 直接返回已有预约
            existing = conn.execute(
                "SELECT * FROM appointments WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing:
                conn.rollback()
                return dict(existing), True

            slot = conn.execute("SELECT * FROM slots WHERE slot_id = ?", (slot_id,)).fetchone()
            if not slot:
                raise NotFound(f"时段不存在: {slot_id}")

            now = _now()
            appointment_id = _uid("APT")

            if slot["occupied"] + amount <= slot["capacity"]:
                status = "booked"
                conn.execute(
                    "UPDATE slots SET occupied = occupied + ? WHERE slot_id = ?",
                    (amount, slot_id),
                )
            else:
                status = "queued"

            conn.execute(
                """INSERT INTO appointments
                   (appointment_id, idempotency_key, boat_owner, slot_id, booked_amount,
                    status, voyage_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (appointment_id, idempotency_key, boat_owner, slot_id, amount,
                 status, voyage_id, now, now),
            )

            # 写 outbox：断网照常记，恢复后重发
            self._write_outbox(conn, idempotency_key, "appointment", {
                "boat_owner": boat_owner,
                "slot_id": slot_id,
                "amount": amount,
                "voyage_id": voyage_id,
            })

            conn.commit()
            row = conn.execute(
                "SELECT * FROM appointments WHERE appointment_id = ?", (appointment_id,)
            ).fetchone()
            return dict(row), False
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_appointments(self, boat_owner=None, slot_id=None, status=None):
        sql = "SELECT * FROM appointments WHERE 1=1"
        args = []
        if boat_owner:
            sql += " AND boat_owner = ?"
            args.append(boat_owner)
        if slot_id:
            sql += " AND slot_id = ?"
            args.append(slot_id)
        if status:
            sql += " AND status = ?"
            args.append(status)
        sql += " ORDER BY created_at"
        conn = get_db(self.db_path)
        try:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
        finally:
            conn.close()

    def get_appointment(self, appointment_id):
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT * FROM appointments WHERE appointment_id = ?", (appointment_id,)
            ).fetchone()
            if not row:
                raise NotFound(f"预约不存在: {appointment_id}")
            return dict(row)
        finally:
            conn.close()

    # ----------------------------------------------------------- confirmations
    def start_confirmation(self, appointment_id):
        """开始处理：booked -> processing（制冰机停机时这批会被整批退回队列）。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            appt = conn.execute(
                "SELECT * FROM appointments WHERE appointment_id = ?", (appointment_id,)
            ).fetchone()
            if not appt:
                raise NotFound(f"预约不存在: {appointment_id}")
            if appt["status"] == "processing":
                conn.rollback()
                return dict(appt)
            if appt["status"] != "booked":
                raise Conflict(f"预约状态不允许开始处理: {appt['status']}")
            now = _now()
            conn.execute(
                "UPDATE appointments SET status = 'processing', updated_at = ? WHERE appointment_id = ?",
                (now, appointment_id),
            )
            conn.commit()
            return self.get_appointment(appointment_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def complete_confirmation(self, appointment_id, actual_amount, idempotency_key,
                              actual_slot_id=None):
        """完成取冰确认：重查余量、记录实拿、差额结欠、释放容量。

        幂等：同一 idempotency_key 或同一 appointment 的确认只记一次。
        """
        if actual_amount < 0:
            raise BadRequest("实拿量不能为负")
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            # 幂等：同 key 或同预约的确认只记一次
            existing = conn.execute(
                "SELECT * FROM confirmations WHERE idempotency_key = ? OR appointment_id = ?",
                (idempotency_key, appointment_id),
            ).fetchone()
            if existing:
                conn.rollback()
                return dict(existing), True

            appt = conn.execute(
                "SELECT * FROM appointments WHERE appointment_id = ?", (appointment_id,)
            ).fetchone()
            if not appt:
                raise NotFound(f"预约不存在: {appointment_id}")
            if appt["status"] == "confirmed":
                raise Conflict("预约已确认，不能重复确认")
            if appt["status"] != "processing":
                raise Conflict(f"预约未在处理中: {appt['status']}")

            # 制冰机停机：正在处理的预约整批退回队列
            ice = conn.execute(
                "SELECT status FROM ice_maker WHERE id = ?", (ICE_MAKER_ROW_ID,)
            ).fetchone()
            if ice["status"] != "running":
                now = _now()
                conn.execute(
                    "UPDATE appointments SET status = 'queued', updated_at = ? WHERE appointment_id = ?",
                    (now, appointment_id),
                )
                conn.commit()
                raise Conflict("制冰机停机，预约已退回队列")

            # 重查余量：实际到港时段（早晚到会撞上原时段）
            eff_slot_id = actual_slot_id or appt["slot_id"]
            eff_slot = conn.execute(
                "SELECT * FROM slots WHERE slot_id = ?", (eff_slot_id,),
            ).fetchone()
            if not eff_slot:
                raise NotFound(f"时段不存在: {eff_slot_id}")

            shortfall = appt["booked_amount"] - actual_amount

            if eff_slot_id == appt["slot_id"]:
                # 同时段：释放差额占用；超量则需额外容量
                if shortfall > 0:
                    conn.execute(
                        "UPDATE slots SET occupied = occupied - ? WHERE slot_id = ?",
                        (shortfall, eff_slot_id),
                    )
                elif shortfall < 0:
                    extra = -shortfall
                    if eff_slot["occupied"] + extra > eff_slot["capacity"]:
                        raise Conflict("实际时段容量不足，无法超量取冰")
                    conn.execute(
                        "UPDATE slots SET occupied = occupied + ? WHERE slot_id = ?",
                        (extra, eff_slot_id),
                    )
            else:
                # 跨时段：释放原时段，占用实际时段
                conn.execute(
                    "UPDATE slots SET occupied = occupied - ? WHERE slot_id = ?",
                    (appt["booked_amount"], appt["slot_id"]),
                )
                if eff_slot["occupied"] + actual_amount > eff_slot["capacity"]:
                    raise Conflict("实际时段容量不足")
                conn.execute(
                    "UPDATE slots SET occupied = occupied + ? WHERE slot_id = ?",
                    (actual_amount, eff_slot_id),
                )

            # 记录确认
            now = _now()
            confirmation_id = _uid("CNF")
            conn.execute(
                """INSERT INTO confirmations
                   (confirmation_id, idempotency_key, appointment_id, actual_slot_id,
                    actual_amount, shortfall, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (confirmation_id, idempotency_key, appointment_id, eff_slot_id,
                 actual_amount, shortfall, now),
            )

            # 少拿的差额结到下一航次（结欠按船主累计，自然结转）
            if shortfall > 0:
                conn.execute(
                    """INSERT INTO balances (boat_owner, amount) VALUES (?, ?)
                       ON CONFLICT(boat_owner) DO UPDATE SET amount = amount + ?""",
                    (appt["boat_owner"], shortfall, shortfall),
                )

            # 预约完成
            conn.execute(
                "UPDATE appointments SET status = 'confirmed', actual_amount = ?, updated_at = ? WHERE appointment_id = ?",
                (actual_amount, now, appointment_id),
            )

            # 写 outbox
            self._write_outbox(conn, idempotency_key, "confirmation", {
                "appointment_id": appointment_id,
                "actual_amount": actual_amount,
                "actual_slot_id": eff_slot_id,
                "shortfall": shortfall,
            })

            conn.commit()

            # 释放容量后补排队列（独立事务）
            self._promote_waitlist(eff_slot_id)
            if eff_slot_id != appt["slot_id"]:
                self._promote_waitlist(appt["slot_id"])

            row = conn.execute(
                "SELECT * FROM confirmations WHERE confirmation_id = ?", (confirmation_id,)
            ).fetchone()
            return dict(row), False
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def confirm_pickup(self, appointment_id, actual_amount, idempotency_key,
                       actual_slot_id=None):
        """便捷确认：若仍是 booked 则先开始处理，再完成确认。幂等。"""
        appt = self.get_appointment(appointment_id)
        if appt["status"] == "booked":
            self.start_confirmation(appointment_id)
        return self.complete_confirmation(
            appointment_id, actual_amount, idempotency_key, actual_slot_id
        )

    def list_confirmations(self, boat_owner=None):
        sql = """SELECT c.*, a.boat_owner, a.voyage_id
                 FROM confirmations c JOIN appointments a USING(appointment_id)"""
        args = []
        if boat_owner:
            sql += " WHERE a.boat_owner = ?"
            args.append(boat_owner)
        sql += " ORDER BY c.created_at"
        conn = get_db(self.db_path)
        try:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
        finally:
            conn.close()

    # ---------------------------------------------------------------- balances
    def get_balance(self, boat_owner):
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT boat_owner, amount FROM balances WHERE boat_owner = ?",
                (boat_owner,),
            ).fetchone()
            if not row:
                return {"boat_owner": boat_owner, "amount": 0.0}
            return dict(row)
        finally:
            conn.close()

    def list_balances(self):
        conn = get_db(self.db_path)
        try:
            return [dict(r) for r in conn.execute(
                "SELECT boat_owner, amount FROM balances ORDER BY boat_owner"
            ).fetchall()]
        finally:
            conn.close()

    # ------------------------------------------------------------------- queue
    def list_queue(self):
        conn = get_db(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM appointments WHERE status = 'queued' ORDER BY created_at"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def _promote_waitlist(self, slot_id):
        """容量释放后，按 FIFO 补排队列（能放下的先补）。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            slot = conn.execute("SELECT * FROM slots WHERE slot_id = ?", (slot_id,)).fetchone()
            if not slot:
                conn.rollback()
                return
            queued = conn.execute(
                "SELECT * FROM appointments WHERE slot_id = ? AND status = 'queued' ORDER BY created_at",
                (slot_id,),
            ).fetchall()
            now = _now()
            occupied = slot["occupied"]
            for q in queued:
                if occupied + q["booked_amount"] <= slot["capacity"]:
                    conn.execute(
                        "UPDATE appointments SET status = 'booked', updated_at = ? WHERE appointment_id = ?",
                        (now, q["appointment_id"]),
                    )
                    conn.execute(
                        "UPDATE slots SET occupied = occupied + ? WHERE slot_id = ?",
                        (q["booked_amount"], slot_id),
                    )
                    occupied += q["booked_amount"]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # -------------------------------------------------------------- ice maker
    def ice_maker_status(self):
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT status, updated_at FROM ice_maker WHERE id = ?", (ICE_MAKER_ROW_ID,)
            ).fetchone()
            return dict(row) if row else {"status": "unknown"}
        finally:
            conn.close()

    def ice_maker_start(self):
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = _now()
            conn.execute(
                "UPDATE ice_maker SET status = 'running', updated_at = ? WHERE id = ?",
                (now, ICE_MAKER_ROW_ID),
            )
            conn.commit()
            return self.ice_maker_status()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ice_maker_stop(self):
        """停机：正在处理(processing)的预约整批退回队列。"""
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = _now()
            result = conn.execute(
                "UPDATE appointments SET status = 'queued', updated_at = ? WHERE status = 'processing'",
                (now,),
            )
            returned = result.rowcount
            conn.execute(
                "UPDATE ice_maker SET status = 'stopped', updated_at = ? WHERE id = ?",
                (now, ICE_MAKER_ROW_ID),
            )
            conn.commit()
            return {"status": "stopped", "returned_to_queue": returned}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ----------------------------------------------------------------- network
    def network_status(self):
        conn = get_db(self.db_path)
        try:
            row = conn.execute(
                "SELECT status, updated_at FROM network WHERE id = ?", (NETWORK_ROW_ID,)
            ).fetchone()
            return dict(row) if row else {"status": "unknown"}
        finally:
            conn.close()

    def network_up(self):
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = _now()
            conn.execute(
                "UPDATE network SET status = 'up', updated_at = ? WHERE id = ?",
                (now, NETWORK_ROW_ID),
            )
            conn.commit()
            return self.network_status()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def network_down(self):
        conn = get_db(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            now = _now()
            conn.execute(
                "UPDATE network SET status = 'down', updated_at = ? WHERE id = ?",
                (now, NETWORK_ROW_ID),
            )
            conn.commit()
            return self.network_status()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------ outbox
    def _write_outbox(self, conn, idempotency_key, kind, payload):
        outbox_id = _uid("OUT")
        now = _now()
        conn.execute(
            """INSERT INTO outbox (outbox_id, idempotency_key, kind, payload, status, created_at)
               VALUES (?, ?, ?, ?, 'pending', ?)""",
            (outbox_id, idempotency_key, kind,
             json.dumps(payload, ensure_ascii=False), now),
        )

    def list_outbox(self, status=None):
        conn = get_db(self.db_path)
        try:
            if status:
                rows = conn.execute(
                    "SELECT * FROM outbox WHERE status = ? ORDER BY created_at", (status,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM outbox ORDER BY created_at").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def replay_outbox(self):
        """重发待同步条目。幂等：唯一约束保证不重复占容量。返回成功条数。"""
        conn = get_db(self.db_path)
        try:
            pending = conn.execute(
                "SELECT * FROM outbox WHERE status = 'pending' ORDER BY created_at"
            ).fetchall()
        finally:
            conn.close()
        count = 0
        for row in pending:
            try:
                self._replay_one(row)
                count += 1
            except Exception as e:
                print(f"[outbox] replay {row['outbox_id']} failed: {e}")
        return count

    def _replay_one(self, row):
        kind = row["kind"]
        payload = json.loads(row["payload"])
        if kind == "appointment":
            self.create_appointment(
                payload["boat_owner"], payload["slot_id"], payload["amount"],
                row["idempotency_key"], payload.get("voyage_id"),
            )
        elif kind == "confirmation":
            self.complete_confirmation(
                payload["appointment_id"], payload["actual_amount"],
                row["idempotency_key"], payload.get("actual_slot_id"),
            )
        # 标记已同步
        conn = get_db(self.db_path)
        try:
            now = _now()
            conn.execute(
                "UPDATE outbox SET status = 'synced', synced_at = ? WHERE outbox_id = ? AND status = 'pending'",
                (now, row["outbox_id"]),
            )
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------ health
    def health(self):
        return {
            "ok": True,
            "ice_maker": self.ice_maker_status()["status"],
            "network": self.network_status()["status"],
        }

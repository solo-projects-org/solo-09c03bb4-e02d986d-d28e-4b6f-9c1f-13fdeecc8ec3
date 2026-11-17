"""线性校时计算与后台重算线程。

一致性设计：
- 任何上传/锚点变更都会递增 state_generation。
- 重算线程按一致快照计算，提交前复核 state_generation：
  若已变化（期间有新上传/更正），本次计算作废重算 —— 迟到的计算绝不提交，
  因此结果不会回退；校时版本号 calc_version 单调递增。
- 每次提交在单个事务内整体替换 calibrated 表：读数跨日时的“旧日移除 +
  新日加入”同时生效；计算或提交失败时旧版本原样保留，绝不提供半版。
- state_generation 持久化，服务重启后启动线程会比对代次并补算，
  所有已接纳且处于锚点范围的点最终都会进入当前版。
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from .db import Database
from .timeutil import fmt_utc, parse_utc, utcnow_iso

log = logging.getLogger("recalc")

# 对外发布的一条变更所含字段（顺序即展示顺序）
CHANGE_FIELDS = (
    "action", "reason", "reading_id", "batch_id", "row_no",
    "segment", "local_seconds", "temperature",
    "utc_time", "utc_date", "anchor_version",
)


class StaleState(Exception):
    """提交时状态代次已变化：本次计算过期，需丢弃重算。"""


def compute_calibration(readings, anchors: dict) -> list[dict]:
    """纯函数：对每枚落在锚点区间内的读数做线性校时。

    readings: 含 id/batch_id/row_no/segment/local_seconds/temperature 的行
    anchors:  segment -> 含 local1/_ts1/local2/_ts2/version 的 dict
    """
    out = []
    for r in readings:
        a = anchors.get(r["segment"])
        if a is None:
            continue
        if a["local1"] <= r["local_seconds"] <= a["local2"]:
            span = a["local2"] - a["local1"]
            frac = (r["local_seconds"] - a["local1"]) / span
            ts = a["_ts1"] + frac * (a["_ts2"] - a["_ts1"])
            ts = round(ts * 1000) / 1000  # 毫秒精度
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            out.append({
                "reading_id": r["id"],
                "batch_id": r["batch_id"],
                "row_no": r["row_no"],
                "segment": r["segment"],
                "local_seconds": r["local_seconds"],
                "temperature": r["temperature"],
                "utc_time": fmt_utc(dt),
                "utc_date": dt.date().isoformat(),
                "anchor_version": a["version"],
            })
    return out


def load_snapshot(db: Database):
    """在锁内一次读出 代次 + 全部读数 + 全部锚点，保证快照一致。"""
    def fn(conn):
        meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
        state_gen = int(meta.get("state_generation", "0"))
        calibrated_gen = int(meta.get("calibrated_generation", "-1"))
        readings = conn.execute(
            "SELECT id, batch_id, row_no, segment, local_seconds, temperature"
            " FROM readings ORDER BY id"
        ).fetchall()
        anchors = {}
        for a in conn.execute("SELECT * FROM anchors").fetchall():
            d = dict(a)
            d["_ts1"] = parse_utc(d["utc1"]).timestamp()
            d["_ts2"] = parse_utc(d["utc2"]).timestamp()
            anchors[d["segment"]] = d
        return state_gen, calibrated_gen, readings, anchors

    return db.snapshot(fn)


def _revision_reason(old: dict, new: dict) -> str:
    """解释一条已发布读数为何被替换（含跨日时下游须同时清理旧日桶）。"""
    reasons = []
    if old["utc_date"] != new["utc_date"]:
        reasons.append("date_changed")
    if old["utc_time"] != new["utc_time"]:
        reasons.append("utc_changed")
    if old["anchor_version"] != new["anchor_version"]:
        reasons.append("anchor_version_changed")
    return "+".join(reasons) or "recalibrated"


def _change_row(action: str, reason: str, version: int, rec: dict) -> tuple:
    return (
        version, action, reason,
        rec["reading_id"], rec["utc_date"], rec["utc_time"], rec["anchor_version"],
        rec["batch_id"], rec["row_no"], rec["segment"], rec["local_seconds"],
        rec["temperature"],
    )


def diff_published(old_rows: dict[int, dict], new_rows: dict[int, dict],
                   anchors: dict[int, dict]) -> list[dict]:
    """比较上一已发布版本与本次计算结果，产出逐条撤销 / 新增记录。

    - 旧有新无（退出锚点范围或该段锚点被移除）→ 撤销旧记录；
    - 新旧都有但 UTC 时刻或锚点版本变化（含跨日）→ 撤销旧值 + 新增新值，
      下游旧日桶因此不会残留失效点；
    - 新增读数 → 新增记录。
    仅 UTC 与锚点版本都相同的读数不产生变更。
    """
    changes: list[dict] = []
    for rid, old in old_rows.items():
        new = new_rows.get(rid)
        if new is None:
            anchor = anchors.get(old["segment"])
            if anchor is None:
                reason = "no_anchor"
            elif not (anchor["local1"] <= old["local_seconds"] <= anchor["local2"]):
                reason = "out_of_anchor_range"
            else:
                # 理论上不可达：在范围内却未出现在结果中
                reason = "not_calibrated"
            changes.append({"action": "retract", "reason": reason, "rec": old})
        elif old["utc_time"] != new["utc_time"] or old["anchor_version"] != new["anchor_version"]:
            reason = _revision_reason(old, new)
            changes.append({"action": "retract", "reason": reason, "rec": old})
            changes.append({"action": "add", "reason": reason, "rec": new})
    for rid, new in new_rows.items():
        if rid not in old_rows:
            changes.append({"action": "add", "reason": "new", "rec": new})
    # 撤销在前、新增在后，便于下游按同一顺序应用
    changes.sort(key=lambda c: (0 if c["action"] == "retract" else 1, c["rec"]["reading_id"]))
    return changes


def commit_calc(db: Database, generation: int, rows: list[dict]) -> int:
    """单事务整体换版并发布本版变更。代次不符则抛 StaleState（不提交）。

    calibrated、calc_versions、calc_changes 与 calc_state 在同一事务内写入：
    失败时整体回滚，旧版本及其旧变更完整保留，绝不会出现半组变更。
    """
    with db.tx() as cur:
        cur_gen = int(
            cur.execute("SELECT value FROM meta WHERE key = 'state_generation'")
            .fetchone()["value"]
        )
        if cur_gen != generation:
            raise StaleState(f"代次已变化：{generation} -> {cur_gen}")

        old_rows: dict[int, dict] = {}
        for r in cur.execute(
            """
            SELECT c.reading_id, c.utc_time, c.utc_date, c.anchor_version,
                   r.batch_id, r.row_no, r.segment, r.local_seconds, r.temperature
            FROM calibrated c JOIN readings r ON r.id = c.reading_id
            """
        ).fetchall():
            old_rows[r["reading_id"]] = dict(r)
        new_rows = {r["reading_id"]: r for r in rows}
        anchors = {
            a["segment"]: dict(a)
            for a in cur.execute("SELECT segment, local1, local2 FROM anchors").fetchall()
        }
        changes = diff_published(old_rows, new_rows, anchors)

        new_version = cur.execute(
            "SELECT version FROM calc_state WHERE id = 1"
        ).fetchone()["version"] + 1
        created_at = utcnow_iso()

        cur.execute("DELETE FROM calibrated")
        cur.executemany(
            "INSERT INTO calibrated(reading_id, utc_time, utc_date, anchor_version, calc_version)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (r["reading_id"], r["utc_time"], r["utc_date"], r["anchor_version"], new_version)
                for r in rows
            ],
        )
        additions = sum(1 for c in changes if c["action"] == "add")
        retractions = len(changes) - additions
        cur.execute(
            "INSERT INTO calc_versions(version, created_at, state_generation, additions, retractions)"
            " VALUES (?, ?, ?, ?, ?)",
            (new_version, created_at, generation, additions, retractions),
        )
        cur.executemany(
            "INSERT INTO calc_changes(calc_version, action, reason, reading_id, utc_date,"
            " utc_time, anchor_version, batch_id, row_no, segment, local_seconds, temperature)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                _change_row(c["action"], c["reason"], new_version, c["rec"])
                for c in changes
            ],
        )
        cur.execute(
            "UPDATE calc_state SET version = ?, status = 'idle', last_error = '', updated_at = ?"
            " WHERE id = 1",
            (new_version, created_at),
        )
        cur.execute(
            "UPDATE meta SET value = ? WHERE key = 'calibrated_generation'", (str(generation),)
        )
    return new_version


def adopt_existing_version(db: Database) -> bool:
    """升级兼容：旧库已有已发布结果但没有变更账本时，把当前结果登记为版本。

    仅在该版本尚无 calc_versions 记录时执行一次；不推进校时版本号，
    登记内容为该版全部读数的新增变更（下游从 since=0 领取即可得到与
    「按日查询」一致的全量状态）。单事务完成，失败整体回滚。
    """
    with db.tx() as cur:
        state = cur.execute("SELECT version, status FROM calc_state WHERE id = 1").fetchone()
        version = state["version"]
        if version <= 0:
            return False
        done = cur.execute(
            "SELECT 1 FROM calc_versions WHERE version = ?", (version,)
        ).fetchone()
        if done:
            return False
        rows = cur.execute(
            """
            SELECT c.reading_id, c.utc_time, c.utc_date, c.anchor_version,
                   r.batch_id, r.row_no, r.segment, r.local_seconds, r.temperature
            FROM calibrated c JOIN readings r ON r.id = c.reading_id
            ORDER BY c.reading_id
            """
        ).fetchall()
        meta = {r["key"]: r["value"] for r in cur.execute("SELECT key, value FROM meta")}
        cur.execute(
            "INSERT INTO calc_versions(version, created_at, state_generation, additions, retractions)"
            " VALUES (?, ?, ?, ?, 0)",
            (version, utcnow_iso(), int(meta.get("calibrated_generation", "-1")), len(rows)),
        )
        cur.executemany(
            "INSERT INTO calc_changes(calc_version, action, reason, reading_id, utc_date,"
            " utc_time, anchor_version, batch_id, row_no, segment, local_seconds, temperature)"
            " VALUES (?, 'add', 'adopted', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    version, r["reading_id"], r["utc_date"], r["utc_time"],
                    r["anchor_version"], r["batch_id"], r["row_no"], r["segment"],
                    r["local_seconds"], r["temperature"],
                )
                for r in rows
            ],
        )
    return True


def set_status(db: Database, status: str, last_error: str = "") -> None:
    db.execute(
        "UPDATE calc_state SET status = ?, last_error = ?, updated_at = ? WHERE id = 1",
        (status, last_error, utcnow_iso()),
    )


class RecalcWorker:
    """后台重算线程：事件触发，循环重算直至 已校代次 == 状态代次。"""

    def __init__(self, db: Database, delay: float = 0.0, max_failures: int = 3):
        self.db = db
        self.delay = delay            # 提交前人为延迟，仅用于演示/测试并发窗口
        self.max_failures = max_failures
        self._event = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._failures = 0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="recalc-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

    def trigger(self) -> None:
        self._event.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._event.wait(timeout=10)  # 周期性兜底唤醒
            self._event.clear()
            if self._stop.is_set():
                break
            try:
                self.run_until_quiet()
            except Exception:  # pragma: no cover - 防御
                log.exception("重算循环出现异常")

    def run_until_quiet(self) -> None:
        """重算直至无待处理变更；失败重试 max_failures 次后保持失败状态等待下次触发。"""
        while not self._stop.is_set():
            state_gen, calibrated_gen, readings, anchors = load_snapshot(self.db)
            if calibrated_gen == state_gen:
                self._failures = 0
                return
            set_status(self.db, "running")
            try:
                rows = compute_calibration(readings, anchors)
                if self.delay > 0:
                    time.sleep(self.delay)  # 放大并发窗口（演示/测试）
                commit_calc(self.db, state_gen, rows)
                self._failures = 0
            except StaleState:
                log.info("状态已变化，丢弃过期计算并重算")
                continue
            except Exception as exc:  # 计算/提交失败：旧版原样保留
                self._failures += 1
                log.warning("重算失败（第 %s 次）：%s", self._failures, exc)
                set_status(self.db, "failed", str(exc)[:500])
                if self._failures >= self.max_failures:
                    return
                time.sleep(0.2)

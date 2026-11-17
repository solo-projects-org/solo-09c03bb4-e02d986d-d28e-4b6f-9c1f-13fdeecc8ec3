"""关键流程自测：上传、锚点、校时、跨日原子性、并发交错、重启恢复、失败不留半版。"""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

CSV_HEADER = "segment,local_seconds,temperature\n"


def make_client(tmp_path, monkeypatch, worker="0", delay="0", max_points=None) -> TestClient:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setenv("WORKER_ENABLED", worker)
    monkeypatch.setenv("RECALC_DELAY", delay)
    if max_points is not None:
        monkeypatch.setenv("MAX_POINTS", str(max_points))
    from app.main import create_app
    return TestClient(create_app())


@pytest.fixture()
def client(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as c:
        yield c


def upload(client: TestClient, rows, header: str | None = CSV_HEADER, filename="data.csv"):
    content = (header or "") + "".join(f"{s},{sec},{t}\n" for s, sec, t in rows)
    return client.post("/api/uploads", files={"file": (filename, content, "text/csv")})


def set_anchors(client: TestClient, seg, l1, u1, l2, u2, version=None):
    payload = {"segment": seg, "local1": l1, "utc1": u1, "local2": l2, "utc2": u2}
    if version is not None:
        payload["version"] = version
    return client.post("/api/anchors", json=payload)


def wait_idle(client: TestClient, timeout=10.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        calc = client.get("/api/status").json()["calc"]
        if calc["status"] != "running" and not calc["dirty"]:
            return calc
        time.sleep(0.05)
    raise AssertionError("重算未在超时内收敛")


# ---------------------------------------------------------------- 上传

def test_upload_happy(client):
    r = upload(client, [(1, 100, 4.5), (1, 101, 4.6), (2, 5, -2.0)])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["accepted"] == 3 and body["batch_id"] == 1
    st = client.get("/api/status").json()
    assert st["points"] == {"accepted": 3, "max": 2000}
    assert st["batches"][0]["rows_accepted"] == 3


def test_duplicate_within_file_rejected(client):
    r = upload(client, [(1, 100, 1.0), (1, 100, 2.0)])
    assert r.status_code == 400
    assert r.json()["code"] == "CSV_INVALID"
    assert any("重复" in e["message"] for e in r.json()["errors"])
    assert client.get("/api/status").json()["points"]["accepted"] == 0


def test_duplicate_against_db_rejected(client):
    assert upload(client, [(1, 100, 1.0)]).status_code == 200
    r = upload(client, [(1, 100, 9.9)])
    assert r.status_code == 400 and r.json()["code"] == "DUPLICATE_SECONDS"
    # 不同段同秒数允许
    assert upload(client, [(2, 100, 3.0)]).status_code == 200
    assert client.get("/api/status").json()["points"]["accepted"] == 2


def test_point_cap_enforced(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, max_points="5") as c:
        assert upload(c, [(1, i, 1.0) for i in range(4)]).status_code == 200
        r = upload(c, [(1, 10, 1.0), (1, 11, 1.0)])
        assert r.status_code == 400 and r.json()["code"] == "POINT_LIMIT_EXCEEDED"
        assert c.get("/api/status").json()["points"]["accepted"] == 4
        assert upload(c, [(1, 10, 1.0)]).status_code == 200
        assert c.get("/api/status").json()["points"]["accepted"] == 5


def test_bad_rows_reported_with_row_numbers(client):
    content = "segment,local_seconds,temperature\n1,100,abc\n2,x,1.0\n"
    r = client.post("/api/uploads", files={"file": ("b.csv", content, "text/csv")})
    assert r.status_code == 400
    assert {e["row"] for e in r.json()["errors"]} == {1, 2}
    assert client.get("/api/status").json()["points"]["accepted"] == 0


def test_csv_chinese_header_bom_and_no_header(client):
    content = "﻿开机段号,本地秒数,温度\n1,100,4.5\n"
    r = client.post("/api/uploads", files={"file": ("c.csv", content.encode("utf-8"), "text/csv")})
    assert r.status_code == 200, r.text
    r = client.post("/api/uploads",
                    files={"file": ("d.csv", "2,100,4.5\n2,101,4.6\n", "text/csv")})
    assert r.status_code == 200 and r.json()["accepted"] == 2


# ---------------------------------------------------------------- 锚点

def test_anchor_create_and_validation(client):
    r = set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    assert r.status_code == 200 and r.json()["version"] == 1
    # UTC 不递增
    r = set_anchors(client, 2, 0, "2026-01-02T00:00:00Z", 10, "2026-01-01T00:00:00Z")
    assert r.status_code == 400 and r.json()["code"] == "UTC_NOT_INCREASING"
    # 本地秒数不递增
    r = set_anchors(client, 2, 10, "2026-01-01T00:00:00Z", 10, "2026-01-02T00:00:00Z")
    assert r.status_code == 400 and r.json()["code"] == "BAD_LOCAL_RANGE"


def test_anchor_correction_requires_seen_version(client):
    assert set_anchors(client, 1, 100, "2026-01-01T00:00:00Z",
                     200, "2026-01-01T00:10:00Z").json()["version"] == 1
    # 未带版本
    r = set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:20:00Z")
    assert r.status_code == 409 and r.json()["current_version"] == 1
    # 版本不符
    r = set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:20:00Z",
                    version=3)
    assert r.status_code == 409 and r.json()["current_version"] == 1
    # 版本正确
    r = set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:20:00Z",
                    version=1)
    assert r.status_code == 200 and r.json()["version"] == 2
    # 对不存在的段按版本更正
    r = set_anchors(client, 9, 0, "2026-01-01T00:00:00Z", 10, "2026-01-01T00:10:00Z",
                    version=1)
    assert r.status_code == 409 and r.json()["current_version"] is None


# ---------------------------------------------------------------- 校时

def test_calibration_math_and_pending(client):
    upload(client, [(1, 99, 1.0), (1, 100, 2.0), (1, 150, 3.0),
                    (1, 200, 4.0), (1, 201, 5.0), (2, 100, 6.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    body = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert body["count"] == 3
    times = {row["local_seconds"]: row["utc_time"] for row in body["readings"]}
    assert times[100] == "2026-01-01T00:00:00.000Z"
    assert times[150] == "2026-01-01T00:05:00.000Z"   # 线性中点
    assert times[200] == "2026-01-01T00:10:00.000Z"
    # 查询结果须含批次号、原行号、锚点版本
    for row in body["readings"]:
        assert row["batch_id"] == 1
        assert row["anchor_version"] == 1
        assert row["row_no"] >= 1
    rows_by_sec = {row["local_seconds"]: row["row_no"] for row in body["readings"]}
    assert rows_by_sec == {100: 2, 150: 3, 200: 4}
    # 范围外与无锚点的读数留存待校时
    pend = client.get("/api/pending").json()
    got = sorted((p["segment"], p["local_seconds"], p["reason"]) for p in pend["pending"])
    assert [(s, sec) for s, sec, _ in got] == [(1, 99), (1, 201), (2, 100)]
    assert "未设置" in got[2][2] and "超出" in got[0][2]


def test_day_crossing_moves_atomically(client):
    upload(client, [(1, 100, 1.0), (1, 150, 2.0)])
    # 100 → 1日23:00，150 → 2日00:00（恰跨日）
    set_anchors(client, 1, 100, "2026-01-01T23:00:00Z", 200, "2026-01-02T01:00:00Z")
    d1 = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    d2 = client.get("/api/readings", params={"date": "2026-01-02"}).json()
    assert d1["count"] == 1 and d2["count"] == 1
    # 更正锚点，整体后移一天：旧日移除与新日加入一起生效
    set_anchors(client, 1, 100, "2026-01-02T23:00:00Z", 200, "2026-01-03T01:00:00Z",
                version=1)
    d1 = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    d2 = client.get("/api/readings", params={"date": "2026-01-02"}).json()
    d3 = client.get("/api/readings", params={"date": "2026-01-03"}).json()
    assert d1["count"] == 0 and d2["count"] == 1 and d3["count"] == 1
    assert d2["readings"][0]["anchor_version"] == 2
    assert d2["calc_version"] == d3["calc_version"]  # 同一次换版


# ---------------------------------------------------------------- 并发 / 恢复

def test_interleaved_upload_during_recalc(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, worker="1", delay="0.6") as c:
        upload(c, [(1, 100, 1.0)])
        set_anchors(c, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
        wait_idle(c)
        v1 = c.get("/api/status").json()["calc"]["version"]

        upload(c, [(1, 150, 2.0)])  # 触发一次带延迟的重算
        t0 = time.time()
        while time.time() - t0 < 5:  # 等它进入重算中
            if c.get("/api/status").json()["calc"]["status"] == "running":
                break
            time.sleep(0.02)
        r = upload(c, [(1, 160, 3.0)])  # 重算期间上传新点，须被接纳
        assert r.status_code == 200

        st = wait_idle(c)
        assert st["version"] > v1  # 版本单调递增，不回退
        body = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert sorted(row["local_seconds"] for row in body["readings"]) == [100, 150, 160]


def test_old_version_visible_during_recalc(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, worker="1", delay="0.6") as c:
        upload(c, [(1, 100, 1.0)])
        set_anchors(c, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
        wait_idle(c)
        # 更正锚点使读数跨到次日；重算带延迟，期间旧版仍完整可见
        set_anchors(c, 1, 100, "2026-01-02T00:00:00Z", 200, "2026-01-02T00:10:00Z",
                    version=1)
        d1 = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        d2 = c.get("/api/readings", params={"date": "2026-01-02"}).json()
        assert d1["count"] == 1 and d2["count"] == 0
        wait_idle(c)
        d1 = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        d2 = c.get("/api/readings", params={"date": "2026-01-02"}).json()
        assert d1["count"] == 0 and d2["count"] == 1
        assert d2["readings"][0]["anchor_version"] == 2


def test_restart_recovers_pending_recalc(tmp_path, monkeypatch):
    from app import calibration
    from app.main import create_app

    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("WORKER_ENABLED", "0")
    # 模拟“崩溃前没来得及重算”：屏蔽同步重算，只落库
    orig = calibration.RecalcWorker.run_until_quiet
    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", lambda self: None)
    with TestClient(create_app()) as c1:
        upload(c1, [(1, 100, 1.0)])
        set_anchors(c1, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
        assert c1.get("/api/status").json()["calc"]["dirty"] is True
    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", orig)
    # 重启后：后台线程自动补算，已接纳且在锚点范围内的点最终进入当前版
    monkeypatch.setenv("WORKER_ENABLED", "1")
    with TestClient(create_app()) as c2:
        wait_idle(c2)
        body = c2.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert body["count"] == 1
        assert body["readings"][0]["local_seconds"] == 100


def test_failed_recalc_keeps_old_version(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as c:  # 同步重算模式
        upload(c, [(1, 100, 1.0)])
        set_anchors(c, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
        before = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert before["count"] == 1
        v_before = c.get("/api/status").json()["calc"]["version"]

        from app import calibration

        def boom(readings, anchors):
            raise RuntimeError("模拟计算失败")

        monkeypatch.setattr(calibration, "compute_calibration", boom)
        r = upload(c, [(1, 150, 2.0)])  # 新点仍被接纳，但重算失败
        assert r.status_code == 200
        calc = c.get("/api/status").json()["calc"]
        assert calc["status"] == "failed"
        assert calc["version"] == v_before          # 版本没有推进
        after = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert after["count"] == 1                   # 旧版完整，无半版
        assert [row["local_seconds"] for row in after["readings"]] == [100]

        monkeypatch.undo()  # 恢复 compute_calibration
        c.post("/api/recalc")
        calc = c.get("/api/status").json()["calc"]
        assert calc["status"] == "idle" and not calc["dirty"]
        body = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert sorted(row["local_seconds"] for row in body["readings"]) == [100, 150]


# ---------------------------------------------------------------- 页面

def test_html_pages_render(client):
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    for path in ["/", "/upload", "/anchors", "/query?date=2026-01-01"]:
        r = client.get(path)
        assert r.status_code == 200 and "text/html" in r.headers["content-type"], path
    html = client.get("/query?date=2026-01-01").text
    assert "批次号" in html and "原行号" in html and "锚点版本" in html




# ---------------------------------------------------------------- 逐版变更（下游同步）

def get_changes(client, since=0):
    r = client.get("/api/changes", params={"since": since})
    assert r.status_code == 200, r.text
    return r.json()


def apply_changes(state: dict, versions: list[dict]) -> None:
    """模拟下游：按版本顺序应用撤销/新增，维护 {reading_id: record}。"""
    for v in versions:
        for c in v["changes"]:
            key = c["reading_id"]
            if c["action"] == "retract":
                assert state[key]["utc_time"] == c["utc_time"]      # 撤回的正是旧值
                assert state[key]["utc_date"] == c["utc_date"]
                state.pop(key)
            else:
                state[key] = c


def sync(client, since=0) -> tuple[dict, int]:
    body = get_changes(client, since)
    return body, body["latest_version"]


def test_first_version_publishes_additions(client):
    upload(client, [(1, 100, 1.0), (1, 150, 2.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    body = get_changes(client, 0)
    assert body["since_version"] == 0
    assert body["latest_version"] == body["versions"][-1]["version"]
    assert body["covers"] == "all_changes" and body["dirty"] is False
    last = body["versions"][-1]
    assert last["additions"] == 2 and last["retractions"] == 0
    adds = last["changes"]
    assert {c["action"] for c in adds} == {"add"}
    for c in adds:
        assert c["reason"] == "new"
        assert c["batch_id"] == 1 and c["row_no"] in (1, 2)
        assert c["anchor_version"] == 1
        assert c["utc_date"] == "2026-01-01"
        assert set(c) == {"action", "reason", "reading_id", "batch_id", "row_no",
                          "segment", "local_seconds", "temperature",
                          "utc_time", "utc_date", "anchor_version"}
    # 每个已发布版本都出现在结果里
    assert [v["version"] for v in body["versions"]] == list(
        range(1, body["latest_version"] + 1))
    # 稳定读数 ID 与按日查询一致
    q = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert {r["reading_id"] for r in q["readings"]} == {c["reading_id"] for c in adds}


def test_anchor_correction_cross_day_retracts_old_day(client):
    upload(client, [(1, 100, 1.0), (1, 150, 2.0)])
    set_anchors(client, 1, 100, "2026-01-01T23:00:00Z", 200, "2026-01-02T01:00:00Z")
    body, v_base = sync(client)
    state: dict = {}
    apply_changes(state, body["versions"])
    assert len(state) == 2
    assert {r["utc_date"] for r in state.values()} == {"2026-01-01", "2026-01-02"}

    # 整体后移一天：旧日点必须被撤回，新日点新增
    set_anchors(client, 1, 100, "2026-01-02T23:00:00Z", 200, "2026-01-03T01:00:00Z",
                version=1)
    body2, v_new = sync(client, v_base)
    assert v_new == v_base + 1
    v2 = body2["versions"][0]
    assert v2["retractions"] == 2 and v2["additions"] == 2
    retracts = [c for c in v2["changes"] if c["action"] == "retract"]
    adds = [c for c in v2["changes"] if c["action"] == "add"]
    assert {c["utc_date"] for c in retracts} == {"2026-01-01", "2026-01-02"}
    assert {c["utc_date"] for c in adds} == {"2026-01-02", "2026-01-03"}
    assert {c["anchor_version"] for c in retracts} == {1}
    assert {c["anchor_version"] for c in adds} == {2}
    assert {c["reading_id"] for c in retracts} == {c["reading_id"] for c in adds}
    assert {c["reason"] for c in v2["changes"]} == {"date_changed+utc_changed+anchor_version_changed"}

    apply_changes(state, body2["versions"])
    # 下游状态与平台按日查询逐日完全一致（旧日不残留）
    for day in ("2026-01-01", "2026-01-02", "2026-01-03"):
        got = client.get("/api/readings", params={"date": day}).json()["readings"]
        expected = {r["reading_id"]: (r["utc_time"], r["anchor_version"]) for r in got}
        actual = {rid: (r["utc_time"], r["anchor_version"])
                  for rid, r in state.items() if r["utc_date"] == day}
        assert actual == expected, day
    assert not {r["utc_date"]: 1 for r in state.values() if r["utc_date"] == "2026-01-01"}


def test_leaving_anchor_range_retracts(client):
    upload(client, [(1, 100, 1.0), (1, 150, 2.0), (1, 200, 3.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    _, v_base = sync(client)
    # 收窄范围，150/200 退出锚点范围：必须撤回，旧日桶不能残留
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 120, "2026-01-01T00:02:00Z",
                version=1)
    v2 = get_changes(client, v_base)["versions"][0]
    changes = v2["changes"]
    retracts = [c for c in changes if c["action"] == "retract"]
    adds = [c for c in changes if c["action"] == "add"]
    # 150/200 退出范围被撤回；100 仍在范围内但因锚点版本变化需重新发布
    assert {c["local_seconds"] for c in retracts} == {100, 150, 200}
    assert {c["local_seconds"]: c["reason"] for c in retracts}[150] == "out_of_anchor_range"
    assert {c["local_seconds"]: c["reason"] for c in retracts}[200] == "out_of_anchor_range"
    assert [(c["local_seconds"], c["utc_time"]) for c in adds] == [
        (100, "2026-01-01T00:00:00.000Z")]
    assert v2["additions"] == 1 and v2["retractions"] == 3
    # 范围外的点绝不会出现在本版新增里
    assert all(c["local_seconds"] <= 120 for c in adds)


def test_utc_time_change_republishes_same_day(client):
    upload(client, [(1, 150, 2.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    _, v_base = sync(client)
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:20:00Z",
                version=1)
    v2 = get_changes(client, v_base)["versions"][0]
    assert [(c["action"], c["reason"]) for c in v2["changes"]] == [
        ("retract", "utc_changed+anchor_version_changed"),
        ("add", "utc_changed+anchor_version_changed"),
    ]
    assert v2["changes"][0]["utc_time"] == "2026-01-01T00:05:00.000Z"
    assert v2["changes"][1]["utc_time"] == "2026-01-01T00:10:00.000Z"
    assert v2["changes"][1]["anchor_version"] == 2


def test_same_value_correction_updates_anchor_version_only_via_republish(client):
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    _, v_base = sync(client)
    # 等价锚点更正：UTC 不变，但锚点版本变化 → 下游仍须更新，故撤销 + 新增
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z",
                version=1)
    v2 = get_changes(client, v_base)["versions"][0]
    assert [(c["action"], c["reason"]) for c in v2["changes"]] == [
        ("retract", "anchor_version_changed"),
        ("add", "anchor_version_changed"),
    ]
    assert v2["changes"][0]["utc_time"] == v2["changes"][1]["utc_time"]


def test_empty_version_is_published_and_replayable(client):
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    _, v_base = sync(client)
    # 上传一批落在锚点范围外的点：重算产生一个零变更版本
    upload(client, [(1, 900, 9.0)])
    body = get_changes(client, v_base)
    v_new = body["versions"][0]
    assert v_new["version"] == v_base + 1
    assert v_new["changes"] == []
    assert v_new["additions"] == 0 and v_new["retractions"] == 0
    assert body["latest_version"] == v_base + 1
    # 同一 since 重复领取结果一致（幂等）；零变更版本也能接续
    assert get_changes(client, v_base) == body
    tail = get_changes(client, body["latest_version"])
    assert tail["versions"] == [] and tail["latest_version"] == body["latest_version"]
    # 再次上传仍产生新版本，游标继续前进
    upload(client, [(1, 901, 9.1)])
    assert get_changes(client, body["latest_version"])["latest_version"] == v_base + 2


def test_bad_since_version(client):
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    latest = get_changes(client)["latest_version"]
    r = client.get("/api/changes", params={"since": latest + 5})
    assert r.status_code == 409 and r.json()["code"] == "UNKNOWN_VERSION"
    assert r.json()["current_version"] == latest
    r = client.get("/api/changes", params={"since": -1})
    assert r.status_code == 400 and r.json()["code"] == "BAD_VERSION"
    r = client.get("/api/changes", params={"since": "abc"})
    assert r.status_code == 400 and r.json()["code"] == "BAD_VALUE"


def test_dirty_changes_only_cover_published(client, monkeypatch):
    """未完成重算时明确标注只覆盖已发布版本，完成后重复领取可接续。"""
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    v1 = get_changes(client)

    from app import calibration
    orig = calibration.RecalcWorker.run_until_quiet
    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", lambda self: None)
    upload(client, [(1, 150, 2.0)])          # 变更落库但未重算
    body = get_changes(client, v1["latest_version"])
    assert body["dirty"] is True and body["covers"] == "published_versions_only"
    assert body["versions"] == [] and body["latest_version"] == v1["latest_version"]

    monkeypatch.setattr(calibration.RecalcWorker, "run_until_quiet", orig)
    client.post("/api/recalc")
    body = get_changes(client, v1["latest_version"])
    assert body["dirty"] is False and body["covers"] == "all_changes"
    assert [c["local_seconds"] for c in body["versions"][0]["changes"]] == [150]
    assert body["versions"][0]["version"] == v1["latest_version"] + 1


def test_failed_recalc_publishes_no_partial_changes(client, monkeypatch):
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    before = get_changes(client)

    from app import calibration

    def boom(readings, anchors):
        raise RuntimeError("模拟计算失败")

    monkeypatch.setattr(calibration, "compute_calibration", boom)
    upload(client, [(1, 150, 2.0)])
    after = get_changes(client)
    assert after["versions"] == before["versions"]          # 无半组变更
    assert after["latest_version"] == before["latest_version"]
    assert after["dirty"] is True
    assert after["covers"] == "published_versions_only"
    # 已发布版本仍可重复领取，且答案不变
    assert get_changes(client)["versions"] == before["versions"]


def test_changes_html_page(client):
    upload(client, [(1, 100, 1.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    r = client.get("/changes?since=0")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    for word in ("逐版撤销与新增记录", "读数 ID", "批次号", "原行号", "锚点版本",
                 "UTC 时刻", "已同步版本"):
        assert word in r.text
    assert "校时版本 v" in r.text
    # 导航入口
    assert 'href="/changes"' in client.get("/").text
    # 非法 since 给页面错误而非 500
    assert "since 须为" in client.get("/changes?since=abc").text
    assert client.get("/changes?since=999").status_code == 200
    assert "超过当前已发布版本" in client.get("/changes?since=999").text


def test_upgrade_existing_db_adopts_published_version(tmp_path, monkeypatch):
    """旧库已有校时结果但无变更账本：启动时把该版登记为可领取版本。"""
    import sqlite3

    from app.main import create_app

    db_path = str(tmp_path / "old.db")
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.setenv("WORKER_ENABLED", "0")
    with TestClient(create_app()) as c:
        upload(c, [(1, 100, 1.0), (1, 150, 2.0)])
        set_anchors(c, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
        old_version = c.get("/api/status").json()["calc"]["version"]

    # 模拟旧版本软件留下的数据库：抹掉变更账本
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM calc_changes")
    conn.execute("DELETE FROM calc_versions")
    conn.commit()
    conn.close()

    with TestClient(create_app()) as c:
        body = get_changes(c)
        assert body["latest_version"] == old_version
        v = body["versions"][0]
        assert v["version"] == old_version
        assert v["additions"] == 2 and v["retractions"] == 0
        assert {c["reason"] for c in v["changes"]} == {"adopted"}
        q = c.get("/api/readings", params={"date": "2026-01-01"}).json()
        assert {r["reading_id"] for r in q["readings"]} == {
            c2["reading_id"] for c2 in v["changes"]}
        # 再次启动不重复登记
    with TestClient(create_app()) as c:
        body = get_changes(c)
        assert [v["version"] for v in body["versions"]] == [old_version]


def test_commit_failure_rolls_back_whole_version(client, monkeypatch):
    """换版提交中途失败：结果与变更一起回滚，旧版本完整、无半组变更。"""
    upload(client, [(1, 100, 1.0), (1, 150, 2.0)])
    set_anchors(client, 1, 100, "2026-01-01T00:00:00Z", 200, "2026-01-01T00:10:00Z")
    before = get_changes(client)
    v_before = before["latest_version"]

    from app import calibration

    orig = calibration._change_row
    calls = {"n": 0}

    def flaky(action, reason, version, rec):
        calls["n"] += 1
        if rec["local_seconds"] == 170:     # 第一条变更已写入后失败
            raise RuntimeError("模拟提交失败")
        return orig(action, reason, version, rec)

    monkeypatch.setattr(calibration, "_change_row", flaky)
    r = upload(client, [(1, 160, 3.0), (1, 170, 4.0)])  # 两个新增点 → 两条 add 变更
    assert r.status_code == 200
    calc = client.get("/api/status").json()["calc"]
    assert calc["status"] == "failed" and calc["version"] == v_before
    assert calls["n"] >= 2                  # 失败确实发生在写入变更的过程中
    after = get_changes(client)
    assert after["versions"] == before["versions"]
    assert after["latest_version"] == v_before
    assert after["dirty"] is True
    q = client.get("/api/readings", params={"date": "2026-01-01"}).json()
    assert q["count"] == 2 and q["calc_version"] == v_before

    monkeypatch.setattr(calibration, "_change_row", orig)
    client.post("/api/recalc")
    final = get_changes(client, v_before)
    assert final["dirty"] is False
    assert len(final["versions"]) == 1
    assert [c["local_seconds"] for c in final["versions"][0]["changes"]] == [160, 170]

from __future__ import annotations

import sqlite3

import pytest


def origin(client, code="VEG-001", quantity=500.0, when="2026-09-20T06:00:00+00:00"):
    response = client.post("/api/trace/origins", json={
        "lot_code": code,
        "quantity_kg": quantity,
        "event_time": when,
        "supplier": "安心农场",
        "origin": "山东寿光三号大棚",
        "product_name": "菠菜",
        "harvest_date": "2026-09-20",
        "certificate_no": "CERT-SG-001",
        "documents": ["产地证明.pdf", "农残快检单.pdf"],
    })
    assert response.status_code == 201, response.text
    return response.json()


def test_origin_split_handover_merge_and_lineage(client):
    first = origin(client)
    assert first["event_type"] == "genesis"
    assert first["prev_hash"] == "0" * 64 and len(first["event_hash"]) == 64

    # 500kg 拆成 300 + 200
    split = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-A", "quantity_kg": 300}, {"lot_code": "VEG-001-B", "quantity_kg": 200}],
        "note": "按食堂订单分装",
    })
    assert split.status_code == 201, split.text

    # 节点交接：300kg 从分拣中心运到学校食堂
    handover = client.post("/api/trace/handovers", json={
        "lot_code": "VEG-001-A", "quantity_kg": 300, "event_time": "2026-09-21T08:00:00+00:00",
        "from_node": "分拣中心冷库", "to_node": "第一学校食堂收货口",
        "handler": "司机张师傅", "shipment_code": "TR-7788", "documents": ["运单.pdf"],
    })
    assert handover.status_code == 201, handover.text
    assert handover.json()["payload"]["to_node"] == "第一学校食堂收货口"

    # 与另一产地批次合并成净菜拼盘
    origin(client, "VEG-200", 100.0, when="2026-09-19T06:00:00+00:00")
    merge = client.post("/api/trace/merges", json={
        "lot_code": "MIX-300", "quantity_kg": 400, "event_time": "2026-09-22T03:00:00+00:00",
        "inputs": [{"lot_code": "VEG-001-A", "quantity_kg": 300}, {"lot_code": "VEG-200", "quantity_kg": 100}],
        "note": "净菜组合包装",
    })
    assert merge.status_code == 201, merge.text

    # 沿上游回溯：拼盘 -> 两个来源批次 -> 产地
    up = client.get("/api/trace/lots/MIX-300/lineage?direction=upstream").json()
    types = [event["event_type"] for event in up["events"]]
    assert types == ["genesis", "genesis", "split", "handover", "merge"]
    assert set(up["root_lots"]) == {"VEG-001", "VEG-200"}

    # 沿下游展开：原始 500kg 最终供应到了哪些批次/餐桌
    down = client.get("/api/trace/lots/VEG-001/lineage?direction=downstream").json()
    touched = {lot["lot_code"] for lot in down["lots"]}
    assert {"VEG-001", "VEG-001-A", "VEG-001-B", "VEG-200", "MIX-300"} <= touched
    assert any(event["event_type"] == "handover" for event in down["events"])

    history = client.get("/api/trace/lots/VEG-001-A/history").json()
    assert history["current_balance_kg"] == 0.0  # 300kg 已全部并入 MIX-300
    assert [event["event_type"] for event in history["events"]] == ["split", "handover", "merge"]

    verify = client.get("/api/trace/verify").json()
    assert verify["ok"] is True and verify["events"] == 5


def test_split_quantity_mismatch_is_rejected(client):
    origin(client)
    response = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-A", "quantity_kg": 300}, {"lot_code": "VEG-001-B", "quantity_kg": 150}],
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "quantity_mismatch"


def test_merge_quantity_mismatch_is_rejected(client):
    origin(client)
    origin(client, "VEG-200", 100.0, when="2026-09-19T06:00:00+00:00")
    response = client.post("/api/trace/merges", json={
        "lot_code": "MIX-900", "quantity_kg": 400, "event_time": "2026-09-22T03:00:00+00:00",
        "inputs": [{"lot_code": "VEG-001", "quantity_kg": 500}, {"lot_code": "VEG-200", "quantity_kg": 100}],
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "quantity_mismatch"


def test_overdrawn_split_and_handover_are_rejected(client):
    origin(client, quantity=100.0)
    response = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 120, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-A", "quantity_kg": 100}, {"lot_code": "VEG-001-B", "quantity_kg": 20}],
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "insufficient_quantity"

    handover = client.post("/api/trace/handovers", json={
        "lot_code": "VEG-001", "quantity_kg": 150, "event_time": "2026-09-21T08:00:00+00:00",
        "from_node": "A", "to_node": "B",
    })
    assert handover.status_code == 409
    assert handover.json()["error"]["context"]["reason"] == "insufficient_quantity"


def test_unknown_lot_and_duplicate_origin_are_rejected(client):
    response = client.post("/api/trace/handovers", json={
        "lot_code": "GHOST", "quantity_kg": 10, "event_time": "2026-09-21T08:00:00+00:00",
        "from_node": "A", "to_node": "B",
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "lot_not_sourced"

    origin(client)
    again = client.post("/api/trace/origins", json={
        "lot_code": "VEG-001", "quantity_kg": 10, "event_time": "2026-09-21T08:00:00+00:00",
        "supplier": "x", "origin": "y",
    })
    assert again.status_code == 409
    assert again.json()["error"]["context"]["reason"] == "lot_already_sourced"


def test_reverse_order_events_are_rejected(client):
    origin(client)
    response = client.post("/api/trace/handovers", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-19T08:00:00+00:00",
        "from_node": "分拣中心", "to_node": "食堂",
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "reverse_order"


def test_revoke_marks_event_without_deleting_history(client):
    first = origin(client)
    split_resp = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-A", "quantity_kg": 300}, {"lot_code": "VEG-001-B", "quantity_kg": 200}],
    })
    split_event = split_resp.json()

    # 撤销拆分：历史保留，但被标示 revoked
    revoke = client.post(f"/api/trace/events/{split_event['id']}/revoke", json={
        "reason": "分装记录录入错误，重新拆分", "actor": "监管员",
        "event_time": "2026-09-21T10:00:00+00:00",
    })
    assert revoke.status_code == 201, revoke.text
    assert revoke.json()["event_type"] == "revoke"

    marked = client.get(f"/api/trace/events/{split_event['id']}").json()
    assert marked["revoked"] is True
    assert marked["revoked_reason"] == "分装记录录入错误，重新拆分"

    history = client.get("/api/trace/lots/VEG-001/history").json()
    ids = {event["id"] for event in history["events"]}
    assert split_event["id"] in ids  # 历史未删除
    assert history["current_balance_kg"] == 500.0  # 撤销后数量回到原状

    lineage = client.get("/api/trace/lots/VEG-001/lineage").json()
    revoked_in_lineage = [event for event in lineage["events"] if event["id"] == split_event["id"]][0]
    assert revoked_in_lineage["revoked"] is True
    assert any(event["event_type"] == "revoke" for event in lineage["events"])

    # 撤销后可以用不早于链上时间的事件重新拆分
    redo = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-21T11:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-C", "quantity_kg": 250}, {"lot_code": "VEG-001-D", "quantity_kg": 250}],
    })
    assert redo.status_code == 201, redo.text
    assert client.get("/api/trace/verify").json()["ok"] is True


def test_revoke_with_downstream_dependency_is_rejected(client):
    origin(client)
    split_event = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-A", "quantity_kg": 300}, {"lot_code": "VEG-001-B", "quantity_kg": 200}],
    }).json()
    client.post("/api/trace/handovers", json={
        "lot_code": "VEG-001-A", "quantity_kg": 300, "event_time": "2026-09-21T08:00:00+00:00",
        "from_node": "分拣中心", "to_node": "食堂",
    })
    response = client.post(f"/api/trace/events/{split_event['id']}/revoke", json={
        "reason": "误录", "event_time": "2026-09-21T10:00:00+00:00",
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "lot_not_sourced"


def test_revoke_rules(client):
    first = origin(client)
    # 逆序撤销：撤销时间早于原事件
    response = client.post(f"/api/trace/events/{first['id']}/revoke", json={
        "reason": "误录", "event_time": "2026-09-19T00:00:00+00:00",
    })
    assert response.status_code == 409
    assert response.json()["error"]["context"]["reason"] == "reverse_order"

    revoke = client.post(f"/api/trace/events/{first['id']}/revoke", json={
        "reason": "误录", "event_time": "2026-09-21T10:00:00+00:00",
    })
    assert revoke.status_code == 201
    # 重复撤销被拒绝
    again = client.post(f"/api/trace/events/{first['id']}/revoke", json={"reason": "再撤一次"})
    assert again.status_code == 409
    assert again.json()["error"]["context"]["reason"] == "already_revoked"
    # 撤销事件不可再撤销
    nested = client.post(f"/api/trace/events/{revoke.json()['id']}/revoke", json={"reason": "x"})
    assert nested.status_code == 409
    assert nested.json()["error"]["context"]["reason"] == "revoke_not_revocable"


def test_chain_is_physically_immutable(client):
    first = origin(client)
    from app.database import get_connection
    connection = get_connection()
    with pytest.raises(sqlite3.IntegrityError, match="trace_chain_immutable"):
        connection.execute("UPDATE trace_events SET actor='hacker' WHERE id=?", (first["id"],))
    with pytest.raises(sqlite3.IntegrityError, match="trace_chain_immutable"):
        connection.execute("DELETE FROM trace_events WHERE id=?", (first["id"],))
    with pytest.raises(sqlite3.IntegrityError, match="trace_chain_immutable"):
        connection.execute("UPDATE trace_lot_events SET quantity_kg=999 WHERE event_id=?", (first["id"],))
    # 数据未被改动
    assert client.get(f"/api/trace/events/{first['id']}").json()["actor"] == "安心农场"


def test_partial_split_preserves_remaining_balance(client):
    origin(client, quantity=500.0)
    response = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 300, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001-A", "quantity_kg": 200}, {"lot_code": "VEG-001-B", "quantity_kg": 100}],
    })
    assert response.status_code == 201, response.text
    history = client.get("/api/trace/lots/VEG-001/history").json()
    assert history["current_balance_kg"] == 200.0  # 原批次保留未分装的 200kg


def test_output_reusing_input_code_is_rejected(client):
    origin(client)
    response = client.post("/api/trace/splits", json={
        "lot_code": "VEG-001", "quantity_kg": 500, "event_time": "2026-09-21T05:00:00+00:00",
        "outputs": [{"lot_code": "VEG-001", "quantity_kg": 300}, {"lot_code": "VEG-001-B", "quantity_kg": 200}],
    })
    assert response.status_code == 422

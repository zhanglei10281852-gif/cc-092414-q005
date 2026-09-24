from __future__ import annotations

import sqlite3

import pytest


def make_lot(client, code="LOT-A", quantity=500.0, product="菠菜"):
    response = client.post(
        "/api/food/lots",
        json={
            "lot_code": code,
            "product_name": product,
            "category": "叶菜",
            "supplier": "安心农场",
            "origin": "山东寿光",
            "harvest_date": "2026-09-20",
            "quantity_kg": quantity,
            "trace_code": code + "-TRACE",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def post_event(client, **payload):
    return client.post("/api/food/trace/events", json=payload)


def register(client, code, quantity, at="2026-09-20T08:00:00+00:00", node="寿光产地"):
    return post_event(
        client,
        event_code=f"EV-REG-{code}",
        event_type="register",
        occurred_at=at,
        actor="监管员",
        to_node=node,
        outputs=[{"lot_code": code, "quantity_kg": quantity}],
        credentials=[
            {"credential_type": "产地证明", "credential_no": f"ORG-{code}", "issuer": "寿光市农业农村局", "issued_at": "2026-09-19"}
        ],
    )


def build_chain(client):
    """登记→拆分→交接→合并→供应 的完整链路，返回关键批次。"""
    lot_a = make_lot(client, "LOT-A", 500)
    lot_b = make_lot(client, "LOT-B", 400)
    assert register(client, "LOT-A", 500, at="2026-09-20T08:00:00+00:00", node="寿光产地").status_code == 201
    assert register(client, "LOT-B", 400, at="2026-09-20T09:00:00+00:00", node="青州产地").status_code == 201

    split = post_event(
        client,
        event_code="EV-SPLIT-1",
        event_type="split",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="分拣员",
        from_node="寿光产地",
        to_node="分拣中心",
        inputs=[{"lot_code": "LOT-A", "quantity_kg": 500}],
        outputs=[{"lot_code": "LOT-A1", "quantity_kg": 200}, {"lot_code": "LOT-A2", "quantity_kg": 300}],
    )
    assert split.status_code == 201, split.text

    handover = post_event(
        client,
        event_code="EV-TR-1",
        event_type="transfer",
        occurred_at="2026-09-22T08:00:00+00:00",
        actor="冷链司机",
        from_node="分拣中心",
        to_node="城北批发市场",
        inputs=[{"lot_code": "LOT-A1", "quantity_kg": 200}],
        outputs=[{"lot_code": "LOT-A1", "quantity_kg": 200}],
    )
    assert handover.status_code == 201, handover.text

    partial = post_event(
        client,
        event_code="EV-TR-2",
        event_type="transfer",
        occurred_at="2026-09-22T09:00:00+00:00",
        actor="冷链司机",
        from_node="分拣中心",
        to_node="城西批发市场",
        inputs=[{"lot_code": "LOT-A2", "quantity_kg": 120}],
        outputs=[{"lot_code": "LOT-A2B", "quantity_kg": 120}],
    )
    assert partial.status_code == 201, partial.text

    merge = post_event(
        client,
        event_code="EV-MERGE-1",
        event_type="merge",
        occurred_at="2026-09-23T08:00:00+00:00",
        actor="中央厨房",
        to_node="中央厨房",
        inputs=[{"lot_code": "LOT-A2", "quantity_kg": 180}, {"lot_code": "LOT-B", "quantity_kg": 400}],
        outputs=[{"lot_code": "LOT-M", "quantity_kg": 580}],
    )
    assert merge.status_code == 201, merge.text

    serve_m = post_event(
        client,
        event_code="EV-SERVE-1",
        event_type="consume",
        occurred_at="2026-09-24T08:00:00+00:00",
        actor="食堂管理员",
        to_node="第一中学食堂3号餐桌",
        inputs=[{"lot_code": "LOT-M", "quantity_kg": 100}],
    )
    assert serve_m.status_code == 201, serve_m.text

    serve_a1 = post_event(
        client,
        event_code="EV-SERVE-2",
        event_type="consume",
        occurred_at="2026-09-24T09:00:00+00:00",
        actor="食堂管理员",
        to_node="第一中学食堂5号餐桌",
        inputs=[{"lot_code": "LOT-A1", "quantity_kg": 50}],
    )
    assert serve_a1.status_code == 201, serve_a1.text
    return {"LOT-A": lot_a, "LOT-B": lot_b}


def lot_id(client, lots, code):
    return lots[code]["id"]


def lot_id_by_code(client, code):
    response = client.get("/api/food/trace/events", params={"lot_code": code})
    assert response.status_code == 200
    for event in response.json()["events"]:
        for item in event["outputs"] + event["inputs"]:
            if item["lot_code"] == code:
                return item["lot_id"]
    raise AssertionError(f"批次未入链: {code}")


def test_full_chain_upstream_and_downstream(client):
    lots = build_chain(client)
    merged_id = lot_id_by_code(client, "LOT-M")

    upstream = client.get(f"/api/food/lots/{merged_id}/trace/upstream")
    assert upstream.status_code == 200
    data = upstream.json()
    event_types = [event["event_type"] for event in data["events"]]
    assert event_types.count("register") == 2
    assert "split" in event_types and "merge" in event_types
    assert {lot["lot_code"] for lot in data["origin_lots"]} == {"LOT-A", "LOT-B"}
    register_event = next(event for event in data["events"] if event["event_type"] == "register" and event["outputs"][0]["lot_code"] == "LOT-A")
    assert register_event["credentials"][0]["credential_type"] == "产地证明"
    assert data["lot"]["balance_kg"] == 480.0

    downstream = client.get(f"/api/food/lots/{lots['LOT-A']['id']}/trace/downstream")
    assert downstream.status_code == 200
    down = downstream.json()
    down_types = sorted(event["event_type"] for event in down["events"])
    assert down_types == ["consume", "consume", "merge", "split", "transfer", "transfer"]
    assert {event["event_code"] for event in down["events"]} == {"EV-SPLIT-1", "EV-TR-1", "EV-TR-2", "EV-MERGE-1", "EV-SERVE-1", "EV-SERVE-2"}
    tables = {(item["to_node"], item["quantity_kg"]) for item in down["served_tables"]}
    assert tables == {("第一中学食堂3号餐桌", 100.0), ("第一中学食堂5号餐桌", 50.0)}

    balances = {
        "LOT-A": 0.0,
        "LOT-A1": 150.0,
        "LOT-A2": 0.0,
        "LOT-A2B": 120.0,
        "LOT-B": 0.0,
        "LOT-M": 480.0,
    }
    for code, expected in balances.items():
        balance = client.get(f"/api/food/lots/{lot_id_by_code(client, code)}/balance")
        assert balance.status_code == 200
        assert balance.json()["balance_kg"] == expected, code

    verify = client.get("/api/food/trace/verify")
    assert verify.status_code == 200
    assert verify.json() == {"valid": True, "checked": 8, "broken_event_id": None, "broken_event_code": None}


def test_derived_lots_inherit_attributes(client):
    build_chain(client)
    merged_id = lot_id_by_code(client, "LOT-M")
    detail = client.get(f"/api/food/lots/{merged_id}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["product_name"] == "菠菜"
    assert body["supplier"] == "安心农场"
    assert body["quantity_kg"] == 580.0


def test_quantity_imbalance_rejected(client):
    make_lot(client, "LOT-Q", 500)
    assert register(client, "LOT-Q", 500).status_code == 201

    split = post_event(
        client,
        event_code="EV-BAD-SPLIT",
        event_type="split",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="分拣员",
        to_node="分拣中心",
        inputs=[{"lot_code": "LOT-Q", "quantity_kg": 500}],
        outputs=[{"lot_code": "LOT-Q1", "quantity_kg": 200}, {"lot_code": "LOT-Q2", "quantity_kg": 200}],
    )
    assert split.status_code == 409 and "数量不平衡" in split.json()["detail"]

    overspend = post_event(
        client,
        event_code="EV-BAD-TR",
        event_type="transfer",
        occurred_at="2026-09-21T09:00:00+00:00",
        actor="司机",
        to_node="批发市场",
        inputs=[{"lot_code": "LOT-Q", "quantity_kg": 600}],
        outputs=[{"lot_code": "LOT-QX", "quantity_kg": 600}],
    )
    assert overspend.status_code == 409 and "可用余额" in overspend.json()["detail"]

    partial_same_lot = post_event(
        client,
        event_code="EV-BAD-TR2",
        event_type="transfer",
        occurred_at="2026-09-21T10:00:00+00:00",
        actor="司机",
        to_node="批发市场",
        inputs=[{"lot_code": "LOT-Q", "quantity_kg": 200}],
        outputs=[{"lot_code": "LOT-Q", "quantity_kg": 200}],
    )
    assert partial_same_lot.status_code == 409 and "部分交接" in partial_same_lot.json()["detail"]

    merge = post_event(
        client,
        event_code="EV-BAD-MERGE",
        event_type="merge",
        occurred_at="2026-09-21T11:00:00+00:00",
        actor="厨房",
        inputs=[{"lot_code": "LOT-Q", "quantity_kg": 100}],
        outputs=[{"lot_code": "LOT-QM", "quantity_kg": 100}],
    )
    assert merge.status_code == 409 and "至少需要两个投入批次" in merge.json()["detail"]


def test_out_of_order_rejected(client):
    make_lot(client, "LOT-T", 300)
    assert register(client, "LOT-T", 300, at="2026-09-21T08:00:00+00:00").status_code == 201

    backwards = post_event(
        client,
        event_code="EV-LATE-1",
        event_type="transfer",
        occurred_at="2026-09-21T07:00:00+00:00",
        actor="司机",
        to_node="批发市场",
        inputs=[{"lot_code": "LOT-T", "quantity_kg": 100}],
        outputs=[{"lot_code": "LOT-T1", "quantity_kg": 100}],
    )
    assert backwards.status_code == 409 and "逆序操作" in backwards.json()["detail"]

    forward = post_event(
        client,
        event_code="EV-OK-1",
        event_type="transfer",
        occurred_at="2026-09-21T09:00:00+00:00",
        actor="司机",
        to_node="批发市场",
        inputs=[{"lot_code": "LOT-T", "quantity_kg": 100}],
        outputs=[{"lot_code": "LOT-T1", "quantity_kg": 100}],
    )
    assert forward.status_code == 201

    early_revoke = post_event(
        client,
        event_code="EV-REV-EARLY",
        event_type="revoke",
        occurred_at="2026-09-21T08:30:00+00:00",
        actor="监管员",
        target_event_code="EV-OK-1",
        reason="时间倒挂验证",
    )
    assert early_revoke.status_code == 409 and "逆序操作" in early_revoke.json()["detail"]


def test_node_discontinuity_rejected(client):
    make_lot(client, "LOT-N", 100)
    assert register(client, "LOT-N", 100, node="寿光产地").status_code == 201
    jump = post_event(
        client,
        event_code="EV-JUMP-1",
        event_type="transfer",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="司机",
        from_node="不存在的节点",
        to_node="批发市场",
        inputs=[{"lot_code": "LOT-N", "quantity_kg": 100}],
        outputs=[{"lot_code": "LOT-N", "quantity_kg": 100}],
    )
    assert jump.status_code == 409 and "节点交接不连续" in jump.json()["detail"]


def test_register_rules(client):
    make_lot(client, "LOT-G", 500)

    no_credential = post_event(
        client,
        event_code="EV-REG-NOCRED",
        event_type="register",
        occurred_at="2026-09-20T08:00:00+00:00",
        actor="监管员",
        outputs=[{"lot_code": "LOT-G", "quantity_kg": 500}],
    )
    assert no_credential.status_code == 409 and "来源凭证" in no_credential.json()["detail"]

    mismatch = post_event(
        client,
        event_code="EV-REG-MIS",
        event_type="register",
        occurred_at="2026-09-20T08:00:00+00:00",
        actor="监管员",
        outputs=[{"lot_code": "LOT-G", "quantity_kg": 400}],
        credentials=[{"credential_type": "产地证明", "credential_no": "ORG-G", "issuer": "农业农村局"}],
    )
    assert mismatch.status_code == 409 and "数量不一致" in mismatch.json()["detail"]

    missing = post_event(
        client,
        event_code="EV-REG-MISSING",
        event_type="register",
        occurred_at="2026-09-20T08:00:00+00:00",
        actor="监管员",
        outputs=[{"lot_code": "LOT-NONE", "quantity_kg": 100}],
        credentials=[{"credential_type": "产地证明", "credential_no": "ORG-X", "issuer": "农业农村局"}],
    )
    assert missing.status_code == 404

    assert register(client, "LOT-G", 500).status_code == 201
    again = post_event(
        client,
        event_code="EV-REG-LOT-G-2",
        event_type="register",
        occurred_at="2026-09-20T09:00:00+00:00",
        actor="监管员",
        outputs=[{"lot_code": "LOT-G", "quantity_kg": 500}],
        credentials=[{"credential_type": "产地证明", "credential_no": "ORG-LOT-G-2", "issuer": "寿光市农业农村局"}],
    )
    assert again.status_code == 409 and "重复登记" in again.json()["detail"]


def test_merge_requires_same_product(client):
    make_lot(client, "LOT-V1", 200, product="菠菜")
    make_lot(client, "LOT-V2", 200, product="黄瓜")
    assert register(client, "LOT-V1", 200).status_code == 201
    assert register(client, "LOT-V2", 200, at="2026-09-20T09:00:00+00:00").status_code == 201
    merge = post_event(
        client,
        event_code="EV-MERGE-BAD",
        event_type="merge",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="厨房",
        inputs=[{"lot_code": "LOT-V1", "quantity_kg": 100}, {"lot_code": "LOT-V2", "quantity_kg": 100}],
        outputs=[{"lot_code": "LOT-VM", "quantity_kg": 200}],
    )
    assert merge.status_code == 409 and "品类不一致" in merge.json()["detail"]


def test_revoke_marks_history_and_restores_balance(client):
    lot = make_lot(client, "LOT-R", 300)
    assert register(client, "LOT-R", 300).status_code == 201
    transfer = post_event(
        client,
        event_code="EV-TR-R1",
        event_type="transfer",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="司机",
        from_node="寿光产地",
        to_node="批发市场",
        inputs=[{"lot_code": "LOT-R", "quantity_kg": 120}],
        outputs=[{"lot_code": "LOT-R1", "quantity_kg": 120}],
    )
    assert transfer.status_code == 201

    revoke = post_event(
        client,
        event_code="EV-REV-1",
        event_type="revoke",
        occurred_at="2026-09-22T08:00:00+00:00",
        actor="监管员",
        target_event_code="EV-TR-R1",
        reason="运输单录入错误",
    )
    assert revoke.status_code == 201, revoke.text

    source_balance = client.get(f"/api/food/lots/{lot['id']}/balance").json()
    assert source_balance["balance_kg"] == 300.0
    derived_balance = client.get(f"/api/food/lots/{lot_id_by_code(client, 'LOT-R1')}/balance").json()
    assert derived_balance["balance_kg"] == 0.0

    downstream = client.get(f"/api/food/lots/{lot['id']}/trace/downstream").json()
    revoked = [event for event in downstream["events"] if event["event_code"] == "EV-TR-R1"]
    assert len(revoked) == 1
    assert revoked[0]["revoked"] is True
    assert revoked[0]["revoked_by"]["event_code"] == "EV-REV-1"
    assert revoked[0]["revoked_by"]["reason"] == "运输单录入错误"
    assert {event["event_code"] for event in downstream["events"]} == {"EV-TR-R1"}

    again = post_event(
        client,
        event_code="EV-REV-2",
        event_type="revoke",
        occurred_at="2026-09-22T09:00:00+00:00",
        actor="监管员",
        target_event_code="EV-TR-R1",
        reason="重复撤销",
    )
    assert again.status_code == 409 and "已被撤销" in again.json()["detail"]

    verify = client.get("/api/food/trace/verify").json()
    assert verify["valid"] is True and verify["checked"] == 3


def test_revoke_blocked_when_downstream_flowed(client):
    make_lot(client, "LOT-C", 300)
    assert register(client, "LOT-C", 300).status_code == 201
    assert post_event(
        client,
        event_code="EV-TR-C1",
        event_type="transfer",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="司机",
        to_node="学校食堂",
        inputs=[{"lot_code": "LOT-C", "quantity_kg": 120}],
        outputs=[{"lot_code": "LOT-C1", "quantity_kg": 120}],
    ).status_code == 201
    assert post_event(
        client,
        event_code="EV-SERVE-C1",
        event_type="consume",
        occurred_at="2026-09-22T08:00:00+00:00",
        actor="食堂",
        to_node="第二小学食堂1号餐桌",
        inputs=[{"lot_code": "LOT-C1", "quantity_kg": 120}],
    ).status_code == 201

    revoke = post_event(
        client,
        event_code="EV-REV-C1",
        event_type="revoke",
        occurred_at="2026-09-23T08:00:00+00:00",
        actor="监管员",
        target_event_code="EV-TR-C1",
        reason="试图撤销已供应的交接",
    )
    assert revoke.status_code == 409 and "下游已流转" in revoke.json()["detail"]


def test_consume_blocked_for_held_lot(client):
    lot = make_lot(client, "LOT-H", 200)
    assert register(client, "LOT-H", 200).status_code == 201
    hold = client.post(f"/api/food/lots/{lot['id']}/risk", json={"decision": "hold", "reason": "农残复检中", "operator": "监管员"})
    assert hold.status_code == 200
    consume = post_event(
        client,
        event_code="EV-SERVE-H1",
        event_type="consume",
        occurred_at="2026-09-21T08:00:00+00:00",
        actor="食堂",
        to_node="第一食堂",
        inputs=[{"lot_code": "LOT-H", "quantity_kg": 50}],
    )
    assert consume.status_code == 409 and "禁止供应" in consume.json()["detail"]


def test_idempotent_replay(client):
    make_lot(client, "LOT-I", 100)
    payload = {
        "event_code": "EV-REG-I",
        "event_type": "register",
        "occurred_at": "2026-09-20T08:00:00+00:00",
        "actor": "监管员",
        "outputs": [{"lot_code": "LOT-I", "quantity_kg": 100}],
        "credentials": [{"credential_type": "检疫证明", "credential_no": "Q-I", "issuer": "检测站"}],
    }
    first = post_event(client, **payload)
    assert first.status_code == 201
    replay = post_event(client, **payload)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]

    conflict = post_event(client, **{**payload, "outputs": [{"lot_code": "LOT-I", "quantity_kg": 90}]})
    assert conflict.status_code == 409 and "内容不一致" in conflict.json()["detail"]


def test_events_are_immutable(client):
    make_lot(client, "LOT-D", 100)
    assert register(client, "LOT-D", 100).status_code == 201

    from app.database import get_connection

    connection = get_connection()
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE trace_events SET actor='篡改者'")
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("DELETE FROM trace_events")
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("DELETE FROM trace_event_outputs")


def test_verify_detects_tampered_chain(client):
    make_lot(client, "LOT-K", 100)
    assert register(client, "LOT-K", 100).status_code == 201
    assert client.get("/api/food/trace/verify").json()["valid"] is True

    from app.database import get_connection

    get_connection().execute(
        "INSERT INTO trace_events(event_code,event_type,occurred_at,actor,from_node,to_node,reason,request_hash,content_hash,prev_hash,event_hash,created_at) VALUES('EV-FAKE','consume','2026-09-22T00:00:00+00:00','伪造者','','','','x','x','bogus','bogus','2026-09-22T00:00:00+00:00')"
    )
    result = client.get("/api/food/trace/verify").json()
    assert result["valid"] is False
    assert result["broken_event_code"] == "EV-FAKE"
    assert result["checked"] == 1


def test_event_listing_and_detail(client):
    lots = build_chain(client)
    listed = client.get("/api/food/trace/events", params={"lot_code": "LOT-M"})
    assert listed.status_code == 200
    assert {event["event_code"] for event in listed.json()["events"]} == {"EV-MERGE-1", "EV-SERVE-1"}

    event_id = listed.json()["events"][0]["id"]
    detail = client.get(f"/api/food/trace/events/{event_id}")
    assert detail.status_code == 200
    assert detail.json()["id"] == event_id

    missing = client.get("/api/food/trace/events/99999")
    assert missing.status_code == 404

    by_type = client.get("/api/food/trace/events", params={"event_type": "consume"})
    assert {event["event_code"] for event in by_type.json()["events"]} == {"EV-SERVE-1", "EV-SERVE-2"}
    del lots

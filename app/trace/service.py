from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


SCHEMA = """
CREATE TABLE IF NOT EXISTS trace_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL CHECK(event_type IN ('genesis','split','merge','handover','revoke')),
    event_time TEXT NOT NULL,
    actor TEXT NOT NULL,
    inputs_json TEXT NOT NULL DEFAULT '[]',
    outputs_json TEXT NOT NULL DEFAULT '[]',
    payload_json TEXT NOT NULL DEFAULT '{}',
    prev_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trace_lot_events (
    lot_code TEXT NOT NULL,
    event_id INTEGER NOT NULL REFERENCES trace_events(id) ON DELETE RESTRICT,
    role TEXT NOT NULL CHECK(role IN ('input','output','revoke')),
    quantity_kg REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(lot_code, event_id, role)
);
CREATE INDEX IF NOT EXISTS idx_trace_lot_event_lot ON trace_lot_events(lot_code, event_id);
CREATE INDEX IF NOT EXISTS idx_trace_lot_event_event ON trace_lot_events(event_id);
CREATE TRIGGER IF NOT EXISTS trace_events_no_update BEFORE UPDATE ON trace_events
BEGIN
    SELECT RAISE(ABORT, 'trace_chain_immutable');
END;
CREATE TRIGGER IF NOT EXISTS trace_events_no_delete BEFORE DELETE ON trace_events
BEGIN
    SELECT RAISE(ABORT, 'trace_chain_immutable');
END;
CREATE TRIGGER IF NOT EXISTS trace_lot_events_no_update BEFORE UPDATE ON trace_lot_events
BEGIN
    SELECT RAISE(ABORT, 'trace_chain_immutable');
END;
CREATE TRIGGER IF NOT EXISTS trace_lot_events_no_delete BEFORE DELETE ON trace_lot_events
BEGIN
    SELECT RAISE(ABORT, 'trace_chain_immutable');
END;
"""

GENESIS_HASH = "0" * 64
QTY_EPS = 1e-6


def ensure_schema() -> None:
    get_connection().executescript(SCHEMA)


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError("事件时间格式无效", context={"value": value}) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _norm_time(value: str) -> str:
    return _parse_time(value).isoformat(timespec="seconds")


def _canonical(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _event_hash(
    *,
    event_type: str,
    event_time: str,
    actor: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    payload: dict[str, Any],
    prev_hash: str,
) -> str:
    body = {
        "event_type": event_type,
        "event_time": event_time,
        "actor": actor,
        "inputs": inputs,
        "outputs": outputs,
        "payload": payload,
        "prev_hash": prev_hash,
    }
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def _normalize_side(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {"lot_code": str(entry["lot_code"]).strip(), "quantity_kg": round(float(entry["quantity_kg"]), 6)}
        for entry in entries
    ]
    return sorted(normalized, key=lambda item: item["lot_code"])


class TraceService:
    """不可篡改的批次追溯事件链：来源、拆分、合并、节点交接与撤销。"""

    def __init__(self, connection: sqlite3.Connection | None = None):
        self.connection = connection or get_connection()
        ensure_schema()

    # ------------------------------------------------------------------ 写入

    def record_origin(self, payload: dict[str, Any]) -> dict[str, Any]:
        actor = payload.get("actor") or payload.get("supplier", "system")
        body = {
            "supplier": payload["supplier"],
            "origin": payload["origin"],
            "product_name": payload.get("product_name", ""),
            "harvest_date": payload.get("harvest_date", ""),
            "certificate_no": payload.get("certificate_no", ""),
            "documents": payload.get("documents", []),
        }
        outputs = [{"lot_code": payload["lot_code"], "quantity_kg": payload["quantity_kg"]}]
        return self._append("genesis", payload["event_time"], actor, [], outputs, body)

    def record_split(self, payload: dict[str, Any]) -> dict[str, Any]:
        actor = payload.get("actor", "warehouse")
        inputs = [{"lot_code": payload["lot_code"], "quantity_kg": payload["quantity_kg"]}]
        outputs = payload["outputs"]
        body = {"note": payload.get("note", ""), "documents": payload.get("documents", [])}
        return self._append("split", payload["event_time"], actor, inputs, outputs, body)

    def record_merge(self, payload: dict[str, Any]) -> dict[str, Any]:
        actor = payload.get("actor", "warehouse")
        outputs = [{"lot_code": payload["lot_code"], "quantity_kg": payload["quantity_kg"]}]
        body = {"note": payload.get("note", ""), "documents": payload.get("documents", [])}
        return self._append("merge", payload["event_time"], actor, payload["inputs"], outputs, body)

    def record_handover(self, payload: dict[str, Any]) -> dict[str, Any]:
        actor = payload.get("actor") or payload.get("handler", "dispatcher")
        side = [{"lot_code": payload["lot_code"], "quantity_kg": payload["quantity_kg"]}]
        body = {
            "from_node": payload["from_node"],
            "to_node": payload["to_node"],
            "handler": payload.get("handler", ""),
            "shipment_code": payload.get("shipment_code", ""),
            "documents": payload.get("documents", []),
        }
        return self._append("handover", payload["event_time"], actor, side, side, body)

    def revoke_event(self, event_id: int, reason: str, actor: str = "supervisor", event_time: str | None = None) -> dict[str, Any]:
        if not reason or not reason.strip():
            raise ValidationError("撤销原因不能为空", context={"field": "reason"})
        event_time = _norm_time(event_time or _now())
        with transaction(immediate=True) as connection:
            target = connection.execute("SELECT * FROM trace_events WHERE id=?", (event_id,)).fetchone()
            if target is None:
                raise NotFoundError("追溯事件不存在", context={"event_id": event_id})
            if target["event_type"] == "revoke":
                raise ConflictError("撤销事件不可再撤销", context={"reason": "revoke_not_revocable", "event_id": event_id})
            if self._revocation(connection, event_id) is not None:
                raise ConflictError("事件已被撤销", context={"reason": "already_revoked", "event_id": event_id})
            target_time = _parse_time(target["event_time"])
            if _parse_time(event_time) < target_time:
                raise ConflictError(
                    "撤销时间早于原事件，属于逆序操作",
                    context={"reason": "reverse_order", "event_time": event_time},
                )
            # 模拟撤销后的链：若仍有下游事件依赖被撤销事件导致数量不平衡，拒绝撤销。
            touched_lots = {
                row["lot_code"]
                for row in connection.execute("SELECT DISTINCT lot_code FROM trace_lot_events WHERE event_id=?", (event_id,)).fetchall()
            }
            balances, generated, last_times = self._replay(connection, excluded_event_id=event_id)
            bound = max((last_times[lot] for lot in touched_lots if lot in last_times), default=None)
            if bound is not None and _parse_time(event_time) < bound:
                raise ConflictError(
                    "撤销时间早于该批次链上的后续事件，属于逆序操作",
                    context={"reason": "reverse_order", "event_time": event_time, "last_event_time": bound.isoformat()},
                )
            payload = {"target_event_id": event_id, "reason": reason.strip()}
            prev_row = connection.execute("SELECT event_hash FROM trace_events ORDER BY id DESC LIMIT 1").fetchone()
            prev_hash = prev_row["event_hash"] if prev_row else GENESIS_HASH
            event_hash = _event_hash(
                event_type="revoke",
                event_time=event_time,
                actor=actor,
                inputs=[],
                outputs=[],
                payload=payload,
                prev_hash=prev_hash,
            )
            now = _now()
            cursor = connection.execute(
                "INSERT INTO trace_events(event_type,event_time,actor,inputs_json,outputs_json,payload_json,prev_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                ("revoke", event_time, actor, "[]", "[]", json.dumps(payload, ensure_ascii=False), prev_hash, event_hash, now),
            )
            revoke_id = cursor.lastrowid
            for lot in connection.execute("SELECT DISTINCT lot_code FROM trace_lot_events WHERE event_id=?", (event_id,)).fetchall():
                connection.execute(
                    "INSERT INTO trace_lot_events(lot_code,event_id,role,quantity_kg) VALUES(?,?,?,0)",
                    (lot["lot_code"], revoke_id, "revoke"),
                )
            return self.get_event(revoke_id) or {}

    def _append(
        self,
        event_type: str,
        event_time: str,
        actor: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        inputs = _normalize_side(inputs)
        outputs = _normalize_side(outputs)
        self._validate_shape(event_type, inputs, outputs)
        event_time = _norm_time(event_time)
        with transaction(immediate=True) as connection:
            balances, generated, last_times = self._replay(connection)
            candidate = {"event_type": event_type, "inputs": inputs, "outputs": outputs}
            self._check_and_apply(candidate, balances, generated)
            touched_lots = {entry["lot_code"] for entry in inputs} | {entry["lot_code"] for entry in outputs}
            bound = max((last_times[lot] for lot in touched_lots if lot in last_times), default=None)
            if bound is not None and _parse_time(event_time) < bound:
                raise ConflictError(
                    "事件时间早于该批次链上的前序事件，属于逆序操作",
                    context={"reason": "reverse_order", "event_time": event_time, "last_event_time": bound.isoformat()},
                )
            prev_row = connection.execute("SELECT event_hash FROM trace_events ORDER BY id DESC LIMIT 1").fetchone()
            prev_hash = prev_row["event_hash"] if prev_row else GENESIS_HASH
            event_hash = _event_hash(
                event_type=event_type,
                event_time=event_time,
                actor=actor,
                inputs=inputs,
                outputs=outputs,
                payload=payload,
                prev_hash=prev_hash,
            )
            now = _now()
            cursor = connection.execute(
                "INSERT INTO trace_events(event_type,event_time,actor,inputs_json,outputs_json,payload_json,prev_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    event_type,
                    event_time,
                    actor,
                    json.dumps(inputs, ensure_ascii=False),
                    json.dumps(outputs, ensure_ascii=False),
                    json.dumps(payload, ensure_ascii=False),
                    prev_hash,
                    event_hash,
                    now,
                ),
            )
            event_id = cursor.lastrowid
            connection.executemany(
                "INSERT INTO trace_lot_events(lot_code,event_id,role,quantity_kg) VALUES(?,?,?,?)",
                [(entry["lot_code"], event_id, "input", entry["quantity_kg"]) for entry in inputs]
                + [(entry["lot_code"], event_id, "output", entry["quantity_kg"]) for entry in outputs],
            )
            return self.get_event(event_id) or {}

    # ------------------------------------------------------------------ 校验

    def _validate_shape(self, event_type: str, inputs: list[dict[str, Any]], outputs: list[dict[str, Any]]) -> None:
        for side_name, entries in (("inputs", inputs), ("outputs", outputs)):
            for entry in entries:
                if entry["quantity_kg"] <= 0:
                    raise ValidationError(
                        "数量必须为正数",
                        context={"field": side_name, "lot_code": entry["lot_code"]},
                    )
        in_lots = [entry["lot_code"] for entry in inputs]
        out_lots = [entry["lot_code"] for entry in outputs]
        if len(in_lots) != len(set(in_lots)) or len(out_lots) != len(set(out_lots)):
            raise ValidationError("同一事件中批次不得重复出现", context={"reason": "duplicate_lot"})
        if event_type == "genesis":
            if inputs or len(outputs) != 1:
                raise ValidationError("来源事件必须恰好产生一个批次", context={"reason": "bad_shape"})
        elif event_type == "handover":
            if len(inputs) != 1 or len(outputs) != 1 or in_lots != out_lots:
                raise ValidationError("交接事件必须在同一批次上发生且数量不变", context={"reason": "bad_shape"})
        elif event_type == "split":
            if len(inputs) != 1 or len(outputs) < 2:
                raise ValidationError("拆分事件需要一个输入批次和至少两个输出批次", context={"reason": "bad_shape"})
            if set(in_lots) & set(out_lots):
                raise ValidationError("拆分产出必须使用新批次编码", context={"reason": "output_reuses_input"})
            total = sum(entry["quantity_kg"] for entry in outputs)
            if abs(total - inputs[0]["quantity_kg"]) > QTY_EPS:
                raise ConflictError(
                    "拆分数量不平衡：产出合计与投入不一致",
                    context={"reason": "quantity_mismatch", "input_kg": inputs[0]["quantity_kg"], "output_total_kg": round(total, 6)},
                )
        elif event_type == "merge":
            if len(inputs) < 2 or len(outputs) != 1:
                raise ValidationError("合并事件需要至少两个输入批次和一个输出批次", context={"reason": "bad_shape"})
            if set(in_lots) & set(out_lots):
                raise ValidationError("合并产出必须使用新批次编码", context={"reason": "output_reuses_input"})
            total = sum(entry["quantity_kg"] for entry in inputs)
            if abs(total - outputs[0]["quantity_kg"]) > QTY_EPS:
                raise ConflictError(
                    "合并数量不平衡：产出与投入合计不一致",
                    context={"reason": "quantity_mismatch", "input_total_kg": round(total, 6), "output_kg": outputs[0]["quantity_kg"]},
                )

    def _check_and_apply(
        self,
        event: sqlite3.Row | dict[str, Any],
        balances: dict[str, float],
        generated: set[str],
    ) -> None:
        event_type = event["event_type"]
        inputs = self._side(event, "inputs")
        outputs = self._side(event, "outputs")
        event_id = event["id"] if isinstance(event, sqlite3.Row) else None
        context = {"event_id": event_id}

        def fail(reason: str, message: str, **extra: Any) -> None:
            raise ConflictError(message, context={**context, "reason": reason, **extra})

        if event_type == "genesis":
            lot = outputs[0]["lot_code"]
            if lot in generated:
                fail("lot_already_sourced", "批次已存在来源事件，不得重复登记", lot_code=lot)
            balances[lot] += outputs[0]["quantity_kg"]
            generated.add(lot)
            return
        if event_type == "handover":
            lot, quantity = inputs[0]["lot_code"], inputs[0]["quantity_kg"]
            if lot not in generated:
                fail("lot_not_sourced", "批次尚无来源，无法交接", lot_code=lot)
            if balances[lot] + QTY_EPS < quantity:
                fail("insufficient_quantity", "可用数量不足，无法交接", lot_code=lot, available_kg=round(balances[lot], 6), requested_kg=quantity)
            return
        if event_type == "split":
            source, quantity = inputs[0]["lot_code"], inputs[0]["quantity_kg"]
            total = sum(entry["quantity_kg"] for entry in outputs)
            if source not in generated:
                fail("lot_not_sourced", "批次尚无来源，无法拆分", lot_code=source)
            if balances[source] + QTY_EPS < quantity:
                fail("insufficient_quantity", "可用数量不足，无法拆分", lot_code=source, available_kg=round(balances[source], 6), requested_kg=quantity)
            if abs(total - quantity) > QTY_EPS:
                fail("quantity_mismatch", "拆分数量不平衡", input_kg=quantity, output_total_kg=round(total, 6))
            for entry in outputs:
                if entry["lot_code"] in generated:
                    fail("lot_already_sourced", "输出批次已被占用", lot_code=entry["lot_code"])
            balances[source] -= quantity
            for entry in outputs:
                balances[entry["lot_code"]] += entry["quantity_kg"]
                generated.add(entry["lot_code"])
            return
        if event_type == "merge":
            total = sum(entry["quantity_kg"] for entry in inputs)
            output = outputs[0]
            if abs(total - output["quantity_kg"]) > QTY_EPS:
                fail("quantity_mismatch", "合并数量不平衡", input_total_kg=round(total, 6), output_kg=output["quantity_kg"])
            for entry in inputs:
                lot, quantity = entry["lot_code"], entry["quantity_kg"]
                if lot not in generated:
                    fail("lot_not_sourced", "输入批次尚无来源，无法合并", lot_code=lot)
                if balances[lot] + QTY_EPS < quantity:
                    fail("insufficient_quantity", "可用数量不足，无法合并", lot_code=lot, available_kg=round(balances[lot], 6), requested_kg=quantity)
            if output["lot_code"] in generated:
                fail("lot_already_sourced", "输出批次已被占用", lot_code=output["lot_code"])
            for entry in inputs:
                balances[entry["lot_code"]] -= entry["quantity_kg"]
            balances[output["lot_code"]] += output["quantity_kg"]
            generated.add(output["lot_code"])

    @staticmethod
    def _side(event: sqlite3.Row | dict[str, Any], name: str) -> list[dict[str, Any]]:
        if isinstance(event, sqlite3.Row):
            return json.loads(event[f"{name}_json"])
        return event[name]

    def _replay(
        self, connection: sqlite3.Connection, excluded_event_id: int | None = None
    ) -> tuple[dict[str, float], set[str], dict[str, datetime]]:
        balances: dict[str, float] = defaultdict(float)
        generated: set[str] = set()
        last_times: dict[str, datetime] = {}
        rows = connection.execute("SELECT * FROM trace_events ORDER BY id").fetchall()
        for row in rows:
            if row["id"] == excluded_event_id:
                continue
            if row["event_type"] == "revoke" or self._revocation(connection, row["id"]) is not None:
                continue
            self._check_and_apply(row, balances, generated)
            event_time = _parse_time(row["event_time"])
            for lot in {entry["lot_code"] for entry in json.loads(row["inputs_json"]) + json.loads(row["outputs_json"])}:
                if lot not in last_times or event_time > last_times[lot]:
                    last_times[lot] = event_time
        return balances, generated, last_times

    def _revocation(self, connection: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
        rows = connection.execute(
            "SELECT t.* FROM trace_events t JOIN trace_events p ON json_extract(t.payload_json,'$.target_event_id')=p.id "
            "WHERE p.id=? AND t.event_type='revoke' ORDER BY t.id",
            (event_id,),
        ).fetchall()
        return rows[0] if rows else None

    # ------------------------------------------------------------------ 查询

    def get_event(self, event_id: int) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM trace_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            return None
        return self._serialize(row)

    def history(self, lot_code: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT e.* FROM trace_events e WHERE e.id IN "
            "(SELECT event_id FROM trace_lot_events WHERE lot_code=?) ORDER BY e.event_time, e.id",
            (lot_code,),
        ).fetchall()
        if not rows:
            raise NotFoundError("批次没有任何追溯事件", context={"lot_code": lot_code})
        balances, _, _ = self._replay(self.connection)
        return {
            "lot_code": lot_code,
            "current_balance_kg": round(balances.get(lot_code, 0.0), 6),
            "events": [self._serialize(row) for row in rows],
        }

    def lineage(self, lot_code: str, direction: str = "both") -> dict[str, Any]:
        if direction not in {"upstream", "downstream", "both"}:
            raise ValidationError("方向只能是 upstream、downstream 或 both", context={"direction": direction})
        rows = self.connection.execute("SELECT * FROM trace_events ORDER BY id").fetchall()
        events = {row["id"]: row for row in rows}
        if not self.connection.execute("SELECT 1 FROM trace_lot_events WHERE lot_code=? LIMIT 1", (lot_code,)).fetchone():
            raise NotFoundError("批次没有任何追溯事件", context={"lot_code": lot_code})

        producers: dict[str, list[int]] = defaultdict(list)
        consumers: dict[str, list[int]] = defaultdict(list)
        for row in rows:
            if row["event_type"] == "revoke":
                continue
            for entry in json.loads(row["outputs_json"]):
                producers[entry["lot_code"]].append(row["id"])
            for entry in json.loads(row["inputs_json"]):
                consumers[entry["lot_code"]].append(row["id"])

        visited: set[int] = set()

        def walk_upstream(code: str) -> None:
            for event_id in producers.get(code, ()):
                if event_id in visited:
                    continue
                visited.add(event_id)
                for entry in json.loads(events[event_id]["inputs_json"]):
                    walk_upstream(entry["lot_code"])

        def walk_downstream(code: str) -> None:
            for event_id in consumers.get(code, ()):
                if event_id in visited:
                    continue
                visited.add(event_id)
                for entry in json.loads(events[event_id]["outputs_json"]):
                    walk_downstream(entry["lot_code"])

        if direction in ("upstream", "both"):
            walk_upstream(lot_code)
        if direction in ("downstream", "both"):
            walk_downstream(lot_code)

        # 撤销事件不删除历史：作为被撤销事件的附属节点一并展开。
        for row in rows:
            if row["event_type"] != "revoke":
                continue
            target_id = json.loads(row["payload_json"])["target_event_id"]
            if target_id in visited:
                visited.add(row["id"])

        ordered = [events[event_id] for event_id in sorted(visited, key=lambda ident: (events[ident]["event_time"], ident))]
        balances, _, _ = self._replay(self.connection)
        involved_lots = sorted(
            {
                entry["lot_code"]
                for row in ordered
                for entry in json.loads(row["inputs_json"]) + json.loads(row["outputs_json"])
            }
        )
        root_lots = [
            entry["lot_code"]
            for row in ordered
            if row["event_type"] == "genesis"
            for entry in json.loads(row["outputs_json"])
        ]
        return {
            "lot_code": lot_code,
            "direction": direction,
            "events": [self._serialize(row) for row in ordered],
            "lots": [
                {"lot_code": code, "current_balance_kg": round(balances.get(code, 0.0), 6)}
                for code in involved_lots
            ],
            "root_lots": sorted(set(root_lots)),
        }

    def verify_chain(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT * FROM trace_events ORDER BY id").fetchall()
        prev_hash = GENESIS_HASH
        broken_at: int | None = None
        for row in rows:
            expected = _event_hash(
                event_type=row["event_type"],
                event_time=row["event_time"],
                actor=row["actor"],
                inputs=json.loads(row["inputs_json"]),
                outputs=json.loads(row["outputs_json"]),
                payload=json.loads(row["payload_json"]),
                prev_hash=prev_hash,
            )
            if row["prev_hash"] != prev_hash or row["event_hash"] != expected:
                broken_at = row["id"]
                break
            prev_hash = row["event_hash"]
        dangling: list[int] = []
        if broken_at is None:
            for row in rows:
                if row["event_type"] != "revoke":
                    continue
                target_id = json.loads(row["payload_json"])["target_event_id"]
                if target_id not in {item["id"] for item in rows}:
                    dangling.append(row["id"])
        return {
            "ok": broken_at is None and not dangling,
            "events": len(rows),
            "broken_at_event_id": broken_at,
            "dangling_revocation_event_ids": dangling,
        }

    def _serialize(self, row: sqlite3.Row) -> dict[str, Any]:
        revocation = self._revocation(self.connection, row["id"]) if row["event_type"] != "revoke" else None
        result = {
            "id": row["id"],
            "event_type": row["event_type"],
            "event_time": row["event_time"],
            "actor": row["actor"],
            "inputs": json.loads(row["inputs_json"]),
            "outputs": json.loads(row["outputs_json"]),
            "payload": json.loads(row["payload_json"]),
            "prev_hash": row["prev_hash"],
            "event_hash": row["event_hash"],
            "created_at": row["created_at"],
            "revoked": revocation is not None,
            "revoked_reason": json.loads(revocation["payload_json"])["reason"] if revocation else None,
            "revoked_event_id": revocation["id"] if revocation else None,
            "revoked_at": revocation["event_time"] if revocation else None,
        }
        if row["event_type"] == "revoke":
            result["target_event_id"] = json.loads(row["payload_json"])["target_event_id"]
        return result

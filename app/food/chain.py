from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from app.database import get_connection, transaction
from app.food.service import ensure_schema as ensure_food_schema

EPSILON = 1e-6
GENESIS_HASH = "0" * 64
EVENT_TYPES = ("register", "split", "merge", "transfer", "consume", "revoke")
FLOW_TYPES = ("split", "merge", "transfer")
BLOCKED_CONSUME_STATUS = {"held", "recalled", "destroyed"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS trace_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_code TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK(event_type IN ('register','split','merge','transfer','consume','revoke')),
    occurred_at TEXT NOT NULL,
    actor TEXT NOT NULL,
    from_node TEXT NOT NULL DEFAULT '',
    to_node TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    target_event_id INTEGER REFERENCES trace_events(id),
    request_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trace_event_inputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES trace_events(id),
    lot_id INTEGER NOT NULL REFERENCES food_lots(id),
    quantity_kg REAL NOT NULL CHECK(quantity_kg > 0),
    UNIQUE(event_id, lot_id)
);
CREATE TABLE IF NOT EXISTS trace_event_outputs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES trace_events(id),
    lot_id INTEGER NOT NULL REFERENCES food_lots(id),
    quantity_kg REAL NOT NULL CHECK(quantity_kg > 0),
    UNIQUE(event_id, lot_id)
);
CREATE TABLE IF NOT EXISTS trace_event_credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL REFERENCES trace_events(id),
    credential_type TEXT NOT NULL,
    credential_no TEXT NOT NULL,
    issuer TEXT NOT NULL,
    issued_at TEXT NOT NULL DEFAULT '',
    UNIQUE(event_id, credential_type, credential_no)
);
CREATE INDEX IF NOT EXISTS idx_trace_inputs_lot ON trace_event_inputs(lot_id);
CREATE INDEX IF NOT EXISTS idx_trace_outputs_lot ON trace_event_outputs(lot_id);
CREATE INDEX IF NOT EXISTS idx_trace_events_target ON trace_events(target_event_id);
CREATE TRIGGER IF NOT EXISTS trg_trace_events_no_update BEFORE UPDATE ON trace_events
BEGIN SELECT RAISE(ABORT,'trace_events 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_events_no_delete BEFORE DELETE ON trace_events
BEGIN SELECT RAISE(ABORT,'trace_events 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_inputs_no_update BEFORE UPDATE ON trace_event_inputs
BEGIN SELECT RAISE(ABORT,'trace_event_inputs 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_inputs_no_delete BEFORE DELETE ON trace_event_inputs
BEGIN SELECT RAISE(ABORT,'trace_event_inputs 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_outputs_no_update BEFORE UPDATE ON trace_event_outputs
BEGIN SELECT RAISE(ABORT,'trace_event_outputs 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_outputs_no_delete BEFORE DELETE ON trace_event_outputs
BEGIN SELECT RAISE(ABORT,'trace_event_outputs 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_credentials_no_update BEFORE UPDATE ON trace_event_credentials
BEGIN SELECT RAISE(ABORT,'trace_event_credentials 不可篡改'); END;
CREATE TRIGGER IF NOT EXISTS trg_trace_credentials_no_delete BEFORE DELETE ON trace_event_credentials
BEGIN SELECT RAISE(ABORT,'trace_event_credentials 不可篡改'); END;
"""

# 已被撤销事件 id 集合；余额与因果顺序只统计未撤销事件
_REVOKED_IDS = "SELECT target_event_id FROM trace_events WHERE event_type='revoke' AND target_event_id IS NOT NULL"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_schema() -> None:
    ensure_food_schema()
    get_connection().executescript(SCHEMA)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _normalize_time(value: Any) -> str:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _normalize(payload: dict[str, Any]) -> dict[str, Any]:
    """统一编码、时间与默认值，保证同一请求得到同一指纹。"""
    p = dict(payload)
    p["event_code"] = str(p["event_code"]).strip()
    p["event_type"] = str(p["event_type"]).strip().lower()
    p["occurred_at"] = _normalize_time(p["occurred_at"])
    p["actor"] = str(p["actor"]).strip()
    p["from_node"] = str(p.get("from_node") or "").strip()
    p["to_node"] = str(p.get("to_node") or "").strip()
    p["reason"] = str(p.get("reason") or "").strip()
    target = p.get("target_event_code")
    p["target_event_code"] = str(target).strip() if target else None
    p["inputs"] = [
        {"lot_code": str(item["lot_code"]).strip().upper(), "quantity_kg": float(item["quantity_kg"])}
        for item in p.get("inputs") or []
    ]
    p["outputs"] = [
        {
            "lot_code": str(item["lot_code"]).strip().upper(),
            "quantity_kg": float(item["quantity_kg"]),
            "trace_code": str(item["trace_code"]).strip().upper() if item.get("trace_code") else None,
        }
        for item in p.get("outputs") or []
    ]
    p["credentials"] = [
        {
            "credential_type": str(item["credential_type"]).strip(),
            "credential_no": str(item["credential_no"]).strip(),
            "issuer": str(item["issuer"]).strip(),
            "issued_at": str(item.get("issued_at") or "").strip(),
        }
        for item in p.get("credentials") or []
    ]
    return p


def _fingerprint(p: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_code": p["event_code"],
        "event_type": p["event_type"],
        "occurred_at": p["occurred_at"],
        "actor": p["actor"],
        "from_node": p["from_node"],
        "to_node": p["to_node"],
        "reason": p["reason"],
        "target_event_code": p["target_event_code"],
        "inputs": sorted(p["inputs"], key=lambda item: item["lot_code"]),
        "outputs": sorted(p["outputs"], key=lambda item: item["lot_code"]),
        "credentials": sorted(p["credentials"], key=lambda item: (item["credential_type"], item["credential_no"])),
    }


def _content_dict(
    *,
    event_code: str,
    event_type: str,
    occurred_at: str,
    actor: str,
    from_node: str,
    to_node: str,
    reason: str,
    target_event_id: int | None,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    credentials: list[dict[str, Any]],
) -> dict[str, Any]:
    """事件哈希内容；写入与校验两条路径必须生成完全一致的结构。"""
    return {
        "event_code": event_code,
        "event_type": event_type,
        "occurred_at": occurred_at,
        "actor": actor,
        "from_node": from_node,
        "to_node": to_node,
        "reason": reason,
        "target_event_id": target_event_id,
        "inputs": sorted(
            ({"lot_id": item["lot_id"], "quantity_kg": item["quantity_kg"]} for item in inputs),
            key=lambda item: item["lot_id"],
        ),
        "outputs": sorted(
            ({"lot_id": item["lot_id"], "quantity_kg": item["quantity_kg"]} for item in outputs),
            key=lambda item: item["lot_id"],
        ),
        "credentials": sorted(
            (
                {
                    "credential_type": item["credential_type"],
                    "credential_no": item["credential_no"],
                    "issuer": item["issuer"],
                    "issued_at": item["issued_at"],
                }
                for item in credentials
            ),
            key=lambda item: (item["credential_type"], item["credential_no"]),
        ),
    }


def _lot_by_code(connection: sqlite3.Connection, lot_code: str) -> sqlite3.Row | None:
    return connection.execute("SELECT * FROM food_lots WHERE lot_code=?", (lot_code,)).fetchone()


class TraceChainService:
    """追溯事件链：追加式事件、数量平衡、因果顺序与哈希完整性。"""

    def __init__(self, connection: sqlite3.Connection | None = None):
        self.connection = connection or get_connection()
        ensure_schema()

    # ---------- 写入 ----------

    def append_event(self, payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """追加事件；返回 (事件详情, 是否为幂等重放)。"""
        p = _normalize(payload)
        request_hash = _sha256(_canonical(_fingerprint(p)))
        with transaction(immediate=True) as connection:
            existing = connection.execute("SELECT * FROM trace_events WHERE event_code=?", (p["event_code"],)).fetchone()
            if existing is not None:
                if existing["request_hash"] == request_hash:
                    return self._event_view(connection, existing), True
                raise ValueError(f"事件编码 {p['event_code']} 已存在且内容不一致")
            plan = self._validate(connection, p)
            event_id = self._insert(connection, p, plan, request_hash)
            event = connection.execute("SELECT * FROM trace_events WHERE id=?", (event_id,)).fetchone()
            return self._event_view(connection, event), False

    def _validate(self, connection: sqlite3.Connection, p: dict[str, Any]) -> dict[str, Any]:
        event_type = p["event_type"]
        if event_type not in EVENT_TYPES:
            raise ValueError(f"不支持的事件类型：{event_type}")

        inputs: list[tuple[sqlite3.Row, float]] = []
        seen: set[str] = set()
        for item in p["inputs"]:
            code = item["lot_code"]
            if code in seen:
                raise ValueError(f"投入批次重复：{code}")
            seen.add(code)
            if item["quantity_kg"] <= 0:
                raise ValueError(f"投入数量必须为正数：{code}")
            lot = _lot_by_code(connection, code)
            if lot is None:
                raise KeyError(code)
            inputs.append((lot, item["quantity_kg"]))

        out_seen: set[str] = set()
        for item in p["outputs"]:
            code = item["lot_code"]
            if code in out_seen:
                raise ValueError(f"产出批次重复：{code}")
            out_seen.add(code)
            if item["quantity_kg"] <= 0:
                raise ValueError(f"产出数量必须为正数：{code}")

        plan: dict[str, Any] = {"inputs": inputs, "outputs": [], "target": None}

        if event_type == "register":
            if inputs:
                raise ValueError("登记事件不允许投入批次")
            if len(p["outputs"]) != 1:
                raise ValueError("登记事件须且仅须一个产出批次")
            if not p["credentials"]:
                raise ValueError("登记事件必须附带来源凭证")
            out = p["outputs"][0]
            lot = _lot_by_code(connection, out["lot_code"])
            if lot is None:
                raise KeyError(out["lot_code"])
            if abs(out["quantity_kg"] - lot["quantity_kg"]) > EPSILON:
                raise ValueError(f"数量不一致：登记数量 {out['quantity_kg']} kg 与批次档案 {lot['quantity_kg']} kg 不符")
            if self._has_history(connection, lot["id"]):
                raise ValueError(f"批次 {out['lot_code']} 已入链，禁止重复登记")
            plan["outputs"] = [(lot, out["lot_code"], out["quantity_kg"], None)]
            return plan

        if event_type == "revoke":
            if inputs or p["outputs"]:
                raise ValueError("撤销事件不允许携带投入或产出")
            if not p["target_event_code"]:
                raise ValueError("撤销事件必须指定目标事件")
            target = connection.execute("SELECT * FROM trace_events WHERE event_code=?", (p["target_event_code"],)).fetchone()
            if target is None:
                raise KeyError(p["target_event_code"])
            if target["event_type"] == "revoke":
                raise ValueError("撤销事件不可再次撤销")
            if self._is_revoked(connection, target["id"]):
                raise ValueError(f"事件 {target['event_code']} 已被撤销，禁止重复撤销")
            if p["occurred_at"] < target["occurred_at"]:
                raise ValueError(f"逆序操作：撤销时间 {p['occurred_at']} 早于目标事件时间 {target['occurred_at']}")
            for row in connection.execute(
                "SELECT o.lot_id, o.quantity_kg, l.lot_code FROM trace_event_outputs o JOIN food_lots l ON l.id=o.lot_id WHERE o.event_id=?",
                (target["id"],),
            ).fetchall():
                balance = self._balance(connection, row["lot_id"])
                if balance < row["quantity_kg"] - EPSILON:
                    raise ValueError(
                        f"批次 {row['lot_code']} 可用余额 {balance} kg 低于撤销回退量 {row['quantity_kg']} kg，下游已流转，禁止撤销"
                    )
            plan["target"] = target
            return plan

        # split / merge / transfer / consume 的结构性校验
        if event_type == "split":
            if len(inputs) != 1:
                raise ValueError("拆分事件须且仅须一个投入批次")
            if len(p["outputs"]) < 2:
                raise ValueError("拆分事件至少产生两个产出批次")
        elif event_type == "merge":
            if len(inputs) < 2:
                raise ValueError("合并事件至少需要两个投入批次")
            if len(p["outputs"]) != 1:
                raise ValueError("合并事件须且仅须一个产出批次")
            products = sorted({lot["product_name"] for lot, _ in inputs})
            if len(products) > 1:
                raise ValueError("品类不一致，禁止合并：" + "、".join(products))
        elif event_type == "transfer":
            if len(inputs) != 1 or len(p["outputs"]) != 1:
                raise ValueError("交接事件须且仅须一个投入批次和一个产出批次")
            if not p["to_node"]:
                raise ValueError("交接事件必须填写去向节点")
        elif event_type == "consume":
            if not inputs:
                raise ValueError("供应事件至少需要一个投入批次")
            if p["outputs"]:
                raise ValueError("供应事件不允许产生新批次")
            if not p["to_node"]:
                raise ValueError("供应事件必须填写供应对象")
            for lot, _ in inputs:
                if lot["status"] in BLOCKED_CONSUME_STATUS:
                    raise ValueError(f"批次 {lot['lot_code']} 状态为 {lot['status']}，禁止供应餐桌")

        # 产出批次落位：拆分/合并必须派生新批次；交接可整批原位交接或派生新批次
        resolved: list[tuple[sqlite3.Row | None, str, float, str | None]] = []
        if event_type in ("split", "merge"):
            for item in p["outputs"]:
                if _lot_by_code(connection, item["lot_code"]) is not None:
                    raise ValueError(f"产出批次 {item['lot_code']} 已存在")
                resolved.append((None, item["lot_code"], item["quantity_kg"], item.get("trace_code")))
        elif event_type == "transfer":
            source_lot, _ = inputs[0]
            item = p["outputs"][0]
            if item["lot_code"] == source_lot["lot_code"]:
                balance = self._balance(connection, source_lot["id"])
                if abs(item["quantity_kg"] - balance) > EPSILON:
                    raise ValueError(
                        f"部分交接须派生新批次：批次 {source_lot['lot_code']} 余额 {balance} kg，交接 {item['quantity_kg']} kg"
                    )
                resolved.append((source_lot, item["lot_code"], item["quantity_kg"], None))
            else:
                if _lot_by_code(connection, item["lot_code"]) is not None:
                    raise ValueError(f"产出批次 {item['lot_code']} 已存在")
                resolved.append((None, item["lot_code"], item["quantity_kg"], item.get("trace_code")))
            current_node = self._current_node(connection, source_lot["id"])
            if p["from_node"] and current_node and p["from_node"] != current_node:
                raise ValueError(f"节点交接不连续：批次 {source_lot['lot_code']} 当前位于 {current_node}，与交接起点 {p['from_node']} 不符")
        plan["outputs"] = resolved

        # 数量平衡：流转类事件投入总量必须等于产出总量
        if event_type in FLOW_TYPES:
            total_in = sum(quantity for _, quantity in inputs)
            total_out = sum(quantity for _, _, quantity, _ in resolved)
            if abs(total_in - total_out) > EPSILON:
                raise ValueError(f"数量不平衡：投入总量 {total_in} kg，产出总量 {total_out} kg")

        # 余额与因果顺序：不得超支，不得早于投入批次最后活动时间
        for lot, quantity in inputs:
            balance = self._balance(connection, lot["id"])
            if quantity - balance > EPSILON:
                raise ValueError(f"数量不一致：批次 {lot['lot_code']} 可用余额 {balance} kg，无法支出 {quantity} kg")
            last = self._last_activity(connection, lot["id"])
            if last and p["occurred_at"] < last:
                raise ValueError(f"逆序操作：事件时间 {p['occurred_at']} 早于批次 {lot['lot_code']} 最后事件时间 {last}")
        return plan

    def _insert(self, connection: sqlite3.Connection, p: dict[str, Any], plan: dict[str, Any], request_hash: str) -> int:
        now = _now()
        outputs: list[dict[str, Any]] = []
        for lot, code, quantity, trace_code in plan["outputs"]:
            if lot is None:
                source = plan["inputs"][0][0]
                lot_id = self._derive_lot(connection, source, code, quantity, trace_code, p["actor"], now)
            else:
                lot_id = lot["id"]
            outputs.append({"lot_id": lot_id, "quantity_kg": quantity})
        inputs = [{"lot_id": lot["id"], "quantity_kg": quantity} for lot, quantity in plan["inputs"]]
        target_id = plan["target"]["id"] if plan["target"] is not None else None

        previous = connection.execute("SELECT event_hash FROM trace_events ORDER BY id DESC LIMIT 1").fetchone()
        prev_hash = previous["event_hash"] if previous else GENESIS_HASH
        content = _content_dict(
            event_code=p["event_code"],
            event_type=p["event_type"],
            occurred_at=p["occurred_at"],
            actor=p["actor"],
            from_node=p["from_node"],
            to_node=p["to_node"],
            reason=p["reason"],
            target_event_id=target_id,
            inputs=inputs,
            outputs=outputs,
            credentials=p["credentials"],
        )
        content_hash = _sha256(_canonical(content))
        event_hash = _sha256(f"{prev_hash}|{content_hash}")
        cursor = connection.execute(
            "INSERT INTO trace_events(event_code,event_type,occurred_at,actor,from_node,to_node,reason,target_event_id,request_hash,content_hash,prev_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                p["event_code"],
                p["event_type"],
                p["occurred_at"],
                p["actor"],
                p["from_node"],
                p["to_node"],
                p["reason"],
                target_id,
                request_hash,
                content_hash,
                prev_hash,
                event_hash,
                now,
            ),
        )
        event_id = cursor.lastrowid
        for item in inputs:
            connection.execute(
                "INSERT INTO trace_event_inputs(event_id,lot_id,quantity_kg) VALUES(?,?,?)",
                (event_id, item["lot_id"], item["quantity_kg"]),
            )
        for item in outputs:
            connection.execute(
                "INSERT INTO trace_event_outputs(event_id,lot_id,quantity_kg) VALUES(?,?,?)",
                (event_id, item["lot_id"], item["quantity_kg"]),
            )
        for credential in p["credentials"]:
            connection.execute(
                "INSERT INTO trace_event_credentials(event_id,credential_type,credential_no,issuer,issued_at) VALUES(?,?,?,?,?)",
                (event_id, credential["credential_type"], credential["credential_no"], credential["issuer"], credential["issued_at"]),
            )
        involved = sorted({item["lot_id"] for item in inputs} | {item["lot_id"] for item in outputs})
        audit_payload = json.dumps({"event_code": p["event_code"]}, ensure_ascii=False)
        if not involved:
            connection.execute(
                "INSERT INTO food_audit(lot_id,action,actor,payload_json,created_at) VALUES(?,?,?,?,?)",
                (None, f"trace.{p['event_type']}", p["actor"], audit_payload, now),
            )
        for lot_id in involved:
            connection.execute(
                "INSERT INTO food_audit(lot_id,action,actor,payload_json,created_at) VALUES(?,?,?,?,?)",
                (lot_id, f"trace.{p['event_type']}", p["actor"], audit_payload, now),
            )
        return event_id

    def _derive_lot(
        self,
        connection: sqlite3.Connection,
        source: sqlite3.Row,
        lot_code: str,
        quantity: float,
        trace_code: str | None,
        actor: str,
        now: str,
    ) -> int:
        cursor = connection.execute(
            "INSERT INTO food_lots(lot_code,product_name,category,supplier,origin,harvest_date,quantity_kg,trace_code,status,risk_level,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                lot_code,
                source["product_name"],
                source["category"],
                source["supplier"],
                source["origin"],
                source["harvest_date"],
                quantity,
                trace_code or f"{lot_code}-TRACE",
                source["status"],
                source["risk_level"],
                now,
                now,
            ),
        )
        lot_id = cursor.lastrowid
        connection.execute(
            "INSERT INTO food_audit(lot_id,action,actor,payload_json,created_at) VALUES(?,?,?,?,?)",
            (lot_id, "lot.derive", actor, json.dumps({"lot_code": lot_code, "source_lot_code": source["lot_code"]}, ensure_ascii=False), now),
        )
        return lot_id

    # ---------- 链内状态 ----------

    def _balance(self, connection: sqlite3.Connection, lot_id: int) -> float:
        inflow = connection.execute(
            f"SELECT COALESCE(SUM(o.quantity_kg),0) FROM trace_event_outputs o JOIN trace_events e ON e.id=o.event_id WHERE o.lot_id=? AND e.id NOT IN ({_REVOKED_IDS})",
            (lot_id,),
        ).fetchone()[0]
        outflow = connection.execute(
            f"SELECT COALESCE(SUM(i.quantity_kg),0) FROM trace_event_inputs i JOIN trace_events e ON e.id=i.event_id WHERE i.lot_id=? AND e.id NOT IN ({_REVOKED_IDS})",
            (lot_id,),
        ).fetchone()[0]
        return round(inflow - outflow, 6)

    def _has_history(self, connection: sqlite3.Connection, lot_id: int) -> bool:
        row = connection.execute(
            "SELECT (SELECT COUNT(*) FROM trace_event_inputs WHERE lot_id=?) + (SELECT COUNT(*) FROM trace_event_outputs WHERE lot_id=?)",
            (lot_id, lot_id),
        ).fetchone()
        return bool(row[0])

    def _is_revoked(self, connection: sqlite3.Connection, event_id: int) -> bool:
        return (
            connection.execute("SELECT 1 FROM trace_events WHERE event_type='revoke' AND target_event_id=?", (event_id,)).fetchone()
            is not None
        )

    def _last_activity(self, connection: sqlite3.Connection, lot_id: int) -> str | None:
        row = connection.execute(
            f"""SELECT MAX(e.occurred_at) FROM trace_events e
                WHERE e.event_type <> 'revoke' AND e.id NOT IN ({_REVOKED_IDS})
                  AND (e.id IN (SELECT event_id FROM trace_event_inputs WHERE lot_id=?)
                    OR e.id IN (SELECT event_id FROM trace_event_outputs WHERE lot_id=?))""",
            (lot_id, lot_id),
        ).fetchone()
        return row[0]

    def _current_node(self, connection: sqlite3.Connection, lot_id: int) -> str | None:
        row = connection.execute(
            f"""SELECT e.to_node FROM trace_events e
                WHERE e.event_type <> 'revoke' AND e.id NOT IN ({_REVOKED_IDS})
                  AND e.id IN (SELECT event_id FROM trace_event_outputs WHERE lot_id=?)
                ORDER BY e.occurred_at DESC, e.id DESC LIMIT 1""",
            (lot_id,),
        ).fetchone()
        return row[0] if row else None

    # ---------- 查询 ----------

    def get_event(self, event_id: int) -> dict[str, Any] | None:
        event = self.connection.execute("SELECT * FROM trace_events WHERE id=?", (event_id,)).fetchone()
        if event is None:
            return None
        return self._event_view(self.connection, event)

    def list_events(self, event_type: str | None = None, lot_code: str | None = None, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if event_type:
            clauses.append("e.event_type=?")
            params.append(event_type)
        if lot_code:
            clauses.append(
                "(e.id IN (SELECT i.event_id FROM trace_event_inputs i JOIN food_lots l ON l.id=i.lot_id WHERE l.lot_code=?)"
                " OR e.id IN (SELECT o.event_id FROM trace_event_outputs o JOIN food_lots l2 ON l2.id=o.lot_id WHERE l2.lot_code=?))"
            )
            params.extend([lot_code, lot_code])
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            f"SELECT e.* FROM trace_events e{where} ORDER BY e.id DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
        return [self._event_view(self.connection, row) for row in rows]

    def lot_balance(self, lot_id: int) -> dict[str, Any]:
        lot = self._require_lot(lot_id)
        inflow = self.connection.execute(
            f"SELECT COALESCE(SUM(o.quantity_kg),0) FROM trace_event_outputs o JOIN trace_events e ON e.id=o.event_id WHERE o.lot_id=? AND e.id NOT IN ({_REVOKED_IDS})",
            (lot_id,),
        ).fetchone()[0]
        outflow = self.connection.execute(
            f"SELECT COALESCE(SUM(i.quantity_kg),0) FROM trace_event_inputs i JOIN trace_events e ON e.id=i.event_id WHERE i.lot_id=? AND e.id NOT IN ({_REVOKED_IDS})",
            (lot_id,),
        ).fetchone()[0]
        return {
            "lot_id": lot["id"],
            "lot_code": lot["lot_code"],
            "product_name": lot["product_name"],
            "in_kg": round(inflow, 6),
            "out_kg": round(outflow, 6),
            "balance_kg": round(inflow - outflow, 6),
            "current_node": self._current_node(self.connection, lot_id),
        }

    def trace_upstream(self, lot_id: int) -> dict[str, Any]:
        lot = self._require_lot(lot_id)
        lots, events = self._traverse(lot_id, "upstream")
        registered = {
            item["lot_id"] for event in events.values() if event["event_type"] == "register" for item in event["outputs"]
        }
        return {
            "lot": self._lot_view(lot),
            "events": sorted(events.values(), key=lambda item: (item["occurred_at"], item["id"])),
            "lots": [self._lot_view(row) for row in lots.values()],
            "origin_lots": [self._lot_view(row) for lot_key, row in lots.items() if lot_key in registered],
        }

    def trace_downstream(self, lot_id: int) -> dict[str, Any]:
        lot = self._require_lot(lot_id)
        lots, events = self._traverse(lot_id, "downstream")
        served = []
        for event in events.values():
            if event["event_type"] == "consume" and not event["revoked"]:
                served.append(
                    {
                        "event_code": event["event_code"],
                        "to_node": event["to_node"],
                        "occurred_at": event["occurred_at"],
                        "quantity_kg": round(sum(item["quantity_kg"] for item in event["inputs"]), 6),
                    }
                )
        return {
            "lot": self._lot_view(lot),
            "events": sorted(events.values(), key=lambda item: (item["occurred_at"], item["id"])),
            "lots": [self._lot_view(row) for row in lots.values()],
            "served_tables": sorted(served, key=lambda item: (item["occurred_at"], item["event_code"])),
        }

    def verify_chain(self) -> dict[str, Any]:
        events = self.connection.execute("SELECT * FROM trace_events ORDER BY id").fetchall()
        prev_hash = GENESIS_HASH
        for index, event in enumerate(events):
            inputs = [
                dict(row)
                for row in self.connection.execute("SELECT lot_id,quantity_kg FROM trace_event_inputs WHERE event_id=?", (event["id"],)).fetchall()
            ]
            outputs = [
                dict(row)
                for row in self.connection.execute("SELECT lot_id,quantity_kg FROM trace_event_outputs WHERE event_id=?", (event["id"],)).fetchall()
            ]
            credentials = [
                dict(row)
                for row in self.connection.execute(
                    "SELECT credential_type,credential_no,issuer,issued_at FROM trace_event_credentials WHERE event_id=?",
                    (event["id"],),
                ).fetchall()
            ]
            content = _content_dict(
                event_code=event["event_code"],
                event_type=event["event_type"],
                occurred_at=event["occurred_at"],
                actor=event["actor"],
                from_node=event["from_node"],
                to_node=event["to_node"],
                reason=event["reason"],
                target_event_id=event["target_event_id"],
                inputs=inputs,
                outputs=outputs,
                credentials=credentials,
            )
            content_hash = _sha256(_canonical(content))
            expected_hash = _sha256(f"{prev_hash}|{content_hash}")
            if content_hash != event["content_hash"] or event["prev_hash"] != prev_hash or event["event_hash"] != expected_hash:
                return {
                    "valid": False,
                    "checked": index,
                    "broken_event_id": event["id"],
                    "broken_event_code": event["event_code"],
                }
            prev_hash = event["event_hash"]
        return {"valid": True, "checked": len(events), "broken_event_id": None, "broken_event_code": None}

    # ---------- 查询辅助 ----------

    def _require_lot(self, lot_id: int) -> sqlite3.Row:
        lot = self.connection.execute("SELECT * FROM food_lots WHERE id=?", (lot_id,)).fetchone()
        if lot is None:
            raise KeyError(str(lot_id))
        return lot

    def _lot_view(self, lot: sqlite3.Row) -> dict[str, Any]:
        data = dict(lot)
        data["balance_kg"] = self._balance(self.connection, lot["id"])
        data["current_node"] = self._current_node(self.connection, lot["id"])
        return data

    def _event_view(self, connection: sqlite3.Connection, event: sqlite3.Row) -> dict[str, Any]:
        inputs = [
            dict(row)
            for row in connection.execute(
                "SELECT i.lot_id, l.lot_code, l.product_name, i.quantity_kg FROM trace_event_inputs i JOIN food_lots l ON l.id=i.lot_id WHERE i.event_id=? ORDER BY i.lot_id",
                (event["id"],),
            ).fetchall()
        ]
        outputs = [
            dict(row)
            for row in connection.execute(
                "SELECT o.lot_id, l.lot_code, l.product_name, o.quantity_kg FROM trace_event_outputs o JOIN food_lots l ON l.id=o.lot_id WHERE o.event_id=? ORDER BY o.lot_id",
                (event["id"],),
            ).fetchall()
        ]
        credentials = [
            dict(row)
            for row in connection.execute(
                "SELECT credential_type,credential_no,issuer,issued_at FROM trace_event_credentials WHERE event_id=? ORDER BY credential_type,credential_no",
                (event["id"],),
            ).fetchall()
        ]
        revoke = connection.execute(
            "SELECT id,event_code,occurred_at,reason,actor FROM trace_events WHERE event_type='revoke' AND target_event_id=? ORDER BY id LIMIT 1",
            (event["id"],),
        ).fetchone()
        target_code = None
        if event["target_event_id"] is not None:
            target = connection.execute("SELECT event_code FROM trace_events WHERE id=?", (event["target_event_id"],)).fetchone()
            target_code = target["event_code"] if target else None
        return {
            "id": event["id"],
            "event_code": event["event_code"],
            "event_type": event["event_type"],
            "occurred_at": event["occurred_at"],
            "actor": event["actor"],
            "from_node": event["from_node"],
            "to_node": event["to_node"],
            "reason": event["reason"],
            "target_event_id": event["target_event_id"],
            "target_event_code": target_code,
            "inputs": inputs,
            "outputs": outputs,
            "credentials": credentials,
            "revoked": revoke is not None,
            "revoked_by": dict(revoke) if revoke else None,
            "prev_hash": event["prev_hash"],
            "content_hash": event["content_hash"],
            "event_hash": event["event_hash"],
            "created_at": event["created_at"],
        }

    def _traverse(self, start_lot_id: int, direction: str) -> tuple[dict[int, sqlite3.Row], dict[int, dict[str, Any]]]:
        visited_lots: dict[int, sqlite3.Row] = {}
        visited_events: dict[int, dict[str, Any]] = {}
        queue = [start_lot_id]
        while queue:
            lot_id = queue.pop()
            if lot_id in visited_lots:
                continue
            lot = self.connection.execute("SELECT * FROM food_lots WHERE id=?", (lot_id,)).fetchone()
            if lot is None:
                continue
            visited_lots[lot_id] = lot
            if direction == "upstream":
                rows = self.connection.execute(
                    "SELECT e.* FROM trace_events e JOIN trace_event_outputs o ON o.event_id=e.id WHERE o.lot_id=? ORDER BY e.occurred_at,e.id",
                    (lot_id,),
                ).fetchall()
            else:
                rows = self.connection.execute(
                    "SELECT e.* FROM trace_events e JOIN trace_event_inputs i ON i.event_id=e.id WHERE i.lot_id=? ORDER BY e.occurred_at,e.id",
                    (lot_id,),
                ).fetchall()
            for event in rows:
                if event["id"] not in visited_events:
                    visited_events[event["id"]] = self._event_view(self.connection, event)
                neighbors = visited_events[event["id"]]["inputs" if direction == "upstream" else "outputs"]
                for item in neighbors:
                    if item["lot_id"] not in visited_lots:
                        queue.append(item["lot_id"])
        return visited_lots, visited_events

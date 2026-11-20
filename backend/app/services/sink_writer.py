"""数据落盘业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "sink_writer"
LIST_FIELDS = ["槽位编号", "输出类型", "目标地址", "写入模式", "批次大小", "刷新间隔", "压缩算法", "分区策略"]
REQUIRED_FIELDS = ["槽位编号", "输出类型", "目标地址"]
STATUS_ORDER = ["待配置", "写入中", "已暂停", "写入失败"]
ACTION_RULES = {"启动写入": "写入中", "暂停写入": "已暂停", "标记失败": "写入失败"}
NEGATIVE_ACTIONS = []

# 指标落盘提交：数据行与提交回执同事务落库的存放表，以及槽位接受写入的状态。
SINK_DATA_TABLE = "_metric_sink_data"
ACCEPTING_STATUS = "写入中"


def slot_receipt_id(batch_id: str, slot_id: str, metric_id: str) -> str:
    """槽位提交回执编号：同一批次内（槽位, 指标）唯一，补写重复到达时据此幂等。"""
    return f"RCPT-{batch_id}-{slot_id}-{metric_id}"


class SinkWriterService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("槽位编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"输出槽位 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据落盘可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"输出槽位已{action}"

    # ------------------------------------------------------------------
    # 指标落盘提交回执：供管道编排按槽位确认写入结果
    # ------------------------------------------------------------------
    def find_slot_by_code(self, slot_code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if row.get("槽位编号") == slot_code:
                return row
        return None

    def find_slot_receipt(self, batch_id: str, slot_id: str, metric_id: str) -> dict[str, Any] | None:
        """按回执编号查已确认的槽位提交；存在即代表该值已随同事务落库。"""
        rid = slot_receipt_id(batch_id, slot_id, metric_id)
        for row in store.rows(SINK_DATA_TABLE):
            if row.get("receipt_id") == rid:
                return row
        return None

    def list_slot_receipts(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(SINK_DATA_TABLE)
        if batch_id is not None:
            rows = [row for row in rows if row.get("batch_id") == batch_id]
        return [dict(row) for row in rows]

    def commit_slot_value(
        self,
        *,
        batch_id: str,
        slot_id: str,
        metric_id: str,
        value: Any,
        force_fail: bool = False,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """向目标槽位提交一个指标值，返回（回执, 失败原因）。

        数据行与提交回执是同一行、一次落库，二者事务一致：
        提交失败时数据与回执都不落库；已存在回执时直接返回历史回执，
        补写请求重复到达也不会产生重复数据。
        """
        existing = self.find_slot_receipt(batch_id, slot_id, metric_id)
        if existing is not None:
            return existing, None
        slot = self.find_slot_by_code(slot_id)
        if slot is None:
            return None, f"目标槽位 {slot_id} 不存在或已归档"
        if slot.get("status") != ACCEPTING_STATUS:
            return None, f"目标槽位 {slot_id} 状态「{slot.get('status')}」不接受写入"
        if force_fail:
            return None, f"目标槽位 {slot_id} 提交超时（模拟目标故障）"
        receipt = {
            "receipt_id": slot_receipt_id(batch_id, slot_id, metric_id),
            "batch_id": batch_id,
            "slot_id": slot_id,
            "指标编号": metric_id,
            "值": value,
            "committed_at": datetime.now().isoformat(),
        }
        store.rows(SINK_DATA_TABLE).append(receipt)
        return receipt, None

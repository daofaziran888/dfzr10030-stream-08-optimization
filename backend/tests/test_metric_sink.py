"""指标落盘批次确认：三项指标中间一项失败、补写请求重复到达的验证。

场景：一批三项指标分别落向 SINK-0001/0002/0003，中间一项（METR-GMV →
SINK-0002）目标失败时批次停在「未齐」；补写只处理未确认槽位，已确认
数据不重复，未写项在恢复前不会显示成功。
"""
from app.services.pipeline_orchestrator import (
    run_metric_sink_pipeline, resume_metric_sink_batch, list_metric_sink_batches,
)
from app.services.sink_writer import SINK_DATA_TABLE
from app.store import store

THREE = ["METR-ORDER-COUNT", "METR-GMV", "METR-AVG-ORDER"]
# 槽位按编号顺序分配：ORDER-COUNT→SINK-0001，GMV→SINK-0002，AVG-ORDER→SINK-0003
MIDDLE_SLOT = "SINK-0002"


def _activate_all_slots() -> None:
    """让三个槽位都进入「写入中」，失败只来自模拟目标故障。"""
    for row in store.rows("sink_writer"):
        row["status"] = "写入中"


def _batch_receipts(batch_id: str) -> list[dict]:
    return [row for row in store.rows(SINK_DATA_TABLE) if row["batch_id"] == batch_id]


def _by_metric(result: dict) -> dict[str, dict]:
    return {s["指标编号"]: s for s in result["sinks"]}


class TestPartialFailure:
    def test_middle_slot_failure_keeps_batch_incomplete(self):
        _activate_all_slots()
        r = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        assert r["执行状态"] == "未齐"
        view = _by_metric(r)
        assert view["METR-ORDER-COUNT"]["写入状态"] == "成功"
        assert view["METR-GMV"]["写入状态"] == "失败"  # 中间一项
        assert view["METR-AVG-ORDER"]["写入状态"] == "成功"
        assert r["待补写槽位"] == [MIDDLE_SLOT]

    def test_failed_slot_has_no_receipt_and_no_reading(self):
        _activate_all_slots()
        r = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        view = _by_metric(r)
        # 未写项不显示成功，也不给出读数与写入时间
        assert view["METR-GMV"]["值"] is None
        assert view["METR-GMV"]["写入时间"] is None
        receipts = _batch_receipts(r["batch_id"])
        assert len(receipts) == 2
        assert all(row["指标编号"] != "METR-GMV" for row in receipts)

    def test_manifest_records_slot_results(self):
        _activate_all_slots()
        r = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        manifest = next(b for b in list_metric_sink_batches() if b["batch_id"] == r["batch_id"])
        slots = {s["指标编号"]: s for s in manifest["slots"]}
        assert slots["METR-ORDER-COUNT"]["status"] == "已确认"
        assert slots["METR-ORDER-COUNT"]["receipt_id"]
        assert slots["METR-GMV"]["status"] == "写入失败"
        assert slots["METR-GMV"]["receipt_id"] is None
        assert slots["METR-GMV"]["失败原因"]
        assert manifest["执行状态"] == "未齐"


class TestResume:
    def test_resume_only_rewrites_unconfirmed_slots(self):
        _activate_all_slots()
        r1 = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        r2 = resume_metric_sink_batch(r1["batch_id"])
        assert r2["执行状态"] == "完成"
        # 本轮只切换了中间槽位，其余沿用历史批次读数
        assert r2["本轮切换槽位"] == [MIDDLE_SLOT]
        assert sorted(r2["沿用历史槽位"]) == ["SINK-0001", "SINK-0003"]
        first, second = _by_metric(r1), _by_metric(r2)
        assert second["METR-ORDER-COUNT"]["来源"] == "历史回执"
        assert second["METR-ORDER-COUNT"]["写入时间"] == first["METR-ORDER-COUNT"]["写入时间"]
        assert second["METR-GMV"]["来源"] == "本轮写入"
        assert second["METR-GMV"]["写入状态"] == "成功"
        # 已确认数据没有重复：全批次仍是三条回执
        receipts = _batch_receipts(r1["batch_id"])
        assert len(receipts) == 3
        assert len({row["receipt_id"] for row in receipts}) == 3

    def test_duplicate_resume_is_idempotent(self):
        _activate_all_slots()
        r1 = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        resume_metric_sink_batch(r1["batch_id"])
        r3 = resume_metric_sink_batch(r1["batch_id"])  # 补写请求重复到达
        assert r3["执行状态"] == "完成"
        assert r3["本轮切换槽位"] == []
        assert sorted(r3["沿用历史槽位"]) == ["SINK-0001", "SINK-0002", "SINK-0003"]
        assert len(_batch_receipts(r1["batch_id"])) == 3  # 已确认数据没有重复

    def test_failed_resume_does_not_mark_success(self):
        _activate_all_slots()
        r1 = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        r2 = resume_metric_sink_batch(r1["batch_id"], fail_slots={MIDDLE_SLOT})
        assert r2["执行状态"] == "未齐"
        view = _by_metric(r2)
        assert view["METR-GMV"]["写入状态"] == "失败"
        assert view["METR-GMV"]["值"] is None
        manifest = next(b for b in list_metric_sink_batches() if b["batch_id"] == r1["batch_id"])
        slot = next(s for s in manifest["slots"] if s["指标编号"] == "METR-GMV")
        assert slot["attempts"] == 2
        assert slot["status"] == "写入失败"

    def test_auto_trigger_resumes_same_batch(self):
        _activate_all_slots()
        r1 = run_metric_sink_pipeline(metric_ids=THREE, fail_slots={MIDDLE_SLOT})
        r2 = run_metric_sink_pipeline(metric_ids=THREE)  # 再次触发 = 接着补写
        assert r2["batch_id"] == r1["batch_id"]
        assert r2["执行状态"] == "完成"
        assert r2["本轮切换槽位"] == [MIDDLE_SLOT]

    def test_new_batch_only_after_completion(self):
        _activate_all_slots()
        r1 = run_metric_sink_pipeline(metric_ids=THREE)
        assert r1["执行状态"] == "完成"
        r2 = run_metric_sink_pipeline(metric_ids=THREE)
        assert r2["batch_id"] != r1["batch_id"]
        assert r2["执行状态"] == "完成"


class TestSlotGating:
    def test_only_accepting_slots_commit(self):
        # 种子数据：SINK-0001 待配置、SINK-0002 写入中、SINK-0003 已暂停
        r = run_metric_sink_pipeline()
        assert r["执行状态"] == "未齐"
        confirmed = [s for s in r["sinks"] if s["写入状态"] == "成功"]
        assert [s["指标编号"] for s in confirmed] == ["METR-GMV"]

    def test_resume_after_slot_activated(self):
        r1 = run_metric_sink_pipeline()
        assert r1["执行状态"] == "未齐"
        _activate_all_slots()
        r2 = resume_metric_sink_batch(r1["batch_id"])
        assert r2["执行状态"] == "完成"
        assert sorted(r2["本轮切换槽位"]) == ["SINK-0001", "SINK-0003"]
        assert r2["沿用历史槽位"] == ["SINK-0002"]

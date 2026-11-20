"""跨模块管道接口：触发执行并查询结果。"""
from __future__ import annotations

from fastapi import APIRouter, Body

from app.services.pipeline_orchestrator import (
    run_all_pipelines, list_pipeline_results,
    run_source_cdc_pipeline, run_window_metric_pipeline,
    run_rule_router_pipeline, run_metric_sink_pipeline,
    resume_metric_sink_batch, list_metric_sink_batches,
    run_quality_scan, run_partition_backpressure_pipeline,
    run_checkpoint_state_pipeline,
)

router = APIRouter(prefix="/api/pipeline", tags=["管道编排"])


@router.post("/run-all")
def run_all() -> dict:
    return {"ok": True, "results": run_all_pipelines()}


@router.get("/results")
def list_results() -> dict:
    return {"ok": True, "results": list_pipeline_results()}


@router.post("/source-cdc")
def trigger_source_cdc() -> dict:
    return {"ok": True, "result": run_source_cdc_pipeline()}


@router.post("/window-metric")
def trigger_window_metric() -> dict:
    return {"ok": True, "result": run_window_metric_pipeline()}


@router.post("/rule-router")
def trigger_rule_router() -> dict:
    return {"ok": True, "result": run_rule_router_pipeline()}


@router.post("/metric-sink")
def trigger_metric_sink(payload: dict | None = Body(default=None)) -> dict:
    """执行指标落盘：存在未齐批次时自动接着补写未确认槽位。"""
    payload = payload or {}
    try:
        result = run_metric_sink_pipeline(
            metric_ids=payload.get("metric_ids"),
            fail_slots=set(payload.get("fail_slots") or []),
        )
    except ValueError as exc:
        return {"ok": False, "message": str(exc)}
    return {"ok": True, "result": result}


@router.post("/metric-sink/{batch_id}/resume")
def resume_metric_sink(batch_id: str, payload: dict | None = Body(default=None)) -> dict:
    """补写指定批次：按目标槽位提交回执决定补写范围，重复请求幂等。"""
    payload = payload or {}
    result = resume_metric_sink_batch(batch_id, fail_slots=set(payload.get("fail_slots") or []))
    if result is None:
        return {"ok": False, "message": f"指标落盘批次 {batch_id} 不存在"}
    return {"ok": True, "result": result}


@router.get("/metric-sink/batches")
def list_metric_sink_manifests() -> dict:
    """指标落盘执行清单：各批次槽位回执的齐备进度。"""
    return {"ok": True, "batches": list_metric_sink_batches()}


@router.post("/quality-scan")
def trigger_quality_scan() -> dict:
    return {"ok": True, "result": run_quality_scan()}


@router.post("/partition-bp")
def trigger_partition_bp() -> dict:
    return {"ok": True, "result": run_partition_backpressure_pipeline()}


@router.post("/checkpoint-state")
def trigger_checkpoint_state() -> dict:
    return {"ok": True, "result": run_checkpoint_state_pipeline()}
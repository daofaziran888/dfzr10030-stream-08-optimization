"""流式管道编排器：实现 7 条跨模块数据管道。

每条管道都读取上游 fixtures 或 store 数据，执行真实流式计算，
把结果写入 store._pipeline_results 供下游消费。
"""
from __future__ import annotations

import math, statistics
from datetime import datetime
from typing import Any

from app.fixtures import (
    SOURCE_EVENTS, TRANSFORM_RECORDS, WINDOW_CONFIGS,
    RULE_TRIGGERS, METRIC_DEFS, QUALITY_SAMPLES,
    PARTITION_STATS, EVENT_ROUTES,
)
from app.services.sink_writer import SinkWriterService
from app.store import store

_sink_service = SinkWriterService()


def _to_float(v: Any) -> float:
    try: return float(v)
    except (TypeError, ValueError): return 0.0


def _wr(pid: str, payload: dict) -> None:
    t = store.rows("_pipeline_results")
    for r in t:
        if r.get("pipeline_id") == pid:
            r.update(payload); r["updated_at"] = datetime.now().isoformat(); return
    e = {"pipeline_id": pid, "created_at": datetime.now().isoformat()}
    e.update(payload); t.append(e)


def _rr(pid: str) -> dict | None:
    for r in store.rows("_pipeline_results"):
        if r.get("pipeline_id") == pid: return dict(r)
    return None


# Pipeline 1: source_binding -> cdc_task -> stream_transform
def run_source_cdc_pipeline() -> dict:
    """模拟源端事件流：统计各源端的事件类型分布和吞吐量。"""
    events = list(SOURCE_EVENTS)
    src_stats: dict[str, dict] = {}
    for e in events:
        src = e["源端编号"]
        if src not in src_stats:
            src_stats[src] = {"total": 0, "INSERT": 0, "UPDATE": 0, "DELETE": 0, "总行数": 0}
        src_stats[src]["total"] += 1
        src_stats[src][e["事件类型"]] += 1
        src_stats[src]["总行数"] += e["数据行数"]

    result = {
        "source_count": len(src_stats),
        "total_events": len(events),
        "total_rows": sum(s["总行数"] for s in src_stats.values()),
        "sources": {k: dict(v) for k, v in src_stats.items()},
    }
    _wr("source_cdc_transform", {"source": ["source_binding", "cdc_task"], "target": "stream_transform", "payload": result})
    return result


# Pipeline 2: stream_transform -> window_agg -> metric_calc
def run_window_metric_pipeline(window_id: str | None = None) -> dict:
    """执行窗口聚合并计算指标。"""
    records = list(TRANSFORM_RECORDS)
    configs = WINDOW_CONFIGS
    if window_id:
        configs = [c for c in configs if c["窗口编号"] == window_id]

    results: dict[str, Any] = {"total_records": len(records)}
    for cfg in configs:
        gmv = sum(_to_float(r["金额"]) for r in records)
        cnt = len(records)
        users = set(r["用户ID"] for r in records)
        channel_dist: dict[str, int] = {}
        for r in records:
            channel_dist[r["渠道"]] = channel_dist.get(r["渠道"], 0) + 1

        results[cfg["窗口编号"]] = {
            "窗口类型": cfg["窗口类型"], "记录数": cnt,
            "gmv": round(gmv, 2), "独立用户": len(users),
            "平均订单": round(gmv / cnt, 2) if cnt else 0.0,
            "渠道分布": channel_dist,
        }

    # 聚合指标
    metrics = {}
    for m in METRIC_DEFS:
        mid = m["指标编号"]
        if "count" in m["计算公式"]:
            metrics[mid] = len(records)
        elif "sum" in m["计算公式"]:
            metrics[mid] = round(sum(_to_float(r["金额"]) for r in records), 2)
        elif "avg" in m["计算公式"]:
            vals = [_to_float(r["金额"]) for r in records]
            metrics[mid] = round(statistics.mean(vals), 2) if vals else 0.0
        elif "distinct" in m["计算公式"]:
            metrics[mid] = len(set(r["用户ID"] for r in records))

    results["metrics"] = metrics
    _wr("window_metric_calc", {"source": ["stream_transform", "window_agg"], "target": "metric_calc", "payload": results})
    return results


# Pipeline 3: rule_engine -> event_router -> pipeline_orch
def run_rule_router_pipeline() -> dict:
    """规则引擎触发后的事件路由。"""
    triggers = list(RULE_TRIGGERS)
    routes = {r["规则编号"]: r.get("目标管道", "default") for r in EVENT_ROUTES}
    routing: dict[str, list] = {}
    total_hits = 0
    for t in triggers:
        rid = t["规则编号"]
        route = routes.get(rid, "default")
        if route not in routing:
            routing[route] = []
        hits = len(t.get("命中用户", []))
        total_hits += hits
        routing[route].append({"规则": rid, "动作": t["动作"], "命中": hits})

    result = {"rules_fired": len(triggers), "total_hits": total_hits, "routing": routing}
    _wr("rule_router_orch", {"source": ["rule_engine", "event_router"], "target": "pipeline_orch", "payload": result})
    return result


# Pipeline 4: metric_calc -> pipeline_orch -> sink_writer
#
# 指标落盘按批次执行：批次内每个落盘值都有对应目标槽位的提交回执，
# 槽位写入结果回写到执行清单（_metric_sink_manifest）。批次执行状态
# 只在全部槽位回执齐备后从「未齐」推进到「完成」；写入中断后只能
# 接着处理未确认槽位，已确认槽位沿用历史批次读数，绝不重复提交。
MANIFEST_TABLE = "_metric_sink_manifest"
BATCH_INCOMPLETE = "未齐"
BATCH_COMPLETE = "完成"


def _assigned_slot_codes(metric_ids: list[str]) -> list[str]:
    """按槽位编号顺序为每个指标分配目标槽位（槽位不足时循环复用）。"""
    rows = sorted(store.rows("sink_writer"), key=lambda r: int(r.get("id", 0)))
    codes = [str(r.get("槽位编号")) for r in rows]
    if not codes:
        raise ValueError("数据落盘模块没有可用的输出槽位")
    return [codes[i % len(codes)] for i in range(len(metric_ids))]


def _open_sink_batch(metric_ids: list[str]) -> dict:
    manifests = store.rows(MANIFEST_TABLE)
    seq = max((int(m.get("batch_seq", 0)) for m in manifests), default=0) + 1
    manifest = {
        "batch_id": f"MSB-{seq:04d}",
        "batch_seq": seq,
        "执行状态": BATCH_INCOMPLETE,
        "slots": [
            {"槽位编号": code, "指标编号": mid, "status": "待写入",
             "receipt_id": None, "失败原因": None, "committed_at": None,
             "attempts": 0, "最近尝试值": None}
            for mid, code in zip(metric_ids, _assigned_slot_codes(metric_ids))
        ],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
    }
    manifests.append(manifest)
    return manifest


def _find_manifest(batch_id: str) -> dict | None:
    for m in store.rows(MANIFEST_TABLE):
        if m.get("batch_id") == batch_id:
            return m
    return None


def _latest_incomplete_manifest(metric_key: set[str]) -> dict | None:
    for m in reversed(store.rows(MANIFEST_TABLE)):
        if m.get("执行状态") == BATCH_INCOMPLETE and {s["指标编号"] for s in m["slots"]} == metric_key:
            return m
    return None


def _execute_sink_batch(manifest: dict, metrics: dict, fail_slots: set[str]) -> dict:
    """按执行清单提交未确认槽位，并把每个槽位的写入结果回写清单。

    已确认槽位直接沿用历史回执读数，不再触碰目标槽位；未确认槽位
    逐个补写，成功即登记回执，失败则记录原因等待下一轮补写。
    """
    newly: list[str] = []
    carried: list[str] = []
    sinks: list[dict] = []
    for slot in manifest["slots"]:
        mid, code = slot["指标编号"], slot["槽位编号"]
        if slot["status"] == "已确认":
            receipt = _sink_service.find_slot_receipt(manifest["batch_id"], code, mid)
            sinks.append({"指标编号": mid, "槽位编号": code, "值": receipt["值"],
                          "写入时间": receipt["committed_at"], "写入状态": "成功",
                          "来源": "历史回执", "失败原因": None})
            carried.append(code)
            continue
        value = metrics.get(mid)
        slot["attempts"] += 1
        slot["最近尝试值"] = value
        receipt, err = _sink_service.commit_slot_value(
            batch_id=manifest["batch_id"], slot_id=code, metric_id=mid,
            value=value, force_fail=code in fail_slots)
        if receipt is not None:
            slot.update(status="已确认", receipt_id=receipt["receipt_id"],
                        committed_at=receipt["committed_at"], 失败原因=None)
            sinks.append({"指标编号": mid, "槽位编号": code, "值": receipt["值"],
                          "写入时间": receipt["committed_at"], "写入状态": "成功",
                          "来源": "本轮写入", "失败原因": None})
            newly.append(code)
        else:
            slot.update(status="写入失败", 失败原因=err)
            sinks.append({"指标编号": mid, "槽位编号": code, "值": None,
                          "写入时间": None, "写入状态": "失败",
                          "来源": None, "失败原因": err})
    if all(s["status"] == "已确认" for s in manifest["slots"]):
        manifest["执行状态"] = BATCH_COMPLETE
    manifest["updated_at"] = datetime.now().isoformat()
    return {
        "batch_id": manifest["batch_id"],
        "batch_seq": manifest["batch_seq"],
        "执行状态": manifest["执行状态"],
        "sink_count": len(sinks),
        "confirmed_count": sum(1 for s in sinks if s["写入状态"] == "成功"),
        "pending_count": sum(1 for s in sinks if s["写入状态"] != "成功"),
        "sinks": sinks,
        "本轮切换槽位": list(dict.fromkeys(newly)),
        "沿用历史槽位": list(dict.fromkeys(carried)),
        "待补写槽位": list(dict.fromkeys(
            s["槽位编号"] for s in manifest["slots"] if s["status"] != "已确认")),
    }


def _known_metric_ids() -> list[str]:
    return [m["指标编号"] for m in METRIC_DEFS]


def run_metric_sink_pipeline(
    metric_ids: list[str] | None = None,
    fail_slots: set[str] | None = None,
) -> dict:
    """指标结果写入下游槽位：批次内每个落盘值都有目标槽位提交回执。

    存在同口径的未齐批次时接着处理未确认槽位（补写），
    上一批次回执齐备后才开启新批次。
    """
    metrics = run_window_metric_pipeline().get("metrics", {})
    known = _known_metric_ids()
    wanted = [mid for mid in (metric_ids or known) if mid in known]
    if not wanted:
        raise ValueError("没有可落盘的指标")
    manifest = _latest_incomplete_manifest(set(wanted))
    if manifest is None:
        manifest = _open_sink_batch(wanted)
    result = _execute_sink_batch(manifest, metrics, fail_slots or set())
    _wr("metric_pipeline_sink", {"source": ["metric_calc", "pipeline_orch"], "target": "sink_writer", "payload": result})
    return result


def resume_metric_sink_batch(batch_id: str, fail_slots: set[str] | None = None) -> dict | None:
    """补写指定批次：只处理未确认槽位；重复到达的补写请求幂等，已确认数据不重复。"""
    manifest = _find_manifest(batch_id)
    if manifest is None:
        return None
    metrics = run_window_metric_pipeline().get("metrics", {})
    result = _execute_sink_batch(manifest, metrics, fail_slots or set())
    _wr("metric_pipeline_sink", {"source": ["metric_calc", "pipeline_orch"], "target": "sink_writer", "payload": result})
    return result


def list_metric_sink_batches() -> list[dict]:
    """执行清单：各批次的槽位确认进度，供排查与补写决策。"""
    return [
        {**m, "slots": [dict(s) for s in m["slots"]]}
        for m in store.rows(MANIFEST_TABLE)
    ]


# Pipeline 5: data_quality -> pipeline_orch
def run_quality_scan() -> dict:
    """质量扫描。"""
    samples = list(QUALITY_SAMPLES)
    anomalies = []
    score = 100.0
    for s in samples:
        cv = _to_float(s["当前值"])
        lo = _to_float(s["阈值下限"])
        hi = _to_float(s["阈值上限"])
        ok = lo <= cv <= hi
        if not ok:
            anomalies.append({"规则": s["规则编号"], "对象": s["监控对象"], "当前值": cv, "阈值": f"[{lo}, {hi}]"})
            score -= 25.0

    # 扫描 store 模块 abnormal 标记
    mod_issues = []
    for mod in store.module_names():
        if mod.startswith("_"): continue
        rs = store.rows(mod)
        ab = sum(1 for r in rs if r.get("abnormal"))
        if ab:
            mod_issues.append({"module": mod, "abnormal": ab})

    result = {"quality_score": max(0, score), "anomalies": anomalies, "module_issues": mod_issues}
    _wr("quality_scan", {"source": ["data_quality"], "target": "_global", "payload": result})
    return result


# Pipeline 6: grp_partition -> backpressure
def run_partition_backpressure_pipeline() -> dict:
    """分区统计与背压评估。"""
    stats = list(PARTITION_STATS)
    bp_alerts = []
    for s in stats:
        vals = [_to_float(s[f"分区{i}行数"]) for i in range(int(s["分区数量"]))]
        if vals:
            mx = max(vals)
            mn = min(vals)
            skew = (mx - mn) / statistics.mean(vals) if statistics.mean(vals) > 0 else 0.0
            if skew > 1.0 or s.get("再平衡建议"):
                bp_alerts.append({"分区键": s["分区键"], "歪斜度": round(skew, 2), "热点": s["热点分区"], "建议": "触发再平衡"})

    result = {"partition_count": len(stats), "backpressure_alerts": bp_alerts, "alert_count": len(bp_alerts)}
    _wr("partition_backpressure", {"source": ["grp_partition"], "target": "backpressure", "payload": result})
    return result


# Pipeline 7: checkpoint -> state_store -> schema_evolve
def run_checkpoint_state_pipeline() -> dict:
    """检查点与状态一致性检查。"""
    ckpts = [r for r in store.rows("checkpoint")]
    states = [r for r in store.rows("state_store")]
    schemas = [r for r in store.rows("schema_evolve")]

    ckpt_available = sum(1 for c in ckpts if c["status"] == "可用")
    state_total = len(states)
    schema_pending = sum(1 for s in schemas if s["status"] == "待执行")

    consistency = {
        "checkpoint_available": ckpt_available,
        "checkpoint_total": len(ckpts),
        "state_count": state_total,
        "schema_pending": schema_pending,
        "health": "healthy" if ckpt_available >= 1 and schema_pending == 0 else "degraded",
    }
    result = consistency
    _wr("checkpoint_state", {"source": ["checkpoint", "state_store"], "target": "schema_evolve", "payload": result})
    return result


def run_all_pipelines() -> dict:
    return {
        "source_cdc": run_source_cdc_pipeline(),
        "window_metric": run_window_metric_pipeline(),
        "rule_router": run_rule_router_pipeline(),
        "metric_sink": run_metric_sink_pipeline(),
        "quality_scan": run_quality_scan(),
        "partition_bp": run_partition_backpressure_pipeline(),
        "checkpoint_state": run_checkpoint_state_pipeline(),
    }


def list_pipeline_results() -> list[dict]:
    return [dict(r) for r in store.rows("_pipeline_results")]
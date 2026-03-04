from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.selector import ZXBrickTurnSelector
from scripts.backtest_selectors import build_calendar, calc_metrics, create_fixed_trade, load_config, load_data


LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "grid_search_zxbrick_v3.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("grid_search_v3")


def parse_float_list(raw: str) -> List[float]:
    values: List[float] = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        values.append(float(text))
    uniq = sorted(set(values))
    if not uniq:
        raise ValueError("grid-values 不能为空")
    return uniq


def build_weight_grid(values: List[float], sum_tol: float = 1e-9) -> List[Dict[str, float]]:
    grid: List[Dict[str, float]] = []
    for brick_w, vol_w, money_w, ind_w in product(values, repeat=4):
        total = brick_w + vol_w + money_w + ind_w
        if abs(total - 1.0) > sum_tol:
            continue
        grid.append(
            {
                "brick_strength": float(brick_w),
                "volume_ratio": float(vol_w),
                "moneyflow_strength": float(money_w),
                "industry_strength": float(ind_w),
            }
        )
    grid.sort(
        key=lambda w: (
            w["brick_strength"],
            w["volume_ratio"],
            w["moneyflow_strength"],
            w["industry_strength"],
        )
    )
    return grid


def resolve_selector_params(cfg_path: Path, selector_alias: str) -> Dict[str, Any]:
    cfgs = load_config(cfg_path)
    for cfg in cfgs:
        if cfg.get("activate", True) is False:
            continue
        alias = str(cfg.get("alias", cfg.get("class", "")))
        if alias == selector_alias:
            params = dict(cfg.get("params", {}))
            params.setdefault("use_score_model", True)
            return params
    raise ValueError(f"配置中未找到 selector alias: {selector_alias}")


def safe_trade_payload(
    *,
    code: str,
    signal_date: pd.Timestamp,
    df: pd.DataFrame,
    hold_days: int,
    entry_price: str,
    end_date: pd.Timestamp,
) -> Dict[str, Any]:
    trade = create_fixed_trade(
        code=code,
        signal_date=signal_date,
        df=df,
        hold_days=hold_days,
        entry_price=entry_price,
        end_date=end_date,
    )
    if trade is None:
        return {
            "trade_valid": False,
            "trade_return": float("nan"),
            "daily_returns": tuple(),
        }
    return {
        "trade_valid": True,
        "trade_return": float(trade.trade_return),
        "daily_returns": tuple((pd.Timestamp(d), float(r)) for d, r in trade.daily_returns),
    }


def extract_candidate_cache(
    *,
    selector: ZXBrickTurnSelector,
    data: Dict[str, pd.DataFrame],
    calendar: List[pd.Timestamp],
    hold_days: int,
    entry_price: str,
    end_date: pd.Timestamp,
) -> Tuple[
    Dict[pd.Timestamp, List[Dict[str, Any]]],
    Counter,
    Counter,
    Counter,
    List[Dict[str, Any]],
]:
    day_candidates: Dict[pd.Timestamp, List[Dict[str, Any]]] = {}
    hard_rule_counter: Counter = Counter()
    hard_reason_counter: Counter = Counter()
    hard_code_counter: Counter = Counter()
    daily_summary_rows: List[Dict[str, Any]] = []

    for idx, signal_date in enumerate(calendar, start=1):
        result = selector.select_with_audit(signal_date, data, audit_level="failed_only")
        summary = dict(result.get("audit_summary", {}))
        daily_summary_rows.append(
            {
                "signal_date": signal_date.date().isoformat(),
                "core_pass_count": int(summary.get("core_pass_count", 0)),
                "hard_pass_count": int(summary.get("hard_pass_count", 0)),
                "selected_count_at_base_weight": int(summary.get("selected_count", 0)),
                "cap_reject_count_at_base_weight": int(summary.get("cap_reject_count", 0)),
            }
        )

        for row in list(result.get("audit_rows", [])):
            if str(row.get("stage")) != "hard_filter":
                continue
            if bool(row.get("passed", False)):
                continue
            key = (
                str(row.get("rule_code", "")),
                str(row.get("rule_name", "")),
                str(row.get("reason", "")),
                str(row.get("threshold_expr", "")),
            )
            hard_rule_counter[key] += 1
            hard_reason_counter[str(row.get("reason", ""))] += 1
            hard_code_counter[str(row.get("code", ""))] += 1

        rows: List[Dict[str, Any]] = []
        for item in list(result.get("picks_scored", [])):
            code = str(item.get("code", ""))
            if not code:
                continue
            factor_scores = dict(item.get("factor_scores", {}))
            trade_payload = safe_trade_payload(
                code=code,
                signal_date=signal_date,
                df=data[code],
                hold_days=hold_days,
                entry_price=entry_price,
                end_date=end_date,
            )
            rows.append(
                {
                    "code": code,
                    "ts_code": str(item.get("ts_code", "")),
                    "brick_strength": float(item.get("brick_strength", float("nan"))),
                    "factor_brick": float(factor_scores.get("brick_strength", 0.5)),
                    "factor_volume": float(factor_scores.get("volume_ratio", 0.5)),
                    "factor_moneyflow": float(factor_scores.get("moneyflow_strength", 0.5)),
                    "factor_industry": float(factor_scores.get("industry_strength", 0.5)),
                    "volume_ratio": float(item.get("volume_ratio", float("nan"))),
                    "moneyflow_net_sum_for_score": float(item.get("moneyflow_net_sum_for_score", float("nan"))),
                    "industry_pct": float(item.get("industry_pct", float("nan"))),
                    "trade_valid": bool(trade_payload["trade_valid"]),
                    "trade_return": float(trade_payload["trade_return"]),
                    "daily_returns": trade_payload["daily_returns"],
                }
            )
        day_candidates[signal_date] = rows

        if idx % 20 == 0 or idx == len(calendar):
            logger.info("缓存候选进度: %d/%d 个交易日", idx, len(calendar))

    return day_candidates, hard_rule_counter, hard_reason_counter, hard_code_counter, daily_summary_rows


def evaluate_weights(
    *,
    weights: Dict[str, float],
    day_candidates: Dict[pd.Timestamp, List[Dict[str, Any]]],
    calendar: List[pd.Timestamp],
    daily_trade_cap: Optional[int],
    collect_cap_rows: bool = False,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    daily_buckets: Dict[pd.Timestamp, List[float]] = defaultdict(list)
    trade_returns: List[float] = []
    cap_rows: List[Dict[str, Any]] = []

    selected_per_day: List[int] = []
    cap_reject_per_day: List[int] = []
    cap_binding_days = 0

    for signal_date in calendar:
        candidates = list(day_candidates.get(signal_date, []))
        if not candidates:
            selected_per_day.append(0)
            cap_reject_per_day.append(0)
            continue

        scored: List[Dict[str, Any]] = []
        for c in candidates:
            score = (
                weights["brick_strength"] * float(c["factor_brick"])
                + weights["volume_ratio"] * float(c["factor_volume"])
                + weights["moneyflow_strength"] * float(c["factor_moneyflow"])
                + weights["industry_strength"] * float(c["factor_industry"])
            )
            row = dict(c)
            row["score"] = float(score)
            scored.append(row)

        scored.sort(
            key=lambda x: (
                -float(x.get("score", float("-inf"))),
                -float(x.get("brick_strength", float("-inf"))),
                str(x.get("code", "")),
            )
        )
        for rank, row in enumerate(scored, start=1):
            row["rank_before_cap"] = int(rank)

        if daily_trade_cap is None:
            selected = scored
            rejected = []
        else:
            selected = scored[:daily_trade_cap]
            rejected = scored[daily_trade_cap:]

        selected_per_day.append(len(selected))
        cap_reject_per_day.append(len(rejected))
        if rejected:
            cap_binding_days += 1

        if collect_cap_rows and rejected:
            for row in rejected:
                cap_rows.append(
                    {
                        "signal_date": signal_date.date().isoformat(),
                        "code": str(row["code"]),
                        "score": float(row["score"]),
                        "rank_before_cap": int(row["rank_before_cap"]),
                        "reject_reason": "DAILY_CAP",
                    }
                )

        for row in selected:
            if not bool(row.get("trade_valid", False)):
                continue
            trade_returns.append(float(row["trade_return"]))
            for d, ret in row["daily_returns"]:
                daily_buckets[pd.Timestamp(d)].append(float(ret))

    strategy_daily_returns: List[float] = []
    active_positions_daily: List[int] = []
    for d in calendar:
        rets = daily_buckets.get(d, [])
        active_positions_daily.append(len(rets))
        strategy_daily_returns.append(float(np.mean(rets)) if rets else 0.0)

    daily_ret_s = pd.Series(strategy_daily_returns, index=calendar, dtype=float)
    active_s = pd.Series(active_positions_daily, index=calendar, dtype=int)
    metrics = calc_metrics(daily_returns=daily_ret_s, active_positions=active_s, trade_returns=trade_returns)

    summary: Dict[str, Any] = {
        "weight_brick_strength": float(weights["brick_strength"]),
        "weight_volume_ratio": float(weights["volume_ratio"]),
        "weight_moneyflow_strength": float(weights["moneyflow_strength"]),
        "weight_industry_strength": float(weights["industry_strength"]),
        "signal_days": int(len(calendar)),
        "avg_selected_per_day": float(np.mean(selected_per_day)) if selected_per_day else 0.0,
        "avg_cap_reject_per_day": float(np.mean(cap_reject_per_day)) if cap_reject_per_day else 0.0,
        "cap_binding_ratio": float(cap_binding_days / len(calendar)) if calendar else 0.0,
    }
    summary.update(metrics)
    return summary, cap_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="ZXBrick v3 权重网格搜索 + 失败原因统计")
    parser.add_argument("--data-dir", default="./data", help="行情目录")
    parser.add_argument("--config", default="./configs/backtest_brick_compare_aux_top2.json", help="策略配置文件")
    parser.add_argument("--selector-alias", default="搬砖_增强v3_top2", help="作为基础参数模板的 selector alias")
    parser.add_argument("--start-date", required=True, help="起始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--tickers", default="all", help="all 或逗号分隔股票代码")
    parser.add_argument("--entry-price", choices=["open", "close"], default="open", help="入场价格")
    parser.add_argument("--hold-days", type=int, default=2, help="固定持有 K 线根数")
    parser.add_argument("--daily-trade-cap", type=int, default=2, help="每日最多交易笔数")
    parser.add_argument(
        "--grid-values",
        default="0.1,0.15,0.2,0.25,0.3,0.35,0.4",
        help="网格取值（逗号分隔，4个权重从该集合取值且和为1）",
    )
    parser.add_argument("--top-k", type=int, default=20, help="输出前 K 组权重")
    parser.add_argument("--audit-top-n", type=int, default=20, help="失败原因榜输出前 N 条")
    parser.add_argument("--out-dir", default="./out", help="输出目录")
    args = parser.parse_args()

    if args.hold_days < 1:
        raise ValueError("--hold-days 必须 >= 1")
    if args.daily_trade_cap is not None and args.daily_trade_cap < 1:
        raise ValueError("--daily-trade-cap 必须 >= 1")

    start_date = pd.to_datetime(args.start_date)
    end_date = pd.to_datetime(args.end_date)
    if end_date < start_date:
        raise ValueError("--end-date 必须 >= --start-date")

    values = parse_float_list(args.grid_values)
    grid = build_weight_grid(values)
    if not grid:
        raise ValueError("当前 grid-values 无法拼出和为 1 的四因子权重组合")

    logger.info("网格组合数: %d", len(grid))

    data_dir = Path(args.data_dir)
    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    data = load_data(data_dir, codes)
    if not data:
        raise ValueError("未加载到任何行情数据")

    calendar = build_calendar(data=data, start_date=start_date, end_date=end_date)
    if not calendar:
        raise ValueError("回测区间无可用交易日")

    params = resolve_selector_params(Path(args.config), args.selector_alias)
    params["use_score_model"] = True
    params["daily_trade_cap"] = args.daily_trade_cap
    params["top_n"] = None
    params["score_weights"] = {
        "brick_strength": 0.35,
        "volume_ratio": 0.25,
        "moneyflow_strength": 0.20,
        "industry_strength": 0.20,
    }
    selector = ZXBrickTurnSelector(**params)

    logger.info(
        "开始提取候选缓存: %s ~ %s, 交易日=%d, 股票数=%d",
        start_date.date(),
        end_date.date(),
        len(calendar),
        len(data),
    )
    day_candidates, hard_rule_counter, hard_reason_counter, hard_code_counter, daily_summary_rows = extract_candidate_cache(
        selector=selector,
        data=data,
        calendar=calendar,
        hold_days=args.hold_days,
        entry_price=args.entry_price,
        end_date=end_date,
    )

    rows: List[Dict[str, Any]] = []
    for idx, weights in enumerate(grid, start=1):
        summary, _ = evaluate_weights(
            weights=weights,
            day_candidates=day_candidates,
            calendar=calendar,
            daily_trade_cap=args.daily_trade_cap,
            collect_cap_rows=False,
        )
        rows.append(summary)
        if idx % 20 == 0 or idx == len(grid):
            logger.info("网格回测进度: %d/%d", idx, len(grid))

    result_df = pd.DataFrame(rows).sort_values(by="total_return", ascending=False, na_position="last")
    if result_df.empty:
        raise RuntimeError("网格回测结果为空")

    best_row = result_df.iloc[0].to_dict()
    best_weights = {
        "brick_strength": float(best_row["weight_brick_strength"]),
        "volume_ratio": float(best_row["weight_volume_ratio"]),
        "moneyflow_strength": float(best_row["weight_moneyflow_strength"]),
        "industry_strength": float(best_row["weight_industry_strength"]),
    }
    best_summary, cap_rows = evaluate_weights(
        weights=best_weights,
        day_candidates=day_candidates,
        calendar=calendar,
        daily_trade_cap=args.daily_trade_cap,
        collect_cap_rows=True,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    summary_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_summary.csv"
    result_df.to_csv(summary_fp, index=False, encoding="utf-8-sig")

    topk_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_top{args.top_k}.csv"
    result_df.head(args.top_k).to_csv(topk_fp, index=False, encoding="utf-8-sig")

    daily_summary_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_daily_signal_summary.csv"
    pd.DataFrame(daily_summary_rows).to_csv(daily_summary_fp, index=False, encoding="utf-8-sig")

    hard_rule_rows = [
        {
            "rule_code": key[0],
            "rule_name": key[1],
            "reason": key[2],
            "threshold_expr": key[3],
            "reject_count": int(cnt),
        }
        for key, cnt in hard_rule_counter.most_common(args.audit_top_n)
    ]
    hard_rule_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_hard_filter_rule_rank.csv"
    pd.DataFrame(hard_rule_rows).to_csv(hard_rule_fp, index=False, encoding="utf-8-sig")

    hard_reason_rows = [
        {"reason": reason, "reject_count": int(cnt)}
        for reason, cnt in hard_reason_counter.most_common(args.audit_top_n)
    ]
    hard_reason_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_hard_filter_reason_rank.csv"
    pd.DataFrame(hard_reason_rows).to_csv(hard_reason_fp, index=False, encoding="utf-8-sig")

    hard_code_rows = [{"code": code, "reject_count": int(cnt)} for code, cnt in hard_code_counter.most_common(args.audit_top_n)]
    hard_code_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_hard_filter_code_rank.csv"
    pd.DataFrame(hard_code_rows).to_csv(hard_code_fp, index=False, encoding="utf-8-sig")

    cap_counter_by_code: Counter = Counter()
    cap_counter_by_date: Counter = Counter()
    for row in cap_rows:
        cap_counter_by_code[str(row["code"])] += 1
        cap_counter_by_date[str(row["signal_date"])] += 1

    cap_code_rows = [{"code": code, "reject_count": int(cnt)} for code, cnt in cap_counter_by_code.most_common(args.audit_top_n)]
    cap_code_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_cap_reject_code_rank.csv"
    pd.DataFrame(cap_code_rows).to_csv(cap_code_fp, index=False, encoding="utf-8-sig")

    cap_date_rows = [
        {"signal_date": d, "reject_count": int(cnt)}
        for d, cnt in cap_counter_by_date.most_common(args.audit_top_n)
    ]
    cap_date_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_cap_reject_date_rank.csv"
    pd.DataFrame(cap_date_rows).to_csv(cap_date_fp, index=False, encoding="utf-8-sig")

    cap_reason_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_cap_reject_reason_rank.csv"
    pd.DataFrame([{"reason": "DAILY_CAP", "reject_count": int(len(cap_rows))}]).to_csv(
        cap_reason_fp,
        index=False,
        encoding="utf-8-sig",
    )

    best_fp = out_dir / f"grid_search_zxbrick_v3_{ts_tag}_best.json"
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date.date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "signal_days": len(calendar),
        "stock_count": len(data),
        "selector_alias": args.selector_alias,
        "daily_trade_cap": args.daily_trade_cap,
        "entry_price": args.entry_price,
        "hold_days": args.hold_days,
        "grid_values": values,
        "grid_size": len(grid),
        "best_weights": best_weights,
        "best_summary": best_summary,
        "output_files": {
            "grid_summary": str(summary_fp),
            "grid_topk": str(topk_fp),
            "daily_signal_summary": str(daily_summary_fp),
            "hard_filter_rule_rank": str(hard_rule_fp),
            "hard_filter_reason_rank": str(hard_reason_fp),
            "hard_filter_code_rank": str(hard_code_fp),
            "cap_reject_code_rank": str(cap_code_fp),
            "cap_reject_date_rank": str(cap_date_fp),
            "cap_reject_reason_rank": str(cap_reason_fp),
        },
    }
    with best_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("网格搜索完成")
    logger.info("最优权重: %s", best_weights)
    logger.info(
        "最优结果: total_return=%.2f%%, max_drawdown=%.2f%%, trade_count=%d",
        float(best_summary.get("total_return", float("nan"))) * 100,
        float(best_summary.get("max_drawdown", float("nan"))) * 100,
        int(best_summary.get("trade_count", 0)),
    )
    logger.info("Summary CSV: %s", summary_fp)
    logger.info("Best JSON: %s", best_fp)


if __name__ == "__main__":
    main()

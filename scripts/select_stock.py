from __future__ import annotations

import argparse
from datetime import datetime
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 将日志写入文件
        logging.FileHandler(LOG_DIR / "select_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("select")
STOCKLIST_FIELDS = ["ts_code", "symbol", "name", "area", "industry"]
STOCKINFO_COLUMNS = ["trade_date", "strategy_alias", "ts_code", "symbol", "name", "area", "industry"]
DETAIL_COLUMNS = ["trade_date", "alias", "class", "rank", "code"]
SUMMARY_COLUMNS = [
    "trade_date",
    "alias",
    "class",
    "count",
    "codes",
    "core_pass_count",
    "hard_pass_count",
    "selected_count",
    "cap_reject_count",
]
FILTER_AUDIT_COLUMNS = [
    "trade_date",
    "alias",
    "class",
    "code",
    "ts_code",
    "stage",
    "rule_code",
    "rule_name",
    "passed",
    "actual_value",
    "threshold_expr",
    "reason",
]
EXEC_AUDIT_COLUMNS = [
    "trade_date",
    "alias",
    "class",
    "code",
    "ts_code",
    "stage",
    "score",
    "brick_strength",
    "rank_before_cap",
    "reject_reason",
]


# ---------- 工具 ----------

def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date")
        frames[code] = df
    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        logger.error("配置文件 %s 不存在", cfg_path)
        sys.exit(1)
    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

    # 兼容三种结构：单对象、对象数组、或带 selectors 键
    if isinstance(cfg_raw, list):
        cfgs = cfg_raw
    elif isinstance(cfg_raw, dict) and "selectors" in cfg_raw:
        cfgs = cfg_raw["selectors"]
    else:
        cfgs = [cfg_raw]

    if not cfgs:
        logger.error("配置文件未定义任何 Selector")
        sys.exit(1)

    return cfgs


def _normalize_symbol(code: str) -> str:
    c = str(code).strip().upper()
    return c.split(".")[0]


def load_stock_meta(stocklist_path: Path) -> Dict[str, Dict[str, str]]:
    """
    从 stocklist.csv 读取股票静态信息，返回以 symbol(6位代码) 为键的映射。
    """
    if not stocklist_path.exists():
        logger.warning("stocklist 文件不存在：%s，将输出空股票信息", stocklist_path)
        return {}

    try:
        stock_df = pd.read_csv(stocklist_path, dtype=str).fillna("")
    except Exception as e:
        logger.warning("读取 stocklist 失败：%s；将输出空股票信息", e)
        return {}

    missing = [c for c in STOCKLIST_FIELDS if c not in stock_df.columns]
    if missing:
        logger.warning("stocklist 缺少字段 %s；将输出空股票信息", missing)
        return {}

    meta: Dict[str, Dict[str, str]] = {}
    for _, row in stock_df.iterrows():
        record = {k: str(row.get(k, "")).strip() for k in STOCKLIST_FIELDS}
        symbol_key = _normalize_symbol(record["symbol"])
        if symbol_key:
            meta[symbol_key] = record
        ts_key = _normalize_symbol(record["ts_code"])
        if ts_key and ts_key not in meta:
            meta[ts_key] = record
    return meta


def instantiate_selector(cfg: Dict[str, Any]):
    """动态加载 Selector 类并实例化"""
    cls_name: str = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    try:
        module = importlib.import_module("core.selector")
        cls = getattr(module, cls_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"无法加载 core.selector.{cls_name}: {e}") from e

    params = cfg.get("params", {})
    return cfg.get("alias", cls_name), cls(**params)


def persist_selection_results(
    *,
    trade_date: pd.Timestamp,
    out_dir: Path,
    run_results: List[Dict[str, Any]],
    stock_meta: Dict[str, Dict[str, str]],
    audit_level: str,
    filter_audit_rows: List[Dict[str, Any]],
    execution_audit_rows: List[Dict[str, Any]],
) -> Dict[str, Path]:
    """
    将当次选股结果落盘：
    - JSON：完整结构化结果（含每个策略 picks）
    - CSV(summary)：每个策略一行，含 count 与逗号拼接的 codes
    - CSV(detail)：每条命中一行（策略 × 股票）
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    date_tag = trade_date.strftime("%Y%m%d")
    ts_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_fp = out_dir / f"select_results_{date_tag}_{ts_tag}.json"
    summary_csv_fp = out_dir / f"select_results_summary_{date_tag}_{ts_tag}.csv"
    detail_csv_fp = out_dir / f"select_results_detail_{date_tag}_{ts_tag}.csv"
    stockinfo_csv_fp = out_dir / f"select_results_stockinfo_{date_tag}_{ts_tag}.csv"
    filter_audit_csv_fp = out_dir / f"select_filter_audit_{date_tag}_{ts_tag}.csv"
    filter_audit_json_fp = out_dir / f"select_filter_audit_{date_tag}_{ts_tag}.json"
    execution_audit_csv_fp = out_dir / f"select_execution_audit_{date_tag}_{ts_tag}.csv"
    execution_audit_json_fp = out_dir / f"select_execution_audit_{date_tag}_{ts_tag}.json"

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trade_date": str(trade_date.date()),
        "strategy_count": len(run_results),
        "results": run_results,
    }
    with json_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    summary_rows = []
    detail_rows = []
    stockinfo_rows = []
    for item in run_results:
        picks = list(item.get("picks", []))
        summary_rows.append(
            {
                "trade_date": str(trade_date.date()),
                "alias": item.get("alias", ""),
                "class": item.get("class", ""),
                "count": int(item.get("count", len(picks))),
                "codes": ",".join(picks),
                "core_pass_count": int(item.get("core_pass_count", 0)),
                "hard_pass_count": int(item.get("hard_pass_count", 0)),
                "selected_count": int(item.get("selected_count", len(picks))),
                "cap_reject_count": int(item.get("cap_reject_count", 0)),
            }
        )
        for idx, code in enumerate(picks, start=1):
            detail_rows.append(
                {
                    "trade_date": str(trade_date.date()),
                    "alias": item.get("alias", ""),
                    "class": item.get("class", ""),
                    "rank": idx,
                    "code": code,
                }
            )
            code_key = _normalize_symbol(code)
            info = stock_meta.get(code_key, {})
            stockinfo_rows.append(
                {
                    "trade_date": str(trade_date.date()),
                    "strategy_alias": item.get("alias", ""),
                    "ts_code": info.get("ts_code", ""),
                    "symbol": info.get("symbol", code_key),
                    "name": info.get("name", ""),
                    "area": info.get("area", ""),
                    "industry": info.get("industry", ""),
                }
            )

    pd.DataFrame(summary_rows, columns=SUMMARY_COLUMNS).to_csv(summary_csv_fp, index=False, encoding="utf-8-sig")
    pd.DataFrame(detail_rows, columns=DETAIL_COLUMNS).to_csv(detail_csv_fp, index=False, encoding="utf-8-sig")
    pd.DataFrame(stockinfo_rows, columns=STOCKINFO_COLUMNS).to_csv(stockinfo_csv_fp, index=False, encoding="utf-8-sig")
    pd.DataFrame(filter_audit_rows, columns=FILTER_AUDIT_COLUMNS).to_csv(
        filter_audit_csv_fp,
        index=False,
        encoding="utf-8-sig",
    )
    pd.DataFrame(execution_audit_rows, columns=EXEC_AUDIT_COLUMNS).to_csv(
        execution_audit_csv_fp,
        index=False,
        encoding="utf-8-sig",
    )

    filter_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trade_date": str(trade_date.date()),
        "audit_level": audit_level,
        "rows": filter_audit_rows,
    }
    execution_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trade_date": str(trade_date.date()),
        "audit_level": audit_level,
        "rows": execution_audit_rows,
    }
    with filter_audit_json_fp.open("w", encoding="utf-8") as f:
        json.dump(filter_payload, f, ensure_ascii=False, indent=2)
    with execution_audit_json_fp.open("w", encoding="utf-8") as f:
        json.dump(execution_payload, f, ensure_ascii=False, indent=2)

    return {
        "json": json_fp,
        "summary_csv": summary_csv_fp,
        "detail_csv": detail_csv_fp,
        "stockinfo_csv": stockinfo_csv_fp,
        "filter_audit_csv": filter_audit_csv_fp,
        "filter_audit_json": filter_audit_json_fp,
        "execution_audit_csv": execution_audit_csv_fp,
        "execution_audit_json": execution_audit_json_fp,
    }


# ---------- 主函数 ----------

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs/configs.json")
    p.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    p.add_argument("--config", default="./configs/configs.json", help="Selector 配置文件")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
    p.add_argument("--out-dir", default="./out", help="结果落盘目录（默认 ./out）")
    p.add_argument("--stocklist", default="./configs/stocklist.csv", help="股票信息 CSV（默认 ./configs/stocklist.csv）")
    p.add_argument(
        "--audit",
        choices=["off", "failed_only", "full"],
        default="failed_only",
        help="过滤审计级别：off/failed_only/full",
    )
    args = p.parse_args()

    # --- 加载行情 ---
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error("数据目录 %s 不存在", data_dir)
        sys.exit(1)

    codes = (
        [f.stem for f in data_dir.glob("*.csv")]
        if args.tickers.lower() == "all"
        else [c.strip() for c in args.tickers.split(",") if c.strip()]
    )
    if not codes:
        logger.error("股票池为空！")
        sys.exit(1)

    data = load_data(data_dir, codes)
    if not data:
        logger.error("未能加载任何行情数据")
        sys.exit(1)

    trade_date = (
        pd.to_datetime(args.date)
        if args.date
        else max(df["date"].max() for df in data.values())
    )
    if not args.date:
        logger.info("未指定 --date，使用最近日期 %s", trade_date.date())

    # --- 加载 Selector 配置 ---
    selector_cfgs = load_config(Path(args.config))
    stock_meta = load_stock_meta(Path(args.stocklist))
    run_results: List[Dict[str, Any]] = []
    filter_audit_rows: List[Dict[str, Any]] = []
    execution_audit_rows: List[Dict[str, Any]] = []

    # --- 逐个 Selector 运行 ---
    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        core_pass_count = 0
        hard_pass_count = 0
        selected_count = 0
        cap_reject_count = 0

        if hasattr(selector, "select_with_audit"):
            select_result = selector.select_with_audit(trade_date, data, audit_level=args.audit)
            picks = list(select_result.get("picks_final", []))
            summary = dict(select_result.get("audit_summary", {}))
            core_pass_count = int(summary.get("core_pass_count", 0))
            hard_pass_count = int(summary.get("hard_pass_count", 0))
            selected_count = int(summary.get("selected_count", len(picks)))
            cap_reject_count = int(summary.get("cap_reject_count", 0))
            for row in list(select_result.get("audit_rows", [])):
                merged = {
                    "trade_date": str(trade_date.date()),
                    "alias": alias,
                    "class": cfg.get("class", ""),
                    **row,
                }
                if row.get("stage") == "execution":
                    execution_audit_rows.append(merged)
                else:
                    filter_audit_rows.append(merged)
        else:
            picks = selector.select(trade_date, data)
            selected_count = len(picks)

        run_results.append(
            {
                "alias": alias,
                "class": cfg.get("class", ""),
                "count": len(picks),
                "picks": picks,
                "core_pass_count": core_pass_count,
                "hard_pass_count": hard_pass_count,
                "selected_count": selected_count,
                "cap_reject_count": cap_reject_count,
            }
        )

        # 将结果写入日志，同时输出到控制台
        logger.info("")
        logger.info("============== 选股结果 [%s] ==============", alias)
        logger.info("交易日: %s", trade_date.date())
        logger.info("符合条件股票数: %d", len(picks))
        logger.info("%s", ", ".join(picks) if picks else "无符合条件股票")

    # --- 结果落盘 ---
    output_files = persist_selection_results(
        trade_date=trade_date,
        out_dir=Path(args.out_dir),
        run_results=run_results,
        stock_meta=stock_meta,
        audit_level=args.audit,
        filter_audit_rows=filter_audit_rows,
        execution_audit_rows=execution_audit_rows,
    )
    logger.info("")
    logger.info("结果已落盘：")
    logger.info("JSON: %s", output_files["json"])
    logger.info("Summary CSV: %s", output_files["summary_csv"])
    logger.info("Detail CSV: %s", output_files["detail_csv"])
    logger.info("StockInfo CSV: %s", output_files["stockinfo_csv"])
    logger.info("Filter Audit CSV: %s", output_files["filter_audit_csv"])
    logger.info("Filter Audit JSON: %s", output_files["filter_audit_json"])
    logger.info("Execution Audit CSV: %s", output_files["execution_audit_csv"])
    logger.info("Execution Audit JSON: %s", output_files["execution_audit_json"])


if __name__ == "__main__":
    main()

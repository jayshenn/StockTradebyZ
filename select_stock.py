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

# ---------- 日志 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 将日志写入文件
        logging.FileHandler("select_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("select")


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
        logger.error("configs.json 未定义任何 Selector")
        sys.exit(1)

    return cfgs


def instantiate_selector(cfg: Dict[str, Any]):
    """动态加载 Selector 类并实例化"""
    cls_name: str = cfg.get("class")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    try:
        module = importlib.import_module("Selector")
        cls = getattr(module, cls_name)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f"无法加载 Selector.{cls_name}: {e}") from e

    params = cfg.get("params", {})
    return cfg.get("alias", cls_name), cls(**params)


def persist_selection_results(
    *,
    trade_date: pd.Timestamp,
    out_dir: Path,
    run_results: List[Dict[str, Any]],
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
    for item in run_results:
        picks = list(item.get("picks", []))
        summary_rows.append(
            {
                "trade_date": str(trade_date.date()),
                "alias": item.get("alias", ""),
                "class": item.get("class", ""),
                "count": int(item.get("count", len(picks))),
                "codes": ",".join(picks),
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

    pd.DataFrame(summary_rows).to_csv(summary_csv_fp, index=False, encoding="utf-8-sig")
    pd.DataFrame(detail_rows).to_csv(detail_csv_fp, index=False, encoding="utf-8-sig")

    return {
        "json": json_fp,
        "summary_csv": summary_csv_fp,
        "detail_csv": detail_csv_fp,
    }


# ---------- 主函数 ----------

def main():
    p = argparse.ArgumentParser(description="Run selectors defined in configs.json")
    p.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    p.add_argument("--config", default="./configs.json", help="Selector 配置文件")
    p.add_argument("--date", help="交易日 YYYY-MM-DD；缺省=数据最新日期")
    p.add_argument("--tickers", default="all", help="'all' 或逗号分隔股票代码列表")
    p.add_argument("--out-dir", default="./out", help="结果落盘目录（默认 ./out）")
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
    run_results: List[Dict[str, Any]] = []

    # --- 逐个 Selector 运行 ---
    for cfg in selector_cfgs:
        if cfg.get("activate", True) is False:
            continue
        try:
            alias, selector = instantiate_selector(cfg)
        except Exception as e:
            logger.error("跳过配置 %s：%s", cfg, e)
            continue

        picks = selector.select(trade_date, data)
        run_results.append(
            {
                "alias": alias,
                "class": cfg.get("class", ""),
                "count": len(picks),
                "picks": picks,
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
    )
    logger.info("")
    logger.info("结果已落盘：")
    logger.info("JSON: %s", output_files["json"])
    logger.info("Summary CSV: %s", output_files["summary_csv"])
    logger.info("Detail CSV: %s", output_files["detail_csv"])


if __name__ == "__main__":
    main()

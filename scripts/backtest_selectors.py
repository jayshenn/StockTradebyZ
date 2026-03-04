from __future__ import annotations

import argparse
import importlib
import json
import logging
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.selector import compute_bbi

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "backtest_results.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("backtest")


@dataclass
class TradeResult:
    code: str
    signal_date: pd.Timestamp
    entry_idx: int
    exit_idx: int
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    trade_return: float
    daily_returns: List[tuple[pd.Timestamp, float]]
    holding_bars: int
    exit_reason: str
    partial_take_count: int
    stop_price: float
    entry_low: float
    prev_swing_low: float


def load_data(data_dir: Path, codes: Iterable[str]) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    for code in codes:
        fp = data_dir / f"{code}.csv"
        if not fp.exists():
            logger.warning("%s 不存在，跳过", fp.name)
            continue
        df = pd.read_csv(fp, parse_dates=["date"]).sort_values("date")
        if df.empty:
            continue
        df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])
        # 预计算 BBI，供自适应止盈止损离场使用
        df["BBI"] = compute_bbi(df)
        frames[code] = df
    return frames


def load_config(cfg_path: Path) -> List[Dict[str, Any]]:
    if not cfg_path.exists():
        logger.error("配置文件 %s 不存在", cfg_path)
        sys.exit(1)

    with cfg_path.open(encoding="utf-8") as f:
        cfg_raw = json.load(f)

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


def instantiate_selector(cfg: Dict[str, Any]):
    cls_name: str = cfg.get("class", "")
    if not cls_name:
        raise ValueError("缺少 class 字段")

    module = importlib.import_module("core.selector")
    cls = getattr(module, cls_name)
    params = cfg.get("params", {})
    alias = cfg.get("alias", cls_name)
    return alias, cls_name, cls(**params)


def normalize_selector_filter(selector_filter: str) -> set[str]:
    items = [x.strip() for x in selector_filter.split(",") if x.strip()]
    return set(items)


def build_calendar(
    data: Dict[str, pd.DataFrame],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> List[pd.Timestamp]:
    all_dates: set[pd.Timestamp] = set()
    for df in data.values():
        in_range = df[(df["date"] >= start_date) & (df["date"] <= end_date)]["date"]
        all_dates.update(pd.Timestamp(x) for x in in_range.tolist())
    return sorted(all_dates)


def find_next_bar_idx(df: pd.DataFrame, signal_date: pd.Timestamp) -> Optional[int]:
    date_arr = df["date"].to_numpy(dtype="datetime64[ns]")
    idx = int(np.searchsorted(date_arr, np.datetime64(signal_date), side="right"))
    if idx >= len(df):
        return None
    return idx


def safe_slug(value: str) -> str:
    normalized = re.sub(r"\s+", "_", value.strip())
    normalized = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff-]", "_", normalized)
    return normalized or "selector"


def calc_max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return float("nan")
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def calc_metrics(
    *,
    daily_returns: pd.Series,
    active_positions: pd.Series,
    trade_returns: List[float],
) -> Dict[str, float]:
    if daily_returns.empty:
        return {}

    n_days = len(daily_returns)
    equity = (1.0 + daily_returns).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)
    annualized_return = float((equity.iloc[-1]) ** (252.0 / n_days) - 1.0) if n_days > 0 else float("nan")
    annualized_vol = float(daily_returns.std(ddof=0) * math.sqrt(252.0))
    sharpe = float(annualized_return / annualized_vol) if annualized_vol > 0 else float("nan")
    max_drawdown = calc_max_drawdown(equity)

    active_mask = active_positions > 0
    active_day_returns = daily_returns[active_mask]
    day_win_rate = (
        float((active_day_returns > 0).sum() / len(active_day_returns))
        if len(active_day_returns) > 0
        else float("nan")
    )

    trade_count = len(trade_returns)
    trade_win_rate = float(sum(r > 0 for r in trade_returns) / trade_count) if trade_count > 0 else float("nan")
    avg_trade_return = float(np.mean(trade_returns)) if trade_count > 0 else float("nan")
    median_trade_return = float(np.median(trade_returns)) if trade_count > 0 else float("nan")

    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_vol,
        "sharpe_rf0": sharpe,
        "max_drawdown": max_drawdown,
        "active_days": int(active_mask.sum()),
        "active_day_ratio": float(active_mask.mean()),
        "day_win_rate_on_active_days": day_win_rate,
        "trade_count": int(trade_count),
        "trade_win_rate": trade_win_rate,
        "avg_trade_return": avg_trade_return,
        "median_trade_return": median_trade_return,
    }


def find_prev_n_swing_low(df: pd.DataFrame, before_idx: int, lookback: int) -> float:
    """
    近似“N 型结构上一个低点”：
    在 before_idx 之前 lookback 范围内，寻找最后一个局部低点(low[i] <= low[i-1] 且 low[i] < low[i+1])。
    若找不到局部低点，则退化为该窗口最低 low。
    """
    if before_idx <= 1:
        return float("nan")

    start = max(1, before_idx - lookback)
    end = before_idx - 1
    lows = df["low"].astype(float).to_numpy()

    pivot_candidates: List[float] = []
    for i in range(start, end):
        if i + 1 >= before_idx:
            break
        if np.isfinite(lows[i - 1]) and np.isfinite(lows[i]) and np.isfinite(lows[i + 1]):
            if lows[i] <= lows[i - 1] and lows[i] < lows[i + 1]:
                pivot_candidates.append(float(lows[i]))

    if pivot_candidates:
        return float(pivot_candidates[-1])

    seg = df.iloc[start:before_idx]["low"].astype(float).dropna()
    if seg.empty:
        return float("nan")
    return float(seg.min())


def collect_fixed_daily_returns(
    *,
    df: pd.DataFrame,
    entry_idx: int,
    exit_idx: int,
    entry_price: str,
) -> List[tuple[pd.Timestamp, float]]:
    rows: List[tuple[pd.Timestamp, float]] = []
    for idx in range(entry_idx, exit_idx + 1):
        bar = df.iloc[idx]
        bar_date = pd.Timestamp(bar["date"])
        if idx == entry_idx:
            if entry_price == "open":
                open_px = float(bar["open"])
                close_px = float(bar["close"])
                if not np.isfinite(open_px) or not np.isfinite(close_px) or open_px <= 0:
                    continue
                daily_ret = close_px / open_px - 1.0
            else:
                daily_ret = 0.0
        else:
            prev_close = float(df.iloc[idx - 1]["close"])
            close_px = float(bar["close"])
            if not np.isfinite(prev_close) or not np.isfinite(close_px) or prev_close <= 0:
                continue
            daily_ret = close_px / prev_close - 1.0
        rows.append((bar_date, daily_ret))
    return rows


def create_fixed_trade(
    *,
    code: str,
    signal_date: pd.Timestamp,
    df: pd.DataFrame,
    hold_days: int,
    entry_price: str,
    end_date: pd.Timestamp,
) -> Optional[TradeResult]:
    entry_idx = find_next_bar_idx(df, signal_date)
    if entry_idx is None:
        return None
    exit_idx = entry_idx + hold_days - 1
    if exit_idx >= len(df):
        return None

    entry_date = pd.Timestamp(df.iloc[entry_idx]["date"])
    exit_date = pd.Timestamp(df.iloc[exit_idx]["date"])
    if exit_date > end_date:
        return None

    entry_px = float(df.iloc[entry_idx][entry_price])
    exit_px = float(df.iloc[exit_idx]["close"])
    if not np.isfinite(entry_px) or not np.isfinite(exit_px) or entry_px <= 0:
        return None

    daily_returns = collect_fixed_daily_returns(
        df=df,
        entry_idx=entry_idx,
        exit_idx=exit_idx,
        entry_price=entry_price,
    )
    trade_return = float(np.prod([1.0 + x[1] for x in daily_returns]) - 1.0) if daily_returns else float("nan")

    entry_low = float(df.iloc[entry_idx]["low"]) if np.isfinite(float(df.iloc[entry_idx]["low"])) else float("nan")
    return TradeResult(
        code=code,
        signal_date=signal_date,
        entry_idx=entry_idx,
        exit_idx=exit_idx,
        entry_date=entry_date,
        exit_date=exit_date,
        trade_return=trade_return,
        daily_returns=daily_returns,
        holding_bars=exit_idx - entry_idx + 1,
        exit_reason="fixed_hold_days",
        partial_take_count=0,
        stop_price=float("nan"),
        entry_low=entry_low,
        prev_swing_low=float("nan"),
    )


def simulate_adaptive_trade(
    *,
    code: str,
    signal_date: pd.Timestamp,
    df: pd.DataFrame,
    entry_price: str,
    end_date: pd.Timestamp,
    max_hold_days: int,
    stop_buffer_pct: float,
    n_structure_lookback: int,
    big_bull_body_pct: float,
    min_position_ratio_for_halve: float,
) -> Optional[TradeResult]:
    entry_idx = find_next_bar_idx(df, signal_date)
    if entry_idx is None:
        return None

    entry_date = pd.Timestamp(df.iloc[entry_idx]["date"])
    if entry_date > end_date:
        return None

    entry_open = float(df.iloc[entry_idx]["open"])
    entry_close = float(df.iloc[entry_idx]["close"])
    entry_low = float(df.iloc[entry_idx]["low"])
    if (not np.isfinite(entry_open)) or (not np.isfinite(entry_close)) or (not np.isfinite(entry_low)):
        return None
    if entry_open <= 0 or entry_close <= 0 or entry_low <= 0:
        return None

    prev_swing_low = find_prev_n_swing_low(df, before_idx=entry_idx, lookback=n_structure_lookback)
    stop_anchor = entry_low
    if np.isfinite(prev_swing_low) and prev_swing_low > 0:
        stop_anchor = min(entry_low, prev_swing_low)
    stop_price = stop_anchor * (1.0 - stop_buffer_pct)

    qty = 0.0
    cash = 0.0
    value_prev = 1.0
    below_bbi_streak = 0
    bull_streak = 0
    partial_take_count = 0
    daily_returns: List[tuple[pd.Timestamp, float]] = []
    exit_idx = -1
    exit_reason = "end_of_backtest"
    holding_bars = 0

    if entry_price == "open":
        qty = 1.0 / entry_open
    else:
        # close 入场：信号后第一根 K 线收盘买入，当天收益记为 0
        qty = 0.0

    i = entry_idx
    while i < len(df):
        bar_date = pd.Timestamp(df.iloc[i]["date"])
        if bar_date > end_date:
            break

        holding_bars += 1
        o = float(df.iloc[i]["open"])
        h = float(df.iloc[i]["high"])
        l = float(df.iloc[i]["low"])
        c = float(df.iloc[i]["close"])
        bbi = float(df.iloc[i]["BBI"]) if pd.notna(df.iloc[i]["BBI"]) else float("nan")
        if not (np.isfinite(o) and np.isfinite(h) and np.isfinite(l) and np.isfinite(c)):
            i += 1
            continue

        # close 入场的首日：收盘买入，当日不承担价格波动
        if i == entry_idx and entry_price == "close":
            qty = 1.0 / c
            daily_returns.append((bar_date, 0.0))
            value_prev = 1.0
            if np.isfinite(bbi):
                below_bbi_streak = 1 if c < bbi else 0
                bull_streak = 1 if (c > bbi and c > o and (c / o - 1.0) >= big_bull_body_pct) else 0
            else:
                below_bbi_streak = 0
                bull_streak = 0
            i += 1
            continue

        # 1) 止损优先：若当根最低跌破止损线，按止损价清仓
        if qty > 0 and l <= stop_price:
            value_end = cash + qty * stop_price
            day_ret = value_end / value_prev - 1.0
            daily_returns.append((bar_date, day_ret))
            cash = value_end
            qty = 0.0
            value_prev = value_end
            exit_idx = i
            exit_reason = "stop_loss_break"
            break

        # 日终市值（先按收盘估值，再做收盘动作）
        value_pre_close_action = cash + qty * c

        # 3) 连续两日收盘跌破 BBI：全离场
        if qty > 0:
            if np.isfinite(bbi) and c < bbi:
                below_bbi_streak += 1
            else:
                below_bbi_streak = 0

            if below_bbi_streak >= 2:
                cash = value_pre_close_action
                qty = 0.0
                value_end = cash
                day_ret = value_end / value_prev - 1.0
                daily_returns.append((bar_date, day_ret))
                value_prev = value_end
                exit_idx = i
                exit_reason = "two_close_below_bbi"
                break

            # 2) 止盈放飞：BBI 线上连续两根中大阳线，减半仓；后续同样处理
            if np.isfinite(bbi) and c > bbi and c > o and (c / o - 1.0) >= big_bull_body_pct:
                bull_streak += 1
            else:
                bull_streak = 0

            if bull_streak >= 2 and qty > 0:
                current_position_value = qty * c
                if current_position_value > min_position_ratio_for_halve:
                    sell_qty = qty * 0.5
                    cash += sell_qty * c
                    qty -= sell_qty
                    partial_take_count += 1
                bull_streak = 0

        # 达到最大持有上限，收盘离场（保险阀）
        if qty > 0 and holding_bars >= max_hold_days:
            cash += qty * c
            qty = 0.0
            value_end = cash
            day_ret = value_end / value_prev - 1.0
            daily_returns.append((bar_date, day_ret))
            value_prev = value_end
            exit_idx = i
            exit_reason = "max_hold_days"
            break

        # 正常收盘记账
        value_end = cash + qty * c
        day_ret = value_end / value_prev - 1.0
        daily_returns.append((bar_date, day_ret))
        value_prev = value_end
        i += 1

    # 回测结束仍有仓位：按最后一日收盘强平
    if exit_idx < 0:
        last_date = daily_returns[-1][0] if daily_returns else entry_date
        last_idx = int(df.index[df["date"] == last_date][0]) if not df[df["date"] == last_date].empty else entry_idx
        if qty > 0 and np.isfinite(float(df.iloc[last_idx]["close"])):
            c = float(df.iloc[last_idx]["close"])
            cash += qty * c
            qty = 0.0
            # 当日已记过收益，此处只更新最终状态，不重复记日收益
        exit_idx = last_idx
        exit_reason = "end_of_backtest"

    if exit_idx < entry_idx:
        return None

    exit_date = pd.Timestamp(df.iloc[exit_idx]["date"])
    trade_return = float(cash - 1.0)
    return TradeResult(
        code=code,
        signal_date=signal_date,
        entry_idx=entry_idx,
        exit_idx=exit_idx,
        entry_date=entry_date,
        exit_date=exit_date,
        trade_return=trade_return,
        daily_returns=daily_returns,
        holding_bars=exit_idx - entry_idx + 1,
        exit_reason=exit_reason,
        partial_take_count=partial_take_count,
        stop_price=stop_price,
        entry_low=entry_low,
        prev_swing_low=prev_swing_low,
    )


def create_trade_for_signal(
    *,
    exit_mode: str,
    code: str,
    signal_date: pd.Timestamp,
    df: pd.DataFrame,
    hold_days: int,
    entry_price: str,
    end_date: pd.Timestamp,
    max_hold_days: int,
    stop_buffer_pct: float,
    n_structure_lookback: int,
    big_bull_body_pct: float,
    min_position_ratio_for_halve: float,
) -> Optional[TradeResult]:
    if exit_mode == "fixed":
        return create_fixed_trade(
            code=code,
            signal_date=signal_date,
            df=df,
            hold_days=hold_days,
            entry_price=entry_price,
            end_date=end_date,
        )

    return simulate_adaptive_trade(
        code=code,
        signal_date=signal_date,
        df=df,
        entry_price=entry_price,
        end_date=end_date,
        max_hold_days=max_hold_days,
        stop_buffer_pct=stop_buffer_pct,
        n_structure_lookback=n_structure_lookback,
        big_bull_body_pct=big_bull_body_pct,
        min_position_ratio_for_halve=min_position_ratio_for_halve,
    )


def compute_benchmark_returns(
    *,
    calendar: List[pd.Timestamp],
    data: Dict[str, pd.DataFrame],
    benchmark_code: str,
) -> pd.Series:
    if benchmark_code not in data:
        return pd.Series([np.nan] * len(calendar), index=calendar, dtype=float)

    bench_df = data[benchmark_code]
    bench_by_date = {pd.Timestamp(d): i for i, d in enumerate(bench_df["date"].tolist())}

    vals: List[float] = []
    for d in calendar:
        idx = bench_by_date.get(d)
        if idx is None or idx <= 0:
            vals.append(0.0)
            continue
        prev_close = float(bench_df.iloc[idx - 1]["close"])
        close_px = float(bench_df.iloc[idx]["close"])
        if prev_close <= 0:
            vals.append(0.0)
            continue
        vals.append(close_px / prev_close - 1.0)
    return pd.Series(vals, index=calendar, dtype=float)


def backtest_selector(
    *,
    alias: str,
    cls_name: str,
    selector: Any,
    data: Dict[str, pd.DataFrame],
    calendar: List[pd.Timestamp],
    hold_days: int,
    entry_price: str,
    end_date: pd.Timestamp,
    exit_mode: str,
    max_hold_days: int,
    stop_buffer_pct: float,
    n_structure_lookback: int,
    big_bull_body_pct: float,
    min_position_ratio_for_halve: float,
) -> tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame]:
    daily_buckets: Dict[pd.Timestamp, List[float]] = defaultdict(list)
    trades: List[TradeResult] = []
    picks_per_signal: List[int] = []
    core_pass_per_signal: List[int] = []
    hard_pass_per_signal: List[int] = []
    selected_per_signal: List[int] = []
    cap_reject_per_signal: List[int] = []
    signal_days = 0
    cap_binding_days = 0

    for signal_date in calendar:
        if hasattr(selector, "select_with_audit"):
            select_result = selector.select_with_audit(signal_date, data, audit_level="off")
            picks = list(select_result.get("picks_final", []))
            audit_summary = dict(select_result.get("audit_summary", {}))
            core_cnt = int(audit_summary.get("core_pass_count", len(picks)))
            hard_cnt = int(audit_summary.get("hard_pass_count", len(picks)))
            selected_cnt = int(audit_summary.get("selected_count", len(picks)))
            cap_reject_cnt = int(audit_summary.get("cap_reject_count", 0))
        else:
            picks = selector.select(signal_date, data)
            core_cnt = len(picks)
            hard_cnt = len(picks)
            selected_cnt = len(picks)
            cap_reject_cnt = 0

        picks_per_signal.append(len(picks))
        core_pass_per_signal.append(core_cnt)
        hard_pass_per_signal.append(hard_cnt)
        selected_per_signal.append(selected_cnt)
        cap_reject_per_signal.append(cap_reject_cnt)
        if cap_reject_cnt > 0:
            cap_binding_days += 1
        signal_days += 1
        if not picks:
            continue

        for code in picks:
            df = data.get(code)
            if df is None or df.empty:
                continue
            trade = create_trade_for_signal(
                exit_mode=exit_mode,
                code=code,
                signal_date=signal_date,
                df=df,
                hold_days=hold_days,
                entry_price=entry_price,
                end_date=end_date,
                max_hold_days=max_hold_days,
                stop_buffer_pct=stop_buffer_pct,
                n_structure_lookback=n_structure_lookback,
                big_bull_body_pct=big_bull_body_pct,
                min_position_ratio_for_halve=min_position_ratio_for_halve,
            )
            if trade is None:
                continue
            trades.append(trade)

    for tr in trades:
        for d, ret in tr.daily_returns:
            daily_buckets[d].append(ret)

    strategy_daily_returns: List[float] = []
    active_positions_daily: List[int] = []
    for d in calendar:
        rets = daily_buckets.get(d, [])
        active_positions_daily.append(len(rets))
        strategy_daily_returns.append(float(np.mean(rets)) if rets else 0.0)

    daily_ret_s = pd.Series(strategy_daily_returns, index=calendar, dtype=float)
    active_s = pd.Series(active_positions_daily, index=calendar, dtype=int)
    equity_s = (1.0 + daily_ret_s).cumprod()
    trade_returns = [t.trade_return for t in trades]
    metrics = calc_metrics(daily_returns=daily_ret_s, active_positions=active_s, trade_returns=trade_returns)

    summary: Dict[str, Any] = {
        "alias": alias,
        "class": cls_name,
        "signal_days": signal_days,
        "avg_picks_per_signal_day": float(np.mean(picks_per_signal)) if picks_per_signal else 0.0,
        "avg_core_pass_per_day": float(np.mean(core_pass_per_signal)) if core_pass_per_signal else 0.0,
        "avg_hard_pass_per_day": float(np.mean(hard_pass_per_signal)) if hard_pass_per_signal else 0.0,
        "avg_selected_per_day": float(np.mean(selected_per_signal)) if selected_per_signal else 0.0,
        "avg_cap_reject_per_day": float(np.mean(cap_reject_per_signal)) if cap_reject_per_signal else 0.0,
        "cap_binding_ratio": float(cap_binding_days / signal_days) if signal_days > 0 else 0.0,
        "exit_mode": exit_mode,
        "hold_days": hold_days,
        "max_hold_days": max_hold_days if exit_mode == "adaptive" else None,
        "entry_price": entry_price,
        "stop_buffer_pct": stop_buffer_pct if exit_mode == "adaptive" else None,
        "n_structure_lookback": n_structure_lookback if exit_mode == "adaptive" else None,
        "big_bull_body_pct": big_bull_body_pct if exit_mode == "adaptive" else None,
    }
    summary.update(metrics)

    daily_df = pd.DataFrame(
        {
            "date": calendar,
            "strategy_daily_return": daily_ret_s.values,
            "strategy_equity": equity_s.values,
            "active_positions": active_s.values,
        }
    )
    trades_df = pd.DataFrame(
        [
            {
                "code": t.code,
                "signal_date": t.signal_date.date(),
                "entry_date": t.entry_date.date(),
                "exit_date": t.exit_date.date(),
                "holding_bars": t.holding_bars,
                "trade_return": t.trade_return,
                "exit_reason": t.exit_reason,
                "partial_take_count": t.partial_take_count,
                "stop_price": t.stop_price,
                "entry_low": t.entry_low,
                "prev_swing_low": t.prev_swing_low,
            }
            for t in trades
        ]
    )
    return summary, daily_df, trades_df


def main() -> None:
    parser = argparse.ArgumentParser(description="对 selector 策略进行历史回测")
    parser.add_argument("--data-dir", default="./data", help="CSV 行情目录")
    parser.add_argument("--config", default="./configs/configs.json", help="Selector 配置文件")
    parser.add_argument("--start-date", required=True, help="回测起始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="回测结束日期 YYYY-MM-DD")
    parser.add_argument("--tickers", default="all", help="'all' 或逗号分隔代码列表")
    parser.add_argument("--selector", default="all", help="'all' 或逗号分隔 alias/class")
    parser.add_argument("--entry-price", choices=["open", "close"], default="open", help="入场价字段")
    parser.add_argument("--exit-mode", choices=["fixed", "adaptive"], default="fixed", help="离场模式")
    parser.add_argument("--hold-days", type=int, default=5, help="fixed 模式下每笔持有 K 线根数")
    parser.add_argument("--max-hold-days", type=int, default=60, help="adaptive 模式最大持有根数（保险阀）")
    parser.add_argument("--stop-buffer-pct", type=float, default=0.001, help="止损线下浮比例，默认 0.1%")
    parser.add_argument("--n-structure-lookback", type=int, default=60, help="N 型前低点回看窗口")
    parser.add_argument("--big-bull-body-pct", type=float, default=0.03, help="中大阳线实体阈值，默认 3%")
    parser.add_argument(
        "--min-position-ratio-for-halve",
        type=float,
        default=0.02,
        help="剩余仓位市值低于该比例时不再继续减半",
    )
    parser.add_argument("--benchmark-code", default="", help="可选：基准代码（如 000001）")
    parser.add_argument("--out-dir", default="./out", help="结果输出目录")
    args = parser.parse_args()

    if args.hold_days < 1:
        logger.error("--hold-days 必须 >= 1")
        sys.exit(1)
    if args.max_hold_days < 1:
        logger.error("--max-hold-days 必须 >= 1")
        sys.exit(1)
    if not (0 <= args.stop_buffer_pct <= 0.2):
        logger.error("--stop-buffer-pct 建议位于 [0, 0.2]")
        sys.exit(1)
    if args.n_structure_lookback < 3:
        logger.error("--n-structure-lookback 必须 >= 3")
        sys.exit(1)
    if not (0 < args.big_bull_body_pct < 1):
        logger.error("--big-bull-body-pct 应位于 (0,1)")
        sys.exit(1)
    if not (0 < args.min_position_ratio_for_halve < 1):
        logger.error("--min-position-ratio-for-halve 应位于 (0,1)")
        sys.exit(1)

    start_date = pd.to_datetime(args.start_date)
    end_date = pd.to_datetime(args.end_date)
    if end_date < start_date:
        logger.error("--end-date 必须 >= --start-date")
        sys.exit(1)

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
        logger.error("股票池为空")
        sys.exit(1)

    data = load_data(data_dir, codes)
    if not data:
        logger.error("未加载到任何行情数据")
        sys.exit(1)

    calendar = build_calendar(data=data, start_date=start_date, end_date=end_date)
    if not calendar:
        logger.error("指定区间无可用交易日：%s ~ %s", start_date.date(), end_date.date())
        sys.exit(1)

    cfgs = load_config(Path(args.config))
    selector_filter = normalize_selector_filter(args.selector) if args.selector.lower() != "all" else None

    selected_cfgs: List[Dict[str, Any]] = []
    for cfg in cfgs:
        if cfg.get("activate", True) is False:
            continue
        alias = str(cfg.get("alias", cfg.get("class", "")))
        cls_name = str(cfg.get("class", ""))
        if selector_filter and alias not in selector_filter and cls_name not in selector_filter:
            continue
        selected_cfgs.append(cfg)

    if not selected_cfgs:
        logger.error("没有匹配到可运行的 selector 配置")
        sys.exit(1)

    logger.info(
        "回测开始：%s ~ %s, 交易日 %d, 股票数 %d, 策略数 %d, exit_mode=%s",
        start_date.date(),
        end_date.date(),
        len(calendar),
        len(data),
        len(selected_cfgs),
        args.exit_mode,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    summary_rows: List[Dict[str, Any]] = []
    all_results: List[Dict[str, Any]] = []

    benchmark_series: Optional[pd.Series] = None
    if args.benchmark_code.strip():
        benchmark_code = args.benchmark_code.strip()
        benchmark_series = compute_benchmark_returns(calendar=calendar, data=data, benchmark_code=benchmark_code)
        bench_equity = (1.0 + benchmark_series.fillna(0.0)).cumprod()
        bench_metrics = calc_metrics(
            daily_returns=benchmark_series.fillna(0.0),
            active_positions=pd.Series(np.ones(len(calendar), dtype=int), index=calendar),
            trade_returns=[],
        )
        logger.info(
            "基准 %s：总收益 %.2f%%, 最大回撤 %.2f%%",
            benchmark_code,
            bench_metrics.get("total_return", float("nan")) * 100,
            bench_metrics.get("max_drawdown", float("nan")) * 100,
        )
        bench_df = pd.DataFrame(
            {
                "date": calendar,
                "benchmark_daily_return": benchmark_series.values,
                "benchmark_equity": bench_equity.values,
            }
        )
        bench_fp = out_dir / f"backtest_{ts_tag}_benchmark_{safe_slug(benchmark_code)}.csv"
        bench_df.to_csv(bench_fp, index=False, encoding="utf-8-sig")
        logger.info("基准明细: %s", bench_fp)

    for cfg in selected_cfgs:
        alias, cls_name, selector = instantiate_selector(cfg)
        logger.info("运行策略: %s (%s)", alias, cls_name)

        summary, daily_df, trades_df = backtest_selector(
            alias=alias,
            cls_name=cls_name,
            selector=selector,
            data=data,
            calendar=calendar,
            hold_days=args.hold_days,
            entry_price=args.entry_price,
            end_date=end_date,
            exit_mode=args.exit_mode,
            max_hold_days=args.max_hold_days,
            stop_buffer_pct=args.stop_buffer_pct,
            n_structure_lookback=args.n_structure_lookback,
            big_bull_body_pct=args.big_bull_body_pct,
            min_position_ratio_for_halve=args.min_position_ratio_for_halve,
        )

        if benchmark_series is not None:
            daily_df["benchmark_daily_return"] = benchmark_series.values
            daily_df["benchmark_equity"] = (1.0 + benchmark_series.fillna(0.0)).cumprod().values
            rel = daily_df["strategy_equity"] / daily_df["benchmark_equity"].replace(0, np.nan)
            summary["relative_total_return_vs_benchmark"] = float(rel.iloc[-1] - 1.0) if not rel.empty else float("nan")

        slug = safe_slug(alias)
        daily_fp = out_dir / f"backtest_{ts_tag}_{slug}_daily.csv"
        trades_fp = out_dir / f"backtest_{ts_tag}_{slug}_trades.csv"
        daily_df.to_csv(daily_fp, index=False, encoding="utf-8-sig")
        trades_df.to_csv(trades_fp, index=False, encoding="utf-8-sig")

        logger.info(
            "策略 %s: 交易数 %d, 总收益 %.2f%%, 最大回撤 %.2f%%",
            alias,
            int(summary.get("trade_count", 0)),
            float(summary.get("total_return", 0.0)) * 100,
            float(summary.get("max_drawdown", 0.0)) * 100,
        )
        logger.info("  日度明细: %s", daily_fp)
        logger.info("  交易明细: %s", trades_fp)

        summary_rows.append(summary)
        all_results.append(
            {
                "alias": alias,
                "class": cls_name,
                "config": cfg,
                "summary": summary,
                "daily_file": str(daily_fp),
                "trades_file": str(trades_fp),
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values(by="total_return", ascending=False, na_position="last")
    summary_fp = out_dir / f"backtest_{ts_tag}_summary.csv"
    summary_df.to_csv(summary_fp, index=False, encoding="utf-8-sig")

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": str(start_date.date()),
        "end_date": str(end_date.date()),
        "calendar_days": len(calendar),
        "stock_count": len(data),
        "exit_mode": args.exit_mode,
        "hold_days": args.hold_days,
        "max_hold_days": args.max_hold_days,
        "entry_price": args.entry_price,
        "stop_buffer_pct": args.stop_buffer_pct,
        "n_structure_lookback": args.n_structure_lookback,
        "big_bull_body_pct": args.big_bull_body_pct,
        "min_position_ratio_for_halve": args.min_position_ratio_for_halve,
        "benchmark_code": args.benchmark_code.strip() or None,
        "results": all_results,
    }
    json_fp = out_dir / f"backtest_{ts_tag}.json"
    with json_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info("回测完成")
    logger.info("Summary: %s", summary_fp)
    logger.info("JSON: %s", json_fp)


if __name__ == "__main__":
    main()

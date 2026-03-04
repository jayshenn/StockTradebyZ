from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import random
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Callable, Dict, Optional

import pandas as pd
import tushare as ts
from tushare.pro.client import DataApi
from tqdm import tqdm


LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "fetch_aux.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("fetch_aux_data")


def load_env_file(env_file: Path) -> None:
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def parse_yyyymmdd(value: str) -> str:
    if value.lower() == "today":
        return dt.date.today().strftime("%Y%m%d")
    _ = dt.datetime.strptime(value, "%Y%m%d")
    return value


def ensure_trade_dates(data_dir: Path, start: str, end: str, anchor_code: str = "000001") -> list[str]:
    anchor_fp = data_dir / f"{anchor_code}.csv"
    if not anchor_fp.exists():
        raise FileNotFoundError(f"未找到锚点文件: {anchor_fp}")

    df = pd.read_csv(anchor_fp, usecols=["date"])
    if df.empty:
        raise ValueError(f"{anchor_fp} 为空，无法提取交易日")

    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    start_ts = pd.to_datetime(start, format="%Y%m%d")
    end_ts = pd.to_datetime(end, format="%Y%m%d")
    in_range = dates[(dates >= start_ts) & (dates <= end_ts)].sort_values()
    out = [x.strftime("%Y%m%d") for x in in_range]
    if not out:
        raise ValueError(f"在 {start}~{end} 区间内没有交易日")
    return out


class MinuteRateLimiter:
    def __init__(self, max_calls: int, window_seconds: float = 60.0) -> None:
        if max_calls < 1:
            raise ValueError("max_calls 必须 >= 1")
        self.max_calls = int(max_calls)
        self.window_seconds = float(window_seconds)
        self._calls: deque[float] = deque()
        self._lock = Lock()

    def acquire(self) -> None:
        while True:
            wait_s = 0.0
            with self._lock:
                now = time.monotonic()
                cutoff = now - self.window_seconds
                while self._calls and self._calls[0] <= cutoff:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                wait_s = self._calls[0] + self.window_seconds - now
            if wait_s > 0:
                time.sleep(wait_s)


def _looks_like_transient_network(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    patterns = (
        "name resolution",
        "failed to resolve",
        "nodename nor servname",
        "newconnectionerror",
        "connection aborted",
        "connection reset",
        "read timed out",
        "connect timeout",
        "max retries exceeded",
    )
    return any(pat in msg for pat in patterns)


def _looks_like_permission(exc: Exception) -> bool:
    msg = str(exc)
    return "没有接口访问权限" in msg


def _looks_like_hourly_limit(exc: Exception) -> bool:
    msg = str(exc)
    return "每小时最多访问该接口" in msg


class EndpointHardLimitError(RuntimeError):
    pass


@dataclass(frozen=True)
class EndpointSpec:
    name: str
    fields: str
    method: str
    extra_params: Dict[str, str]


ENDPOINTS: list[EndpointSpec] = [
    EndpointSpec(
        name="daily_basic",
        method="daily_basic",
        fields="ts_code,trade_date,turnover_rate_f,volume_ratio,circ_mv",
        extra_params={},
    ),
    EndpointSpec(
        name="moneyflow",
        method="moneyflow",
        fields=(
            "ts_code,trade_date,net_mf_amount,"
            "buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount"
        ),
        extra_params={},
    ),
    EndpointSpec(
        name="cyq_perf",
        method="cyq_perf",
        fields="ts_code,trade_date,winner_rate,cost_50pct,cost_85pct,cost_95pct,weight_avg",
        extra_params={},
    ),
    EndpointSpec(
        name="sw_daily",
        method="sw_daily",
        fields="ts_code,trade_date,name,pct_change",
        extra_params={},
    ),
]


def call_with_retry(
    call: Callable[[], pd.DataFrame],
    *,
    limiter: MinuteRateLimiter,
    endpoint_name: str,
    trade_date: str,
    retries: int = 3,
) -> pd.DataFrame:
    for attempt in range(1, retries + 1):
        limiter.acquire()
        try:
            df = call()
            if df is None:
                return pd.DataFrame()
            return df
        except Exception as exc:
            if _looks_like_permission(exc):
                raise
            if _looks_like_hourly_limit(exc):
                raise EndpointHardLimitError(str(exc)) from exc
            wait_s = max(2, 8 * attempt)
            if _looks_like_transient_network(exc):
                logger.warning(
                    "%s %s 网络异常，第%d次失败，%d秒后重试：%s",
                    endpoint_name,
                    trade_date,
                    attempt,
                    wait_s,
                    exc,
                )
            else:
                logger.warning(
                    "%s %s 调用失败，第%d次失败，%d秒后重试：%s",
                    endpoint_name,
                    trade_date,
                    attempt,
                    wait_s,
                    exc,
                )
            time.sleep(wait_s + random.uniform(0, 1.5))
    raise RuntimeError(f"{endpoint_name} {trade_date} 连续失败 {retries} 次")


def fetch_static_tables(pro: DataApi, out_dir: Path, limiter: MinuteRateLimiter) -> None:
    static_dir = out_dir / "meta"
    static_dir.mkdir(parents=True, exist_ok=True)

    def _save(name: str, df: pd.DataFrame) -> None:
        fp = static_dir / f"{name}.csv"
        df.to_csv(fp, index=False)
        logger.info("静态表写入: %s (rows=%d)", fp, len(df))

    limiter.acquire()
    df_cls = pro.index_classify(src="SW2021", level="L1", fields="index_code,industry_name,level,src")
    if df_cls is None:
        df_cls = pd.DataFrame()
    _save("index_classify_sw2021_l1", df_cls)

    limiter.acquire()
    df_member = pro.index_member_all(fields="ts_code,l1_name,l2_name,l3_name,is_new")
    if df_member is None:
        df_member = pd.DataFrame()
    _save("index_member_all", df_member)


def fetch_daily_endpoint(
    pro: DataApi,
    *,
    spec: EndpointSpec,
    trade_dates: list[str],
    out_dir: Path,
    limiter: MinuteRateLimiter,
    force: bool,
) -> tuple[int, int, int]:
    endpoint_dir = out_dir / spec.name
    endpoint_dir.mkdir(parents=True, exist_ok=True)

    success = 0
    skipped = 0
    empty = 0
    disabled = False

    pbar = tqdm(trade_dates, desc=f"{spec.name}", leave=False)
    for trade_date in pbar:
        fp = endpoint_dir / f"{trade_date}.csv"
        if fp.exists() and not force:
            skipped += 1
            continue
        if disabled:
            skipped += 1
            continue

        def _call() -> pd.DataFrame:
            kwargs = dict(trade_date=trade_date, fields=spec.fields)
            kwargs.update(spec.extra_params)
            method = getattr(pro, spec.method)
            return method(**kwargs)

        try:
            df = call_with_retry(
                _call,
                limiter=limiter,
                endpoint_name=spec.name,
                trade_date=trade_date,
            )
        except EndpointHardLimitError as exc:
            logger.warning(
                "接口 %s 触发小时级硬限流，后续日期跳过。错误：%s",
                spec.name,
                exc,
            )
            disabled = True
            skipped += 1
            continue
        except Exception as exc:
            if _looks_like_permission(exc):
                logger.warning("接口 %s 无权限，后续日期跳过。错误：%s", spec.name, exc)
                disabled = True
                skipped += 1
                continue
            logger.error("接口 %s 在 %s 失败：%s", spec.name, trade_date, exc)
            skipped += 1
            continue

        if df.empty:
            empty += 1
            pd.DataFrame(columns=[x.strip() for x in spec.fields.split(",")]).to_csv(fp, index=False)
            continue

        if "trade_date" not in df.columns:
            df["trade_date"] = trade_date

        df.to_csv(fp, index=False)
        success += 1

    return success, skipped, empty


def main() -> None:
    parser = argparse.ArgumentParser(description="抓取搬砖增强所需的 Tushare 辅助数据（按交易日分片保存）")
    parser.add_argument("--start", default="20250101", help="起始日期 YYYYMMDD 或 today")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 today")
    parser.add_argument("--data-dir", type=Path, default=Path("./data"), help="本地行情目录（用于提取交易日）")
    parser.add_argument("--out-dir", type=Path, default=Path("./data_aux"), help="辅助数据输出目录")
    parser.add_argument("--env-file", type=Path, default=Path("./.env"), help=".env 路径")
    parser.add_argument("--anchor-code", default="000001", help="用于提取交易日的锚点代码")
    parser.add_argument("--max-requests-per-minute", type=int, default=40, help="全局限频")
    parser.add_argument("--force", action="store_true", help="强制覆盖已存在分片")
    args = parser.parse_args()

    start = parse_yyyymmdd(args.start)
    end = parse_yyyymmdd(args.end)
    if start > end:
        raise ValueError("start 不得晚于 end")

    load_env_file(args.env_file)
    os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
    os.environ["no_proxy"] = os.environ["NO_PROXY"]

    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise ValueError(f"未读取到 TUSHARE_TOKEN，请检查 {args.env_file}")

    trade_dates = ensure_trade_dates(args.data_dir, start, end, anchor_code=args.anchor_code)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "开始抓取辅助数据：%s -> %s, 交易日=%d, out=%s",
        start,
        end,
        len(trade_dates),
        args.out_dir.resolve(),
    )

    pro = ts.pro_api(token)
    limiter = MinuteRateLimiter(args.max_requests_per_minute)

    fetch_static_tables(pro, args.out_dir, limiter)

    summary_rows: list[dict[str, int | str]] = []
    for spec in ENDPOINTS:
        logger.info("开始抓取接口: %s", spec.name)
        success, skipped, empty = fetch_daily_endpoint(
            pro,
            spec=spec,
            trade_dates=trade_dates,
            out_dir=args.out_dir,
            limiter=limiter,
            force=args.force,
        )
        summary_rows.append(
            {
                "endpoint": spec.name,
                "trade_dates_total": len(trade_dates),
                "success_dates": success,
                "skipped_dates": skipped,
                "empty_dates": empty,
            }
        )
        logger.info(
            "接口完成: %s | success=%d, skipped=%d, empty=%d",
            spec.name,
            success,
            skipped,
            empty,
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_fp = args.out_dir / "fetch_summary.csv"
    summary_df.to_csv(summary_fp, index=False)
    logger.info("全部完成，汇总文件：%s", summary_fp.resolve())


if __name__ == "__main__":
    main()

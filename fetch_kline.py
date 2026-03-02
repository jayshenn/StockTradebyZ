from __future__ import annotations

import argparse
import datetime as dt
import logging
import random
import sys
import time
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import List, Optional
import os

import pandas as pd
import tushare as ts
from tqdm import tqdm

warnings.filterwarnings("ignore")


def _patch_pandas_fillna_method_compat() -> None:
    """
    兼容 pandas>=3 与 tushare 旧版内部调用：
    - 旧代码常用: obj.fillna(method="ffill"/"bfill", ...)
    - pandas>=3 已移除 method 参数
    这里做一次轻量 monkey patch，仅在 pandas 主版本>=3 时生效。
    """
    try:
        major = int(str(pd.__version__).split(".", 1)[0])
    except Exception:
        return
    if major < 3:
        return

    from pandas.core.generic import NDFrame

    if getattr(NDFrame.fillna, "__name__", "") == "_compat_fillna_with_method":
        return

    _orig_fillna = NDFrame.fillna

    def _compat_fillna_with_method(
        self,
        value=None,
        *,
        method=None,
        axis=None,
        inplace: bool = False,
        limit=None,
        **kwargs,
    ):
        if method is not None:
            m = str(method).lower()
            if m in ("ffill", "pad"):
                return self.ffill(axis=axis, inplace=inplace, limit=limit)
            if m in ("bfill", "backfill"):
                return self.bfill(axis=axis, inplace=inplace, limit=limit)
            raise ValueError(f"Unsupported fillna(method={method!r})")

        if value is None:
            raise TypeError("fillna() missing required argument: 'value'")
        return _orig_fillna(self, value=value, axis=axis, inplace=inplace, limit=limit, **kwargs)

    NDFrame.fillna = _compat_fillna_with_method
    logging.getLogger("fetch_from_stocklist").warning(
        "检测到 pandas %s，已启用 fillna(method=...) 兼容补丁（用于 tushare）。",
        pd.__version__,
    )


_patch_pandas_fillna_method_compat()

# --------------------------- 全局日志配置 --------------------------- #
LOG_FILE = Path("fetch.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("fetch_from_stocklist")

# --------------------------- 限流/封禁处理配置 --------------------------- #
COOLDOWN_SECS = 600
BAN_PATTERNS = (
    "访问频繁", "请稍后", "超过频率", "频繁访问",
    "too many requests", "429",
    "forbidden", "403",
    "max retries exceeded"
)

def _looks_like_ip_ban(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return any(pat in msg for pat in BAN_PATTERNS)

class RateLimitError(RuntimeError):
    """表示命中限流/封禁，需要长时间冷却后重试。"""
    pass

def _cool_sleep(base_seconds: int) -> None:
    jitter = random.uniform(0.9, 1.2)
    sleep_s = max(1, int(base_seconds * jitter))
    logger.warning("疑似被限流/封禁，进入冷却期 %d 秒...", sleep_s)
    time.sleep(sleep_s)

# --------------------------- 历史K线（Tushare 日线，固定qfq） --------------------------- #
pro: Optional[ts.pro_api] = None  # 模块级会话
KLINE_COLUMNS = ["date", "open", "close", "high", "low", "volume"]
request_limiter: Optional["MinuteRateLimiter"] = None


class MinuteRateLimiter:
    """全局分钟级限流器：控制所有线程总请求数 <= max_calls / 60秒。"""

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

def set_api(session) -> None:
    """由外部(比如GUI)注入已创建好的 ts.pro_api() 会话"""
    global pro
    pro = session


def load_env_file(env_file: Path) -> None:
    """
    从 .env 文件加载键值对到环境变量。
    - 支持 `KEY=VALUE` 与 `export KEY=VALUE`
    - 已存在于系统环境中的键不覆盖
    """
    if not env_file.exists():
        return

    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value
    

def _to_ts_code(code: str) -> str:
    """把6位code映射到标准 ts_code 后缀。"""
    code = str(code).zfill(6)
    if code.startswith(("60", "68", "9")):
        return f"{code}.SH"
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"
    else:
        return f"{code}.SZ"

def _get_kline_tushare(code: str, start: str, end: str) -> pd.DataFrame:
    if request_limiter is not None:
        request_limiter.acquire()
    ts_code = _to_ts_code(code)
    try:
        df = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            start_date=start,
            end_date=end,
            freq="D",
            api=pro
        )
    except Exception as e:
        if _looks_like_ip_ban(e):
            raise RateLimitError(str(e)) from e
        raise

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"trade_date": "date", "vol": "volume"})[KLINE_COLUMNS].copy()
    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "close", "high", "low", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)

def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df


def _load_existing_kline(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=KLINE_COLUMNS)
    try:
        old_df = pd.read_csv(csv_path, parse_dates=["date"])
    except Exception as e:
        logger.warning("%s 读取失败，将按空文件处理并重建：%s", csv_path.name, e)
        return pd.DataFrame(columns=KLINE_COLUMNS)

    if any(col not in old_df.columns for col in KLINE_COLUMNS):
        logger.warning("%s 列结构异常，将按空文件处理并重建。", csv_path.name)
        return pd.DataFrame(columns=KLINE_COLUMNS)

    old_df = old_df[KLINE_COLUMNS].copy()
    for c in ["open", "close", "high", "low", "volume"]:
        old_df[c] = pd.to_numeric(old_df[c], errors="coerce")

    try:
        return validate(old_df)
    except Exception as e:
        logger.warning("%s 历史数据校验失败，将按空文件处理并重建：%s", csv_path.name, e)
        return pd.DataFrame(columns=KLINE_COLUMNS)

# --------------------------- 读取 stocklist.csv & 过滤板块 --------------------------- #

def _filter_by_boards_stocklist(df: pd.DataFrame, exclude_boards: set[str]) -> pd.DataFrame:
    """
    exclude_boards 子集：{'gem','star','bj'}
    - gem  : 创业板 300/301（.SZ）
    - star : 科创板 688（.SH）
    - bj   : 北交所（.BJ 或 4/8 开头）
    """
    # 注意：stocklist 里的 symbol 可能被 pandas 读成 int（如 000400 -> 400），
    # 这里先统一补齐到 6 位，避免板块过滤误判。
    code = df["symbol"].astype(str).str.zfill(6)
    ts_code = df["ts_code"].astype(str).str.upper()
    mask = pd.Series(True, index=df.index)

    if "gem" in exclude_boards:
        mask &= ~code.str.startswith(("300", "301"))
    if "star" in exclude_boards:
        mask &= ~code.str.startswith(("688",))
    if "bj" in exclude_boards:
        mask &= ~(ts_code.str.endswith(".BJ") | code.str.startswith(("4", "8")))

    return df[mask].copy()

def load_codes_from_stocklist(stocklist_csv: Path, exclude_boards: set[str]) -> List[str]:
    df = pd.read_csv(stocklist_csv)    
    df = _filter_by_boards_stocklist(df, exclude_boards)
    codes = df["symbol"].astype(str).str.zfill(6).tolist()
    codes = list(dict.fromkeys(codes))  # 去重保持顺序
    logger.info("从 %s 读取到 %d 只股票（排除板块：%s）",
                stocklist_csv, len(codes), ",".join(sorted(exclude_boards)) or "无")
    return codes

# --------------------------- 单只抓取（增量更新，保留历史） --------------------------- #
def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
):
    csv_path = out_dir / f"{code}.csv"
    start_ts = pd.to_datetime(start, format="%Y%m%d", errors="raise")
    end_ts = pd.to_datetime(end, format="%Y%m%d", errors="raise")

    if start_ts > end_ts:
        logger.error("%s 日期区间无效：start=%s 大于 end=%s", code, start, end)
        return

    for attempt in range(1, 4):
        try:
            old_df = _load_existing_kline(csv_path)

            fetch_start_ts = start_ts
            if not old_df.empty:
                last_dt = old_df["date"].max().normalize()
                fetch_start_ts = max(start_ts, last_dt + pd.Timedelta(days=1))

            if fetch_start_ts > end_ts:
                logger.debug("%s 已是最新，无需新增（本地最后日期：%s）", code, old_df["date"].max().date() if not old_df.empty else "无")
                merged_df = old_df
            else:
                fetch_start = fetch_start_ts.strftime("%Y%m%d")
                new_df = _get_kline_tushare(code, fetch_start, end)
                if new_df.empty:
                    logger.debug("%s 在区间 %s~%s 无新增数据。", code, fetch_start, end)
                    merged_df = old_df
                else:
                    new_df = validate(new_df)
                    merged_df = pd.concat([old_df, new_df], ignore_index=True)

            if merged_df.empty:
                merged_df = pd.DataFrame(columns=KLINE_COLUMNS)
            merged_df = validate(merged_df)
            merged_df.to_csv(csv_path, index=False)
            break
        except Exception as e:
            if _looks_like_ip_ban(e):
                logger.error(f"{code} 第 {attempt} 次抓取疑似被封禁，沉睡 {COOLDOWN_SECS} 秒")
                _cool_sleep(COOLDOWN_SECS)
            else:
                silent_seconds = 15 * attempt
                logger.info(f"{code} 第 {attempt} 次抓取失败，{silent_seconds} 秒后重试：{e}")
                time.sleep(silent_seconds)
    else:
        logger.error("%s 三次抓取均失败，已跳过！", code)

# --------------------------- 主入口 --------------------------- #
def main():
    parser = argparse.ArgumentParser(description="从 stocklist.csv 读取股票池并用 Tushare 抓取日线K线（固定qfq，增量更新）")
    # 抓取范围
    parser.add_argument("--start", default="20190101", help="起始日期 YYYYMMDD 或 'today'")
    parser.add_argument("--end", default="today", help="结束日期 YYYYMMDD 或 'today'")
    # 股票清单与板块过滤
    parser.add_argument("--stocklist", type=Path, default=Path("./stocklist.csv"), help="股票清单CSV路径（需含 ts_code 或 symbol）")
    parser.add_argument(
        "--exclude-boards",
        nargs="*",
        default=[],
        choices=["gem", "star", "bj"],
        help="排除板块，可多选：gem(创业板300/301) star(科创板688) bj(北交所.BJ/4/8)"
    )
    parser.add_argument("--env-file", type=Path, default=Path("./.env"), help=".env 文件路径（默认 ./.env）")
    # 其它
    parser.add_argument("--out", default="./data", help="输出目录")
    parser.add_argument("--workers", type=int, default=6, help="并发线程数")
    parser.add_argument("--max-requests-per-minute", type=int, default=50, help="全局请求限频（每分钟总请求数）")
    args = parser.parse_args()

    # ---------- Tushare Token ---------- #
    load_env_file(args.env_file)
    os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    ts_token = os.environ.get("TUSHARE_TOKEN")
    if not ts_token:
        raise ValueError(
            f"未读取到 TUSHARE_TOKEN。请在 {args.env_file} 中配置，例如：TUSHARE_TOKEN=你的token"
        )
    # 优先使用直传 token，避免 ts.set_token 写入 ~/tk.csv 带来的环境依赖
    global pro
    try:
        pro = ts.pro_api(ts_token)
    except TypeError:
        # 兼容极老版本 tushare
        ts.set_token(ts_token)
        pro = ts.pro_api()
    global request_limiter
    request_limiter = MinuteRateLimiter(args.max_requests_per_minute)

    # ---------- 日期解析 ---------- #
    start = dt.date.today().strftime("%Y%m%d") if str(args.start).lower() == "today" else args.start
    end = dt.date.today().strftime("%Y%m%d") if str(args.end).lower() == "today" else args.end

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 从 stocklist.csv 读取股票池 ---------- #
    exclude_boards = set(args.exclude_boards or [])
    codes = load_codes_from_stocklist(args.stocklist, exclude_boards)

    if not codes:
        logger.error("stocklist 为空或被过滤后无代码，请检查。")
        sys.exit(1)

    logger.info(
        "开始抓取 %d 支股票 | 数据源:Tushare(日线,qfq) | 模式:增量更新 | 日期:%s → %s | 排除:%s | 限频:%d次/分钟",
        len(codes), start, end, ",".join(sorted(exclude_boards)) or "无", args.max_requests_per_minute,
    )

    # ---------- 多线程抓取（增量更新） ---------- #
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(
                fetch_one,
                code,
                start,
                end,
                out_dir,
            )
            for code in codes
        ]
        for _ in tqdm(as_completed(futures), total=len(futures), desc="下载进度"):
            pass

    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())

if __name__ == "__main__":
    main()

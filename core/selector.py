from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Any

from scipy.signal import find_peaks
import numpy as np
import pandas as pd

# --------------------------- 通用指标 --------------------------- #

def compute_kdj(df: pd.DataFrame, n: int = 9) -> pd.DataFrame:
    if df.empty:
        return df.assign(K=np.nan, D=np.nan, J=np.nan)

    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_n = df["high"].rolling(window=n, min_periods=1).max()
    rsv = (df["close"] - low_n) / (high_n - low_n + 1e-9) * 100

    K = np.zeros_like(rsv, dtype=float)
    D = np.zeros_like(rsv, dtype=float)
    for i in range(len(df)):
        if i == 0:
            K[i] = D[i] = 50.0
        else:
            K[i] = 2 / 3 * K[i - 1] + 1 / 3 * rsv.iloc[i]
            D[i] = 2 / 3 * D[i - 1] + 1 / 3 * K[i]
    J = 3 * K - 2 * D
    return df.assign(K=K, D=D, J=J)


def compute_bbi(df: pd.DataFrame) -> pd.Series:
    ma3 = df["close"].rolling(3).mean()
    ma6 = df["close"].rolling(6).mean()
    ma12 = df["close"].rolling(12).mean()
    ma24 = df["close"].rolling(24).mean()
    return (ma3 + ma6 + ma12 + ma24) / 4


def compute_rsv(
    df: pd.DataFrame,
    n: int,
) -> pd.Series:
    """
    按公式：RSV(N) = 100 × (C - LLV(L,N)) ÷ (HHV(C,N) - LLV(L,N))
    - C 用收盘价最高值 (HHV of close)
    - L 用最低价最低值 (LLV of low)
    """
    low_n = df["low"].rolling(window=n, min_periods=1).min()
    high_close_n = df["close"].rolling(window=n, min_periods=1).max()
    rsv = (df["close"] - low_n) / (high_close_n - low_n + 1e-9) * 100.0
    return rsv


def compute_dif(df: pd.DataFrame, fast: int = 12, slow: int = 26) -> pd.Series:
    """计算 MACD 指标中的 DIF (EMA fast - EMA slow)。"""
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    return ema_fast - ema_slow


def compute_tdx_sma(series: pd.Series, n: int, m: int = 1) -> pd.Series:
    """通达信 SMA(X,N,M): Y=(M*X+(N-M)*Y')/N。"""
    if n < 1:
        raise ValueError("n 必须 >= 1")
    if m < 1 or m > n:
        raise ValueError("m 必须位于 [1, n]")

    x = pd.to_numeric(series, errors="coerce").astype(float)
    out = np.full(len(x), np.nan, dtype=float)
    last = np.nan
    for i, v in enumerate(x.to_numpy()):
        if not np.isfinite(v):
            continue
        if i == 0 or not np.isfinite(last):
            y = v
        else:
            y = (m * v + (n - m) * last) / n
        out[i] = y
        last = y
    return pd.Series(out, index=series.index)


def compute_brick_turn_signal(
    df: pd.DataFrame,
    *,
    brick_window: int = 4,
    var2_sma_n: int = 4,
    var4_sma_n: int = 6,
    var5_sma_n: int = 6,
    brick_threshold: float = 4.0,
) -> pd.DataFrame:
    """
    复刻公式：
    VAR1A~VAR6A, 砖型图, AA, CC, XG。
    """
    if brick_window < 1:
        raise ValueError("brick_window 必须 >= 1")

    hist = df.copy()
    close = hist["close"].astype(float)
    high_n = hist["high"].astype(float).rolling(window=brick_window, min_periods=1).max()
    low_n = hist["low"].astype(float).rolling(window=brick_window, min_periods=1).min()
    denom = (high_n - low_n).replace(0.0, np.nan)

    var1a = (high_n - close) / denom * 100.0 - 90.0
    var2a = compute_tdx_sma(var1a, n=var2_sma_n, m=1) + 100.0
    var3a = (close - low_n) / denom * 100.0
    var4a = compute_tdx_sma(var3a, n=var4_sma_n, m=1)
    var5a = compute_tdx_sma(var4a, n=var5_sma_n, m=1) + 100.0
    var6a = var5a - var2a

    brick = pd.Series(0.0, index=hist.index, dtype=float)
    valid = var6a > brick_threshold
    brick.loc[valid] = (var6a.loc[valid] - brick_threshold).astype(float)
    brick = brick.fillna(0.0)

    prev_brick = brick.shift(1, fill_value=float(brick.iloc[0]))
    aa = (prev_brick < brick).astype(bool)                          # REF(砖型图,1) < 砖型图
    bb = (prev_brick > brick).astype(bool)                          # REF(砖型图,1) > 砖型图
    aa_prev = aa.shift(1, fill_value=False).astype(bool)
    cc = ((~aa_prev) & aa).astype(bool)                             # REF(AA,1)=0 && AA=1
    xg = cc.astype(int)                                             # XG=CC>0

    return pd.DataFrame(
        {
            "brick": brick,
            "aa": aa,
            "bb": bb,
            "cc": cc,
            "xg": xg,
        },
        index=hist.index,
    )


def bbi_deriv_uptrend(
    bbi: pd.Series,
    *,
    min_window: int,
    max_window: int | None = None,
    q_threshold: float = 0.0,
) -> bool:
    """
    判断 BBI 是否“整体上升”。

    令最新交易日为 T，在区间 [T-w+1, T]（w 自适应，w ≥ min_window 且 ≤ max_window）
    内，先将 BBI 归一化：BBI_norm(t) = BBI(t) / BBI(T-w+1)。

    再计算一阶差分 Δ(t) = BBI_norm(t) - BBI_norm(t-1)。  
    若 Δ(t) 的前 q_threshold 分位数 ≥ 0，则认为该窗口通过；只要存在
    **最长** 满足条件的窗口即可返回 True。q_threshold=0 时退化为
    “全程单调不降”（旧版行为）。

    Parameters
    ----------
    bbi : pd.Series
        BBI 序列（最新值在最后一位）。
    min_window : int
        检测窗口的最小长度。
    max_window : int | None
        检测窗口的最大长度；None 表示不设上限。
    q_threshold : float, default 0.0
        允许一阶差分为负的比例（0 ≤ q_threshold ≤ 1）。
    """
    if not 0.0 <= q_threshold <= 1.0:
        raise ValueError("q_threshold 必须位于 [0, 1] 区间内")

    bbi = bbi.dropna()
    if len(bbi) < min_window:
        return False

    longest = min(len(bbi), max_window or len(bbi))

    # 自最长窗口向下搜索，找到任一满足条件的区间即通过
    for w in range(longest, min_window - 1, -1):
        seg = bbi.iloc[-w:]                # 区间 [T-w+1, T]
        norm = seg / seg.iloc[0]           # 归一化
        diffs = np.diff(norm.values)       # 一阶差分
        if np.quantile(diffs, q_threshold) >= 0:
            return True
    return False


def _find_peaks(
    df: pd.DataFrame,
    *,
    column: str = "high",
    distance: Optional[int] = None,
    prominence: Optional[float] = None,
    height: Optional[float] = None,
    width: Optional[float] = None,
    rel_height: float = 0.5,
    **kwargs: Any,
) -> pd.DataFrame:
    
    if column not in df.columns:
        raise KeyError(f"'{column}' not found in DataFrame columns: {list(df.columns)}")

    y = df[column].to_numpy()

    indices, props = find_peaks(
        y,
        distance=distance,
        prominence=prominence,
        height=height,
        width=width,
        rel_height=rel_height,
        **kwargs,
    )

    peaks_df = df.iloc[indices].copy()
    peaks_df["is_peak"] = True

    # Flatten SciPy arrays into columns (only those with same length as indices)
    for key, arr in props.items():
        if isinstance(arr, (list, np.ndarray)) and len(arr) == len(indices):
            peaks_df[f"peak_{key}"] = arr

    return peaks_df

def last_valid_ma_cross_up(
    close: pd.Series,
    ma: pd.Series,
    lookback_n: int | None = None,
) -> Optional[int]:
    """
    查找“有效上穿 MA”的最后一个交易日 T（close[T-1] < ma[T-1] 且 close[T] ≥ ma[T]）。
    - 返回的是 **整数位置**（iloc 用）。
    - lookback_n: 仅在最近 N 根内查找；None 则全历史。
    """
    n = len(close)
    start = 1  # 至少要从 1 起，因为要看 T-1
    if lookback_n is not None:
        start = max(start, n - lookback_n)

    # 自后向前找最后一次有效上穿
    for i in range(n - 1, start - 1, -1):
        if i - 1 < 0:
            continue
        c_prev, c_now = close.iloc[i - 1], close.iloc[i]
        m_prev, m_now = ma.iloc[i - 1], ma.iloc[i]
        if pd.notna(c_prev) and pd.notna(c_now) and pd.notna(m_prev) and pd.notna(m_now):
            if c_prev < m_prev and c_now >= m_now:
                return i
    return None


def compute_zx_lines(
    df: pd.DataFrame,
    m1: int = 14, m2: int = 28, m3: int = 57, m4: int = 114
) -> tuple[pd.Series, pd.Series]:
    """返回 (ZXDQ, ZXDKX)
    ZXDQ = EMA(EMA(C,10),10)
    ZXDKX = (MA(C,14)+MA(C,28)+MA(C,57)+MA(C,114))/4
    """
    close = df["close"].astype(float)
    zxdq = close.ewm(span=10, adjust=False).mean().ewm(span=10, adjust=False).mean()

    ma1 = close.rolling(window=m1, min_periods=m1).mean()
    ma2 = close.rolling(window=m2, min_periods=m2).mean()
    ma3 = close.rolling(window=m3, min_periods=m3).mean()
    ma4 = close.rolling(window=m4, min_periods=m4).mean()
    zxdkx = (ma1 + ma2 + ma3 + ma4) / 4.0
    return zxdq, zxdkx


def passes_day_constraints_today(df: pd.DataFrame, pct_limit: float = 0.02, amp_limit: float = 0.07) -> bool:
    """
    所有战法的统一当日过滤：
    1) 当前交易日相较于前一日涨跌幅 < pct_limit（绝对值）
    2) 当日振幅（High-Low 相对 Low） < amp_limit
    """
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    close_today = float(last["close"])
    close_yest = float(prev["close"])
    high_today = float(last["high"])
    low_today  = float(last["low"])
    if close_yest <= 0 or low_today <= 0:
        return False
    pct_chg = abs(close_today / close_yest - 1.0)
    amplitude = (high_today - low_today) / low_today
    return (pct_chg < pct_limit) and (amplitude < amp_limit)


def zx_condition_at_positions(
    df: pd.DataFrame,
    *,
    m1: int = 14,
    m2: int = 28,
    m3: int = 57,
    m4: int = 114,
    require_close_gt_long: bool = True,
    require_short_gt_long: bool = True,
    require_low_gt_long: bool = False,
    max_close_to_short_mult: float | None = None,
    max_low_to_short_mult: float | None = None,
    pos: int | None = None,
) -> bool:
    """
    在指定位置 pos（iloc 位置；None 表示当日）检查知行条件：
      - 白线：EMA(EMA(C,10),10)
      - 黄线：(MA(C,m1)+MA(C,m2)+MA(C,m3)+MA(C,m4))/4
      - 收盘 > 长期线（可选）
      - 短期线 > 长期线（可选）
      - 最低价 > 长期线（可选）
      - 收盘 <= 短期线 * max_close_to_short_mult（可选）
      - 最低价 <= 短期线 * max_low_to_short_mult（可选）
    注：长期线需满样本；若为 NaN 直接返回 False。
    """
    if df.empty:
        return False
    zxdq, zxdkx = compute_zx_lines(df, m1=m1, m2=m2, m3=m3, m4=m4)
    if pos is None:
        pos = len(df) - 1

    if pos < 0 or pos >= len(df):
        return False

    s = float(zxdq.iloc[pos])
    l = float(zxdkx.iloc[pos]) if pd.notna(zxdkx.iloc[pos]) else float("nan")
    c = float(df["close"].iloc[pos])
    lo = float(df["low"].iloc[pos])

    if not np.isfinite(l) or not np.isfinite(s):
        return False

    if require_close_gt_long and not (c > l):
        return False
    if require_short_gt_long and not (s > l):
        return False
    if require_low_gt_long and not (np.isfinite(lo) and lo > l):
        return False
    if max_close_to_short_mult is not None and not (c <= s * max_close_to_short_mult):
        return False
    if max_low_to_short_mult is not None and not (lo <= s * max_low_to_short_mult):
        return False
    return True

# --------------------------- Selector 类 --------------------------- #
class BBIKDJSelector:
    """
    自适应 *BBI(导数)* + *KDJ* 选股器
        • BBI: 允许 bbi_q_threshold 比例的回撤
        • KDJ: J < threshold ；或位于历史 J 的 j_q_threshold 分位及以下
        • MACD: DIF > 0
        • 收盘价波动幅度 ≤ price_range_pct
    """

    def __init__(
        self,
        j_threshold: float = -5,
        bbi_min_window: int = 90,
        max_window: int = 90,
        price_range_pct: float = 100.0,
        bbi_q_threshold: float = 0.05,
        j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.bbi_min_window = bbi_min_window
        self.max_window = max_window
        self.price_range_pct = price_range_pct
        self.bbi_q_threshold = bbi_q_threshold  # ← 原 q_threshold
        self.j_q_threshold = j_q_threshold      # ← 新增

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)
        
        if not passes_day_constraints_today(hist):
            return False

        # 0. 收盘价波动幅度约束（最近 max_window 根 K 线）
        win = hist.tail(self.max_window)
        high, low = win["close"].max(), win["close"].min()
        if low <= 0 or (high / low - 1) > self.price_range_pct:           
            return False

        # 1. BBI 上升（允许部分回撤）
        if not bbi_deriv_uptrend(
            hist["BBI"],
            min_window=self.bbi_min_window,
            max_window=self.max_window,
            q_threshold=self.bbi_q_threshold,
        ):            
            return False

        # 2. KDJ 过滤 —— 双重条件
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])

        # 最近 max_window 根 K 线的 J 分位
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))

        if not (j_today < self.j_threshold or j_today <= j_quantile):
            
            return False
        
        # —— 2.5 60日均线条件（使用通用函数）
        hist["MA60"] = hist["close"].rolling(window=60, min_periods=1).mean()

        # 当前必须在 MA60 上方（保持原条件）
        if hist["close"].iloc[-1] < hist["MA60"].iloc[-1]:
            return False

        # 寻找最近一次“有效上穿 MA60”的 T（使用 max_window 作为回看长度，避免过旧）
        t_pos = last_valid_ma_cross_up(hist["close"], hist["MA60"], lookback_n=self.max_window)
        if t_pos is None:
            return False        

        # 3. MACD：DIF > 0
        hist["DIF"] = compute_dif(hist)
        if hist["DIF"].iloc[-1] <= 0:
            return False
       
        # 4. 当日：收盘>长期线 且 短期线>长期线
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
        self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 额外预留 20 根 K 线缓冲
            hist = hist.tail(self.max_window + 20)
            if self._passes_filters(hist):
                picks.append(code)
        return picks
    
    
class SuperB1Selector:
    """SuperB1 选股器

    过滤逻辑概览
    ----------------
    1. **历史匹配 (t_m)** — 在 *lookback_n* 个交易日窗口内，至少存在一日
       满足 :class:`BBIKDJSelector`。

    2. **盘整区间** — 区间 ``[t_m, date-1]`` 收盘价波动率不超过 ``close_vol_pct``。

    3. **当日下跌** — ``(close_{date-1} - close_date) / close_{date-1}``
       ≥ ``price_drop_pct``。

    4. **J 值极低** — ``J < j_threshold`` *或* 位于历史 ``j_q_threshold`` 分位。
    """

    # ---------------------------------------------------------------------
    # 构造函数
    # ---------------------------------------------------------------------
    def __init__(
        self,
        *,
        lookback_n: int = 60,
        close_vol_pct: float = 0.05,
        price_drop_pct: float = 0.03,
        j_threshold: float = -5,
        j_q_threshold: float = 0.10,
        # ↓↓↓ 新增：嵌套 BBIKDJSelector 配置
        B1_params: Optional[Dict[str, Any]] = None        
    ) -> None:        
        # ---------- 参数合法性检查 ----------
        if lookback_n < 2:
            raise ValueError("lookback_n 应 ≥ 2")
        if not (0 < close_vol_pct < 1):
            raise ValueError("close_vol_pct 应位于 (0, 1) 区间")
        if not (0 < price_drop_pct < 1):
            raise ValueError("price_drop_pct 应位于 (0, 1) 区间")
        if not (0 <= j_q_threshold <= 1):
            raise ValueError("j_q_threshold 应位于 [0, 1] 区间")
        if B1_params is None:
            raise ValueError("bbi_params没有给出")

        # ---------- 基本参数 ----------
        self.lookback_n = lookback_n
        self.close_vol_pct = close_vol_pct
        self.price_drop_pct = price_drop_pct
        self.j_threshold = j_threshold
        self.j_q_threshold = j_q_threshold

        # ---------- 内部 BBIKDJSelector ----------
        self.bbi_selector = BBIKDJSelector(**(B1_params or {}))

        # 为保证给 BBIKDJSelector 提供足够历史，预留额外缓冲
        self._extra_for_bbi = self.bbi_selector.max_window + 20

    # 单支股票过滤核心
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if len(hist) < 2:
            return False

        # —— 新增：所有战法统一当日过滤
        if not passes_day_constraints_today(hist):
            return False

        # ---------- Step-0: 数据量判断 ----------
        if len(hist) < self.lookback_n + self._extra_for_bbi:
            return False

        # ---------- Step-1: 搜索满足 BBIKDJ 的 t_m ----------
        lb_hist = hist.tail(self.lookback_n + 1)  # +1 以排除自身
        tm_idx: int | None = None
        for idx in lb_hist.index[:-1]:
            if self.bbi_selector._passes_filters(hist.loc[:idx]):
                tm_idx = idx
                stable_seg = hist.loc[tm_idx : hist.index[-2], "close"]
                if len(stable_seg) < 3:
                    tm_idx = None
                    break
                high, low = stable_seg.max(), stable_seg.min()
                if low <= 0 or (high / low - 1) > self.close_vol_pct:
                    tm_idx = None
                    continue
                else:
                    break
        if tm_idx is None:
            return False

        # —— 新增：在 t_m 当日检查【收盘>长期线 且 短期线>长期线】
        tm_pos = hist.index.get_loc(tm_idx)
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=tm_pos):
            return False

        # ---------- Step-3: 当日相对前一日跌幅 ----------
        close_today, close_prev = hist["close"].iloc[-1], hist["close"].iloc[-2]
        if close_prev <= 0 or (close_prev - close_today) / close_prev < self.price_drop_pct:
            return False

        # ---------- Step-4: J 值极低 ----------
        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        j_window = kdj["J"].iloc[-self.lookback_n:].dropna()
        j_q_val = float(j_window.quantile(self.j_q_threshold)) if not j_window.empty else np.nan
        if not (j_today < self.j_threshold or j_today <= j_q_val):
            return False

        # —— 当日仅要求【短期线>长期线】
        if not zx_condition_at_positions(hist, require_close_gt_long=False, require_short_gt_long=True, pos=None):
            return False

        return True

    # 批量选股接口
    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:        
        picks: List[str] = []
        min_len = self.lookback_n + self._extra_for_bbi

        for code, df in data.items():
            hist = df[df["date"] <= date].tail(min_len)
            if len(hist) < min_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class PeakKDJSelector:
    """
    Peaks + KDJ 选股器    
    """

    def __init__(
        self,
        j_threshold: float = -5,
        max_window: int = 90,
        fluc_threshold: float = 0.03,
        gap_threshold: float = 0.02,
        j_q_threshold: float = 0.10,
    ) -> None:
        self.j_threshold = j_threshold
        self.max_window = max_window
        self.fluc_threshold = fluc_threshold  # 当日↔peak_(t-n) 波动率上限
        self.gap_threshold = gap_threshold    # oc_prev 必须高于区间最低收盘价的比例
        self.j_q_threshold = j_q_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if hist.empty:
            return False
        
        if not passes_day_constraints_today(hist):
            return False

        hist = hist.copy().sort_values("date")
        hist["oc_max"] = hist[["open", "close"]].max(axis=1)

        # 1. 提取 peaks
        peaks_df = _find_peaks(
            hist,
            column="oc_max",
            distance=6,
            prominence=0.5,
        )
        
        # 至少两个峰      
        date_today = hist.iloc[-1]["date"]
        peaks_df = peaks_df[peaks_df["date"] < date_today]
        if len(peaks_df) < 2:               
            return False

        peak_t = peaks_df.iloc[-1]          # 最新一个峰
        peaks_list = peaks_df.reset_index(drop=True)
        oc_t = peak_t.oc_max
        total_peaks = len(peaks_list)

        # 2. 回溯寻找 peak_(t-n)
        target_peak = None        
        for idx in range(total_peaks - 2, -1, -1):
            peak_prev = peaks_list.loc[idx]
            oc_prev = peak_prev.oc_max
            if oc_t <= oc_prev:             # 要求 peak_t > peak_(t-n)
                continue

            # 只有当“总峰数 ≥ 3”时才检查区间内其他峰 oc_max
            if total_peaks >= 3 and idx < total_peaks - 2:
                inter_oc = peaks_list.loc[idx + 1 : total_peaks - 2, "oc_max"]
                if not (inter_oc < oc_prev).all():
                    continue

            # 新增： oc_prev 高于区间最低收盘价 gap_threshold
            date_prev = peak_prev.date
            mask = (hist["date"] > date_prev) & (hist["date"] < peak_t.date)
            min_close = hist.loc[mask, "close"].min()
            if pd.isna(min_close):
                continue                    # 区间无数据
            if oc_prev <= min_close * (1 + self.gap_threshold):
                continue

            target_peak = peak_prev
            
            break

        if target_peak is None:
            return False

        # 3. 当日收盘价波动率
        close_today = hist.iloc[-1]["close"]
        fluc_pct = abs(close_today - target_peak.close) / target_peak.close
        if fluc_pct > self.fluc_threshold:
            return False

        # 4. KDJ 过滤
        kdj = compute_kdj(hist)
        j_today = float(kdj.iloc[-1]["J"])
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_quantile = float(j_window.quantile(self.j_q_threshold))
        if not (j_today < self.j_threshold or j_today <= j_quantile):
            return False

        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    # ---------- 多股票批量 ---------- #
    def select(
        self,
        date: pd.Timestamp,
        data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            hist = hist.tail(self.max_window + 20)  # 额外缓冲
            if self._passes_filters(hist):
                picks.append(code)
        return picks
    

class BBIShortLongSelector:
    """
    BBI 上升 + 短/长期 RSV 条件 + DIF > 0 选股器
    """
    def __init__(
        self,
        n_short: int = 3,
        n_long: int = 21,
        m: int = 3,
        bbi_min_window: int = 90,
        max_window: int = 150,
        bbi_q_threshold: float = 0.05,
        upper_rsv_threshold: float = 75,
        lower_rsv_threshold: float = 25
    ) -> None:
        if m < 2:
            raise ValueError("m 必须 ≥ 2")
        self.n_short = n_short
        self.n_long = n_long
        self.m = m
        self.bbi_min_window = bbi_min_window
        self.max_window = max_window
        self.bbi_q_threshold = bbi_q_threshold
        self.upper_rsv_threshold = upper_rsv_threshold
        self.lower_rsv_threshold = lower_rsv_threshold

    # ---------- 单支股票过滤 ---------- #
    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        hist = hist.copy()
        hist["BBI"] = compute_bbi(hist)
        
        if not passes_day_constraints_today(hist):
            return False      

        # 1. BBI 上升（允许部分回撤）
        if not bbi_deriv_uptrend(
            hist["BBI"],
            min_window=self.bbi_min_window,
            max_window=self.max_window,
            q_threshold=self.bbi_q_threshold,
        ):
            return False

        # 2. 计算短/长期 RSV -----------------
        hist["RSV_short"] = compute_rsv(hist, self.n_short)
        hist["RSV_long"] = compute_rsv(hist, self.n_long)

        if len(hist) < self.m:
            return False                        # 数据不足

        win = hist.iloc[-self.m :]              # 最近 m 天
        long_ok = (win["RSV_long"] >= self.upper_rsv_threshold).all() # 长期 RSV 全 ≥ upper_rsv_threshold

        short_series = win["RSV_short"]

        # 条件：从最近 m 天的第一天起，存在某天 i 满足 RSV_short[i] >= upper，
        # 且在该天之后（j > i）存在某天 j 满足 RSV_short[j] < lower
        mask_upper = short_series >= self.upper_rsv_threshold
        mask_lower = short_series < self.lower_rsv_threshold

        has_upper_then_lower = False
        if mask_upper.any():
            upper_indices = np.where(mask_upper.to_numpy())[0]
            for i in upper_indices:
                # 只检查 i 之后的日子
                if i + 1 < len(short_series) and mask_lower.iloc[i + 1 :].any():
                    has_upper_then_lower = True
                    break
        
        end_ok = short_series.iloc[-1] >= self.upper_rsv_threshold

        if not (long_ok and has_upper_then_lower and end_ok):
            return False

        # 3. MACD：DIF > 0 -------------------
        hist["DIF"] = compute_dif(hist)
        if hist["DIF"].iloc[-1] <= 0:
            return False

        # 4. 新增：知行情形
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True


    # ---------- 多股票批量 ---------- #
    def select(
        self,
        date: pd.Timestamp,
        data: Dict[str, pd.DataFrame],
    ) -> List[str]:
        picks: List[str] = []
        for code, df in data.items():
            hist = df[df["date"] <= date]
            if hist.empty:
                continue
            # 预留足够长度：RSV 计算窗口 + BBI 检测窗口 + m
            need_len = (
                max(self.n_short, self.n_long)
                + self.bbi_min_window
                + self.m
            )
            hist = hist.tail(max(need_len, self.max_window))
            if self._passes_filters(hist):
                picks.append(code)
        return picks
    
    
class MA60CrossVolumeWaveSelector:
    """
    条件：
    1) 当日 J 绝对低或相对低（J < j_threshold 或 J ≤ 近 max_window 根 J 的 j_q_threshold 分位）
    2) 最近 lookback_n 内，存在一次“有效上穿 MA60”（t-1 收盘 < MA60, t 收盘 ≥ MA60）；
       且从该上穿日 T 到今天的“上涨波段”日均成交量 ≥ 上穿前等长窗口的日均成交量 * vol_multiple
       —— 上涨波段定义为 [T, today] 间的所有交易日（不做趋势单调性强约束，稳健且可复现）
    3) 近 ma60_slope_days（默认 5）个交易日的 MA60 回归斜率 > 0
    """
    def __init__(
        self,
        *,
        lookback_n: int = 60,
        vol_multiple: float = 1.5,
        j_threshold: float = -5.0,
        j_q_threshold: float = 0.10,
        ma60_slope_days: int = 5,
        max_window: int = 120,   # 用于计算 J 分位        
    ) -> None:
        if lookback_n < 2:
            raise ValueError("lookback_n 应 ≥ 2")
        if not (0.0 <= j_q_threshold <= 1.0):
            raise ValueError("j_q_threshold 应位于 [0,1]")
        if ma60_slope_days < 2:
            raise ValueError("ma60_slope_days 应 ≥ 2")
        self.lookback_n = lookback_n
        self.vol_multiple = vol_multiple
        self.j_threshold = j_threshold
        self.j_q_threshold = j_q_threshold
        self.ma60_slope_days = ma60_slope_days
        self.max_window = max_window        

    @staticmethod
    def _ma_slope_positive(series: pd.Series, days: int) -> bool:
        """对最近 days 个点做一阶线性回归，斜率 > 0 判为正"""
        seg = series.dropna().tail(days)
        if len(seg) < days:
            return False
        x = np.arange(len(seg), dtype=float)
        # 线性回归（最小二乘）：斜率 k
        k, _ = np.polyfit(x, seg.values.astype(float), 1)
        return bool(k > 0)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        """
        hist：按日期升序，最后一行是目标交易日
        需包含列：date, open, high, low, close, volume
        """
        if hist.empty:
            return False

        hist = hist.copy().sort_values("date")
        # 至少要有 60 日用于 MA60，再加 lookback/slope 的缓冲
        min_len = max(60 + self.lookback_n + self.ma60_slope_days, self.max_window + 5)
        if len(hist) < min_len:
            return False
        
        if not passes_day_constraints_today(hist):
            return False

        # --- 计算指标 ---
        kdj = compute_kdj(hist)
        j_today = float(kdj["J"].iloc[-1])
        j_window = kdj["J"].tail(self.max_window).dropna()
        if j_window.empty:
            return False
        j_q_val = float(j_window.quantile(self.j_q_threshold))

        # 1) 当日 J 绝对低或相对低
        if not (j_today < self.j_threshold or j_today <= j_q_val):
            return False

        # 2) MA60 及有效上穿（使用通用函数）
        hist["MA60"] = hist["close"].rolling(window=60, min_periods=1).mean()
        if hist["close"].iloc[-1] < hist["MA60"].iloc[-1]:
            return False

        t_pos = last_valid_ma_cross_up(hist["close"], hist["MA60"], lookback_n=self.lookback_n)
        if t_pos is None:
            return False

        # === [T, today] 内以 High 最大值的交易日为 Tmax ===
        seg_T_to_today = hist.iloc[t_pos:]
        if seg_T_to_today.empty:
            return False

        # 若并列最高，默认取“第一次”出现的那天；要“最后一次”可改见注释
        tmax_label = seg_T_to_today["high"].idxmax()
        int_pos_T   = t_pos
        int_pos_Tmax = hist.index.get_loc(tmax_label)

        if int_pos_Tmax < int_pos_T:
            return False

        # 上涨波段 [T, Tmax]（含端点）
        wave = hist.iloc[int_pos_T : int_pos_Tmax + 1]
        wave_len = len(wave)
        if wave_len < 3:
            return False

        # 等长前置窗口 [T - wave_len, T-1]
        pre_start_pos = max(0, int_pos_T - min(wave_len, 10))
        pre = hist.iloc[pre_start_pos:int_pos_T]
        if len(pre) < max(5, min(10, wave_len)):
            return False

        # 成交量均值对比
        wave_avg_vol = float(wave["volume"].replace(0, np.nan).dropna().mean())
        pre_avg_vol  = float(pre["volume"].replace(0, np.nan).dropna().mean())
        if not (np.isfinite(wave_avg_vol) and np.isfinite(pre_avg_vol) and pre_avg_vol > 0):
            return False

        if wave_avg_vol < self.vol_multiple * pre_avg_vol:
            return False

        # 3) MA60 斜率 > 0（保留原实现）
        if not self._ma_slope_positive(hist["MA60"], self.ma60_slope_days):
            return False
        
        if not zx_condition_at_positions(hist, require_close_gt_long=True, require_short_gt_long=True, pos=None):
            return False

        return True

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        # 给足 60 日均线与量能比较的历史长度
        need_len = max(60 + self.lookback_n + self.ma60_slope_days, self.max_window + 20)
        for code, df in data.items():
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < need_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)
        return picks

class BigBullishVolumeSelector:    

    def __init__(
        self,
        *,
        up_pct_threshold: float = 0.04,       # 长阳阈值：例如 0.04 表示涨幅>4%
        upper_wick_pct_max: float = 0.5,      # 上影线比例上限（口径由 wick_mode 决定）
        vol_lookback_n: int = 20,             # 放量比较的历史天数 n
        vol_multiple: float = 1.5,            # 放量倍数阈值
        min_history: int | None = None,       # 最少历史长度（默认自动 = vol_lookback_n + 2）
        require_bullish_close: bool = True,   # 可选：要求当日收阳（close >= open）
        ignore_zero_volume: bool = True,      # 计算均量时是否忽略 volume=0
        close_lt_zxdq_mult: float = 1.0       # 例如 1.0 表示 close < zxdq；1.02 表示 close < 1.02*zxdq        
    ) -> None:
        if up_pct_threshold <= 0:
            raise ValueError("up_pct_threshold 应 > 0")
        if upper_wick_pct_max < 0:
            raise ValueError("upper_wick_pct_max 应 >= 0")
        if vol_lookback_n < 1:
            raise ValueError("vol_lookback_n 应 >= 1")
        if vol_multiple <= 0:
            raise ValueError("vol_multiple 应 > 0")
        if close_lt_zxdq_mult <= 0:
            raise ValueError("close_lt_zxdq_mult 应 > 0")    

        self.up_pct_threshold = float(up_pct_threshold)
        self.upper_wick_pct_max = float(upper_wick_pct_max)
        self.vol_lookback_n = int(vol_lookback_n)
        self.vol_multiple = float(vol_multiple)
        self.require_bullish_close = bool(require_bullish_close)
        self.ignore_zero_volume = bool(ignore_zero_volume)
        self.close_lt_zxdq_mult = float(close_lt_zxdq_mult)
        self.eps = float(1e-12)        
        self.min_history = int(min_history) if min_history is not None else (self.vol_lookback_n + 2)
        

    @staticmethod
    def _to_float(x) -> float:
        try:
            return float(x)
        except Exception:
            return float("nan")

    def _upper_wick_pct(self, o: float, h: float, c: float) -> float:
        return (h - max(o, c)) / max(o, c)

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        if hist is None or hist.empty:
            return False

        hist = hist.sort_values("date").copy()

        if len(hist) < self.min_history:
            return False
        if len(hist) < (self.vol_lookback_n + 2):
            return False  # 至少需要：T、T-1、以及 T-1 往前 n 天

        today = hist.iloc[-1]
        prev  = hist.iloc[-2]

        oT = self._to_float(today.get("open"))
        hT = self._to_float(today.get("high"))
        lT = self._to_float(today.get("low"))
        cT = self._to_float(today.get("close"))
        vT = self._to_float(today.get("volume"))

        cP = self._to_float(prev.get("close"))

        # 基础合法性
        if not (np.isfinite(oT) and np.isfinite(hT) and np.isfinite(lT) and np.isfinite(cT) and np.isfinite(vT) and np.isfinite(cP)):
            return False
        if cP <= 0 or cT <= 0:
            return False
        if hT < max(oT, cT) or lT > min(oT, cT):
            # K线数据异常（不一定必需，但建议保持严谨）
            return False

        # (可选) 要求当日收阳
        if self.require_bullish_close and not (cT >= oT):
            return False

        # 1) 长阳：涨幅 > 阈值
        pct_chg = cT / cP - 1.0
        if pct_chg <= self.up_pct_threshold:
            return False

        # 2) 上影线百分比 < 阈值
        wick_pct = self._upper_wick_pct(oT, hT, cT)
        if not np.isfinite(wick_pct):
            return False
        if wick_pct >= self.upper_wick_pct_max:
            return False

        # 3) 放量：当日成交量 > 前 n 日均量 * 倍数
        vol_hist = hist["volume"].iloc[-(self.vol_lookback_n + 1):-1].astype(float)  # T-n ... T-1
        if self.ignore_zero_volume:
            vol_hist = vol_hist.replace(0, np.nan).dropna()

        if len(vol_hist) < max(3, int(self.vol_lookback_n * 0.6)):
            # 有效样本过少就不做判断（你也可以改成直接 False 或严格要求=vol_lookback_n）
            return False

        avg_vol = float(vol_hist.mean())
        if not (np.isfinite(avg_vol) and avg_vol > 0):
            return False

        if vT < self.vol_multiple * avg_vol:
            return False
        
        # 4) 偏离短线小于阈值
        try:
            zxdq, _ = compute_zx_lines(hist)
            zxdq_T = float(zxdq.iloc[-1])
        except Exception:
            zxdq_T = float("nan")

        if not np.isfinite(zxdq_T):
            return False
        else:
            if not (cT < zxdq_T * self.close_lt_zxdq_mult):
                return False

        return True

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        picks: List[str] = []
        need_len = max(self.min_history, self.vol_lookback_n + 2)

        for code, df in data.items():
            if df is None or df.empty:
                continue
            hist = df[df["date"] <= date].tail(need_len)
            if len(hist) < need_len:
                continue
            if self._passes_filters(hist):
                picks.append(code)

        return picks


class ZXBrickTurnSelector:
    """
    知行短期砖型图选股器（ZXBrick v3）。
    流水：
      universe -> core_signal -> hard_filter -> score -> rank/top_n -> daily_trade_cap
    """

    def __init__(
        self,
        *,
        brick_window: int = 4,
        var2_sma_n: int = 4,
        var4_sma_n: int = 6,
        var5_sma_n: int = 6,
        brick_threshold: float = 4.0,
        m1: int = 14,
        m2: int = 28,
        m3: int = 57,
        m4: int = 114,
        min_history: int = 140,
        max_window: int = 180,
        strong_red_ratio: float = 2 / 3,
        aux_data_dir: str | None = None,
        t_day_pct_min: float | None = None,
        t_day_pct_max: float | None = None,
        close_near_high_max: float | None = None,
        body_ratio_min: float | None = None,
        close_to_zxdq_max_mult: float | None = None,
        daily_basic_volume_ratio_min: float | None = None,
        daily_basic_volume_ratio_max: float | None = None,
        require_moneyflow_3d_net_inflow: bool = False,
        require_moneyflow_3d_large_net_inflow: bool = False,
        moneyflow_lookback_days: int = 3,
        require_sw_strength: bool = False,
        sw_strength_quantile_min: float = 0.5,
        require_sw_positive: bool = True,
        use_score_model: bool = False,
        top_n: int | None = None,
        daily_trade_cap: int | None = None,
        score_weights: Dict[str, float] | None = None,
        score_moneyflow_lookback_days: int = 3,
    ) -> None:
        if min_history < 20:
            raise ValueError("min_history 应 >= 20")
        if max_window < 20:
            raise ValueError("max_window 应 >= 20")
        if max_window < min_history:
            raise ValueError("max_window 应 >= min_history")
        if brick_threshold < 0:
            raise ValueError("brick_threshold 应 >= 0")
        if min(m1, m2, m3, m4) < 2:
            raise ValueError("m1~m4 应 >= 2")
        if strong_red_ratio < 0:
            raise ValueError("strong_red_ratio 应 >= 0")
        if moneyflow_lookback_days < 1:
            raise ValueError("moneyflow_lookback_days 应 >= 1")
        if score_moneyflow_lookback_days < 1:
            raise ValueError("score_moneyflow_lookback_days 应 >= 1")
        if not (0.0 <= sw_strength_quantile_min <= 1.0):
            raise ValueError("sw_strength_quantile_min 应位于 [0,1]")
        if (
            t_day_pct_min is not None
            and t_day_pct_max is not None
            and t_day_pct_min > t_day_pct_max
        ):
            raise ValueError("t_day_pct_min 不应大于 t_day_pct_max")
        if close_near_high_max is not None and close_near_high_max < 0:
            raise ValueError("close_near_high_max 应 >= 0")
        if body_ratio_min is not None and not (0 <= body_ratio_min <= 1):
            raise ValueError("body_ratio_min 应位于 [0,1]")
        if close_to_zxdq_max_mult is not None and close_to_zxdq_max_mult <= 0:
            raise ValueError("close_to_zxdq_max_mult 应 > 0")
        if (
            daily_basic_volume_ratio_min is not None
            and daily_basic_volume_ratio_max is not None
            and daily_basic_volume_ratio_min > daily_basic_volume_ratio_max
        ):
            raise ValueError("daily_basic_volume_ratio_min 不应大于 daily_basic_volume_ratio_max")
        if top_n is not None and top_n < 1:
            raise ValueError("top_n 应 >= 1 或 None")
        if daily_trade_cap is not None and daily_trade_cap < 1:
            raise ValueError("daily_trade_cap 应 >= 1 或 None")

        self.brick_window = int(brick_window)
        self.var2_sma_n = int(var2_sma_n)
        self.var4_sma_n = int(var4_sma_n)
        self.var5_sma_n = int(var5_sma_n)
        self.brick_threshold = float(brick_threshold)
        self.m1 = int(m1)
        self.m2 = int(m2)
        self.m3 = int(m3)
        self.m4 = int(m4)
        self.min_history = int(min_history)
        self.max_window = int(max_window)
        self.strong_red_ratio = float(strong_red_ratio)
        self.aux_data_dir = Path(aux_data_dir) if aux_data_dir else None

        self.t_day_pct_min = t_day_pct_min
        self.t_day_pct_max = t_day_pct_max
        self.close_near_high_max = close_near_high_max
        self.body_ratio_min = body_ratio_min
        self.close_to_zxdq_max_mult = close_to_zxdq_max_mult
        self.daily_basic_volume_ratio_min = daily_basic_volume_ratio_min
        self.daily_basic_volume_ratio_max = daily_basic_volume_ratio_max
        self.require_moneyflow_3d_net_inflow = bool(require_moneyflow_3d_net_inflow)
        self.require_moneyflow_3d_large_net_inflow = bool(require_moneyflow_3d_large_net_inflow)
        self.moneyflow_lookback_days = int(moneyflow_lookback_days)
        self.require_sw_strength = bool(require_sw_strength)
        self.sw_strength_quantile_min = float(sw_strength_quantile_min)
        self.require_sw_positive = bool(require_sw_positive)

        self.use_score_model = bool(use_score_model)
        self.top_n = int(top_n) if top_n is not None else None
        self.daily_trade_cap = int(daily_trade_cap) if daily_trade_cap is not None else None
        self.score_moneyflow_lookback_days = int(score_moneyflow_lookback_days)
        self.score_weights = self._normalize_score_weights(score_weights)

        self._lru_size = 32
        self._daily_basic_volume_ratio_cache: OrderedDict[str, Dict[str, float]] = OrderedDict()
        self._moneyflow_cache: OrderedDict[str, Dict[str, tuple[float, float]]] = OrderedDict()
        self._sw_daily_cache: OrderedDict[str, tuple[Dict[str, float], float]] = OrderedDict()
        self._code_to_industry_l1: Dict[str, str] = {}
        if self.aux_data_dir is not None:
            self._load_aux_meta()

    @staticmethod
    def _to_ts_code(code: str) -> str:
        c = str(code).zfill(6)
        if c.startswith(("60", "68", "9")):
            return f"{c}.SH"
        if c.startswith(("4", "8")):
            return f"{c}.BJ"
        return f"{c}.SZ"

    @staticmethod
    def _to_trade_date_str(value: pd.Timestamp) -> str:
        return pd.Timestamp(value).strftime("%Y%m%d")

    @staticmethod
    def _normalize_score_weights(score_weights: Dict[str, float] | None) -> Dict[str, float]:
        default = {
            "brick_strength": 0.35,
            "volume_ratio": 0.25,
            "moneyflow_strength": 0.20,
            "industry_strength": 0.20,
        }
        if not score_weights:
            return default

        key_map = {
            "brick": "brick_strength",
            "brick_strength": "brick_strength",
            "volume": "volume_ratio",
            "volume_ratio": "volume_ratio",
            "moneyflow": "moneyflow_strength",
            "moneyflow_strength": "moneyflow_strength",
            "industry": "industry_strength",
            "industry_strength": "industry_strength",
        }
        out = {k: 0.0 for k in default}
        for k, v in score_weights.items():
            canonical = key_map.get(str(k), None)
            if canonical is None:
                continue
            try:
                fv = float(v)
            except Exception:
                continue
            if np.isfinite(fv) and fv > 0:
                out[canonical] += fv
        total = float(sum(out.values()))
        if total <= 0:
            return default
        return {k: float(v / total) for k, v in out.items()}

    def _put_lru(self, cache: OrderedDict, key: str, value: Any) -> None:
        cache[key] = value
        cache.move_to_end(key, last=True)
        while len(cache) > self._lru_size:
            cache.popitem(last=False)

    def _load_aux_meta(self) -> None:
        if self.aux_data_dir is None:
            return
        fp = self.aux_data_dir / "meta" / "index_member_all.csv"
        if not fp.exists():
            return
        try:
            df = pd.read_csv(fp, usecols=["ts_code", "l1_name"])
        except Exception:
            return

        ts_codes = df["ts_code"].astype(str).str.upper()
        l1_names = df["l1_name"].astype(str)
        for ts_code, l1_name in zip(ts_codes, l1_names):
            if ts_code and l1_name and l1_name != "nan":
                self._code_to_industry_l1[ts_code] = l1_name

    def _get_daily_basic_volume_ratio(self, trade_date: str) -> Dict[str, float]:
        if trade_date in self._daily_basic_volume_ratio_cache:
            self._daily_basic_volume_ratio_cache.move_to_end(trade_date, last=True)
            return self._daily_basic_volume_ratio_cache[trade_date]

        out: Dict[str, float] = {}
        if self.aux_data_dir is not None:
            fp = self.aux_data_dir / "daily_basic" / f"{trade_date}.csv"
            if fp.exists():
                try:
                    df = pd.read_csv(fp, usecols=["ts_code", "volume_ratio"])
                    ts_codes = df["ts_code"].astype(str).str.upper()
                    volume_ratio = pd.to_numeric(df["volume_ratio"], errors="coerce")
                    for ts_code, vr in zip(ts_codes, volume_ratio):
                        if np.isfinite(vr):
                            out[ts_code] = float(vr)
                except Exception:
                    out = {}

        self._put_lru(self._daily_basic_volume_ratio_cache, trade_date, out)
        return out

    def _get_moneyflow_map(self, trade_date: str) -> Dict[str, tuple[float, float]]:
        if trade_date in self._moneyflow_cache:
            self._moneyflow_cache.move_to_end(trade_date, last=True)
            return self._moneyflow_cache[trade_date]

        out: Dict[str, tuple[float, float]] = {}
        if self.aux_data_dir is not None:
            fp = self.aux_data_dir / "moneyflow" / f"{trade_date}.csv"
            if fp.exists():
                try:
                    df = pd.read_csv(
                        fp,
                        usecols=[
                            "ts_code",
                            "net_mf_amount",
                            "buy_lg_amount",
                            "sell_lg_amount",
                            "buy_elg_amount",
                            "sell_elg_amount",
                        ],
                    )
                    ts_codes = df["ts_code"].astype(str).str.upper()
                    net_mf = pd.to_numeric(df["net_mf_amount"], errors="coerce")
                    buy_lg = pd.to_numeric(df["buy_lg_amount"], errors="coerce")
                    sell_lg = pd.to_numeric(df["sell_lg_amount"], errors="coerce")
                    buy_elg = pd.to_numeric(df["buy_elg_amount"], errors="coerce")
                    sell_elg = pd.to_numeric(df["sell_elg_amount"], errors="coerce")

                    for ts_code, n, bl, sl, be, se in zip(ts_codes, net_mf, buy_lg, sell_lg, buy_elg, sell_elg):
                        if not np.isfinite(n):
                            continue
                        large_net = 0.0
                        if np.isfinite(bl):
                            large_net += float(bl)
                        if np.isfinite(be):
                            large_net += float(be)
                        if np.isfinite(sl):
                            large_net -= float(sl)
                        if np.isfinite(se):
                            large_net -= float(se)
                        out[ts_code] = (float(n), float(large_net))
                except Exception:
                    out = {}

        self._put_lru(self._moneyflow_cache, trade_date, out)
        return out

    def _get_sw_strength(self, trade_date: str) -> tuple[Dict[str, float], float]:
        if trade_date in self._sw_daily_cache:
            self._sw_daily_cache.move_to_end(trade_date, last=True)
            return self._sw_daily_cache[trade_date]

        industry_pct: Dict[str, float] = {}
        q_value = float("nan")
        if self.aux_data_dir is not None:
            fp = self.aux_data_dir / "sw_daily" / f"{trade_date}.csv"
            if fp.exists():
                try:
                    df = pd.read_csv(fp, usecols=["name", "pct_change"])
                    names = df["name"].astype(str)
                    pct = pd.to_numeric(df["pct_change"], errors="coerce")
                    valid_pct = pct.dropna().to_numpy(dtype=float)
                    if len(valid_pct) > 0:
                        q_value = float(np.quantile(valid_pct, self.sw_strength_quantile_min))
                    for name, p in zip(names, pct):
                        if np.isfinite(p):
                            industry_pct[name] = float(p)
                except Exception:
                    industry_pct = {}
                    q_value = float("nan")

        self._put_lru(self._sw_daily_cache, trade_date, (industry_pct, q_value))
        return industry_pct, q_value

    @staticmethod
    def _audit_should_append(audit_level: str, passed: bool) -> bool:
        if audit_level == "off":
            return False
        if audit_level == "full":
            return True
        return (not passed)

    def _append_rule_audit(
        self,
        rows: List[Dict[str, Any]],
        *,
        audit_level: str,
        code: str,
        ts_code: str,
        rule_code: str,
        rule_name: str,
        passed: bool,
        actual_value: Any,
        threshold_expr: str,
        reason: str,
    ) -> None:
        if not self._audit_should_append(audit_level, passed):
            return
        rows.append(
            {
                "stage": "hard_filter",
                "code": code,
                "ts_code": ts_code,
                "rule_code": rule_code,
                "rule_name": rule_name,
                "passed": bool(passed),
                "actual_value": actual_value,
                "threshold_expr": threshold_expr,
                "reason": reason,
            }
        )

    def _core_signal_eval(self, hist: pd.DataFrame) -> Dict[str, Any]:
        if len(hist) < self.min_history:
            return {"passed": False}

        zxdq, zxdkx = compute_zx_lines(hist, m1=self.m1, m2=self.m2, m3=self.m3, m4=self.m4)
        c = float(hist["close"].iloc[-1])
        s = float(zxdq.iloc[-1])
        l = float(zxdkx.iloc[-1]) if pd.notna(zxdkx.iloc[-1]) else float("nan")
        if not (np.isfinite(c) and np.isfinite(s) and np.isfinite(l)):
            return {"passed": False}
        if not (c > l and s > l):
            return {"passed": False}

        brick_df = compute_brick_turn_signal(
            hist,
            brick_window=self.brick_window,
            var2_sma_n=self.var2_sma_n,
            var4_sma_n=self.var4_sma_n,
            var5_sma_n=self.var5_sma_n,
            brick_threshold=self.brick_threshold,
        )
        if brick_df.empty or len(brick_df) < 3:
            return {"passed": False}
        if not bool(brick_df["aa"].iloc[-1]):
            return {"passed": False}
        if not bool(brick_df["bb"].iloc[-2]):
            return {"passed": False}

        red_height = float(brick_df["brick"].iloc[-1] - brick_df["brick"].iloc[-2])
        green_height = float(brick_df["brick"].iloc[-3] - brick_df["brick"].iloc[-2])
        if red_height <= 0 or green_height <= 0:
            return {"passed": False}
        if red_height < self.strong_red_ratio * green_height:
            return {"passed": False}

        brick_strength = float(red_height / (green_height + 1e-9))
        return {
            "passed": True,
            "close": c,
            "short_line": s,
            "long_line": l,
            "brick_strength": brick_strength,
            "red_height": red_height,
            "green_height": green_height,
        }

    def _calc_moneyflow_sum(
        self,
        *,
        ts_code: str,
        hist_dates: pd.Series,
        lookback_days: int,
    ) -> tuple[bool, float, float]:
        tail_dates = hist_dates.tail(lookback_days)
        if len(tail_dates) < lookback_days:
            return False, float("nan"), float("nan")
        net_sum = 0.0
        large_sum = 0.0
        for d in tail_dates:
            td = self._to_trade_date_str(pd.Timestamp(d))
            values = self._get_moneyflow_map(td).get(ts_code)
            if values is None:
                return False, float("nan"), float("nan")
            net_sum += values[0]
            large_sum += values[1]
        return True, float(net_sum), float(large_sum)

    @staticmethod
    def _to_percentile_map(values_by_code: Dict[str, float]) -> Dict[str, float]:
        valid = {k: float(v) for k, v in values_by_code.items() if np.isfinite(v)}
        if not valid:
            return {}
        s = pd.Series(valid)
        ranks = s.rank(method="average", pct=True)
        return {str(k): float(v) for k, v in ranks.items()}

    def _score_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not candidates:
            return []
        if not self.use_score_model:
            for c in candidates:
                c["score"] = float(c.get("brick_strength", 0.0))
            return candidates

        brick_raw = {c["code"]: float(c.get("brick_strength", float("nan"))) for c in candidates}
        volume_raw = {c["code"]: float(c.get("volume_ratio", float("nan"))) for c in candidates}
        money_raw = {c["code"]: float(c.get("moneyflow_net_sum_for_score", float("nan"))) for c in candidates}
        industry_raw = {c["code"]: float(c.get("industry_pct", float("nan"))) for c in candidates}

        brick_rank = self._to_percentile_map(brick_raw)
        volume_rank = self._to_percentile_map(volume_raw)
        money_rank = self._to_percentile_map(money_raw)
        industry_rank = self._to_percentile_map(industry_raw)

        for c in candidates:
            code = c["code"]
            brick_s = brick_rank.get(code, 0.5)
            volume_s = volume_rank.get(code, 0.5)
            money_s = money_rank.get(code, 0.5)
            industry_s = industry_rank.get(code, 0.5)

            score = (
                self.score_weights["brick_strength"] * brick_s
                + self.score_weights["volume_ratio"] * volume_s
                + self.score_weights["moneyflow_strength"] * money_s
                + self.score_weights["industry_strength"] * industry_s
            )
            c["score"] = float(score)
            c["factor_scores"] = {
                "brick_strength": float(brick_s),
                "volume_ratio": float(volume_s),
                "moneyflow_strength": float(money_s),
                "industry_strength": float(industry_s),
            }
        return candidates

    def _evaluate_hard_filters(
        self,
        *,
        code: str,
        ts_code: str,
        trade_date: str,
        hist: pd.DataFrame,
        core: Dict[str, Any],
        audit_level: str,
    ) -> tuple[bool, List[Dict[str, Any]], Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        metrics: Dict[str, Any] = {}

        today = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) >= 2 else None
        c = float(today["close"])
        o = float(today["open"])
        h = float(today["high"])
        l = float(today["low"])
        day_range = h - l

        if self.t_day_pct_min is not None or self.t_day_pct_max is not None:
            prev_close = float(prev["close"]) if prev is not None else float("nan")
            pct_chg = float("nan")
            passed = False
            reason = "前一日收盘无效"
            if np.isfinite(prev_close) and prev_close > 0:
                pct_chg = c / prev_close - 1.0
                lo = self.t_day_pct_min if self.t_day_pct_min is not None else -np.inf
                hi = self.t_day_pct_max if self.t_day_pct_max is not None else np.inf
                passed = bool(lo <= pct_chg <= hi)
                reason = "" if passed else "涨幅不在允许区间"
            metrics["pct_chg"] = pct_chg
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_PCT_RANGE",
                rule_name="T日涨幅区间",
                passed=passed,
                actual_value=pct_chg,
                threshold_expr=f"{self.t_day_pct_min} <= pct_chg <= {self.t_day_pct_max}",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

        if self.close_near_high_max is not None:
            close_near_high = float("nan")
            passed = False
            reason = "当日振幅无效"
            if np.isfinite(day_range) and day_range > 0:
                close_near_high = (h - c) / day_range
                passed = bool(close_near_high <= self.close_near_high_max)
                reason = "" if passed else "收盘距离最高点过远"
            metrics["close_near_high"] = close_near_high
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_CLOSE_NEAR_HIGH",
                rule_name="收盘接近日高",
                passed=passed,
                actual_value=close_near_high,
                threshold_expr=f"(high-close)/(high-low) <= {self.close_near_high_max}",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

        if self.body_ratio_min is not None:
            body_ratio = float("nan")
            passed = False
            reason = "当日振幅无效"
            if np.isfinite(day_range) and day_range > 0:
                body_ratio = abs(c - o) / day_range
                passed = bool(body_ratio >= self.body_ratio_min)
                reason = "" if passed else "实体占比不足"
            metrics["body_ratio"] = body_ratio
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_BODY_RATIO",
                rule_name="实体占比下限",
                passed=passed,
                actual_value=body_ratio,
                threshold_expr=f"abs(close-open)/(high-low) >= {self.body_ratio_min}",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

        if self.close_to_zxdq_max_mult is not None:
            short_line = float(core.get("short_line", float("nan")))
            ratio = float("nan")
            passed = False
            reason = "短线值无效"
            if np.isfinite(short_line) and short_line > 0:
                ratio = c / short_line
                passed = bool(ratio <= self.close_to_zxdq_max_mult)
                reason = "" if passed else "短线乖离过热"
            metrics["close_to_zxdq"] = ratio
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_ZXDQ_OVERHEAT",
                rule_name="短线乖离不过热",
                passed=passed,
                actual_value=ratio,
                threshold_expr=f"close/zxdq <= {self.close_to_zxdq_max_mult}",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

        volume_ratio = float("nan")
        if self.daily_basic_volume_ratio_min is not None or self.daily_basic_volume_ratio_max is not None:
            volume_ratio = self._get_daily_basic_volume_ratio(trade_date).get(ts_code, float("nan"))
            passed = bool(np.isfinite(volume_ratio))
            reason = "" if passed else "缺少 daily_basic.volume_ratio"
            if passed:
                if self.daily_basic_volume_ratio_min is not None and volume_ratio < self.daily_basic_volume_ratio_min:
                    passed = False
                    reason = "量比低于下限"
                if self.daily_basic_volume_ratio_max is not None and volume_ratio > self.daily_basic_volume_ratio_max:
                    passed = False
                    reason = "量比高于上限"
            metrics["volume_ratio"] = volume_ratio
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_VOLUME_RATIO",
                rule_name="量比区间",
                passed=passed,
                actual_value=volume_ratio,
                threshold_expr=f"{self.daily_basic_volume_ratio_min} <= volume_ratio <= {self.daily_basic_volume_ratio_max}",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics
        metrics["volume_ratio"] = volume_ratio

        mf_ok, mf_net_sum, mf_large_sum = self._calc_moneyflow_sum(
            ts_code=ts_code,
            hist_dates=hist["date"],
            lookback_days=self.moneyflow_lookback_days,
        )
        metrics["moneyflow_net_sum_hard"] = mf_net_sum
        metrics["moneyflow_large_sum_hard"] = mf_large_sum
        if self.require_moneyflow_3d_net_inflow:
            passed = bool(mf_ok and np.isfinite(mf_net_sum) and mf_net_sum > 0)
            reason = "" if passed else "近3日净流入和不为正或缺失"
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_MONEYFLOW_3D_NET",
                rule_name="3日净流入为正",
                passed=passed,
                actual_value=mf_net_sum,
                threshold_expr="moneyflow_3d_net_sum > 0",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

        if self.require_moneyflow_3d_large_net_inflow:
            passed = bool(mf_ok and np.isfinite(mf_large_sum) and mf_large_sum > 0)
            reason = "" if passed else "近3日大单净流入和不为正或缺失"
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_MONEYFLOW_3D_LARGE",
                rule_name="3日大单净流入为正",
                passed=passed,
                actual_value=mf_large_sum,
                threshold_expr="moneyflow_3d_large_net_sum > 0",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

        industry_pct = float("nan")
        industry_q = float("nan")
        industry_name = ""
        if self.require_sw_strength:
            industry_map, industry_q = self._get_sw_strength(trade_date)
            industry_name = self._code_to_industry_l1.get(ts_code, "")
            if industry_name:
                industry_pct = float(industry_map.get(industry_name, float("nan")))

            passed = bool(industry_name and np.isfinite(industry_pct) and np.isfinite(industry_q) and industry_pct >= industry_q)
            reason = "" if passed else "行业强度低于分位阈值或缺失"
            self._append_rule_audit(
                rows,
                audit_level=audit_level,
                code=code,
                ts_code=ts_code,
                rule_code="HF_SW_STRENGTH",
                rule_name="行业强度分位",
                passed=passed,
                actual_value=industry_pct,
                threshold_expr=f"industry_pct >= q({self.sw_strength_quantile_min})",
                reason=reason,
            )
            if not passed:
                return False, rows, metrics

            if self.require_sw_positive:
                passed = bool(np.isfinite(industry_pct) and industry_pct > 0)
                reason = "" if passed else "行业涨幅不为正"
                self._append_rule_audit(
                    rows,
                    audit_level=audit_level,
                    code=code,
                    ts_code=ts_code,
                    rule_code="HF_SW_POSITIVE",
                    rule_name="行业涨幅为正",
                    passed=passed,
                    actual_value=industry_pct,
                    threshold_expr="industry_pct > 0",
                    reason=reason,
                )
                if not passed:
                    return False, rows, metrics

        metrics["industry_name"] = industry_name
        metrics["industry_pct"] = industry_pct
        metrics["industry_q"] = industry_q
        return True, rows, metrics

    def _build_hist_until_date(self, df: pd.DataFrame, date: pd.Timestamp) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return None
        df_sorted = df.sort_values("date").reset_index(drop=True)
        idx = df_sorted.index[df_sorted["date"] == date]
        if len(idx) == 0:
            return None
        pos = int(idx[-1])
        need_len = max(self.min_history, self.max_window)
        hist = df_sorted.iloc[max(0, pos - need_len + 1): pos + 1]
        if len(hist) < self.min_history:
            return None
        return hist

    def select_with_audit(
        self,
        date: pd.Timestamp,
        data: Dict[str, pd.DataFrame],
        audit_level: str = "failed_only",
    ) -> Dict[str, Any]:
        if audit_level not in {"off", "failed_only", "full"}:
            raise ValueError("audit_level 必须为 off/failed_only/full")

        core_pass_count = 0
        hard_pass_count = 0
        audit_rows: List[Dict[str, Any]] = []
        candidates: List[Dict[str, Any]] = []

        for code, df in data.items():
            hist = self._build_hist_until_date(df, date)
            if hist is None:
                continue

            core = self._core_signal_eval(hist)
            if not bool(core.get("passed", False)):
                continue
            core_pass_count += 1

            trade_date = self._to_trade_date_str(pd.Timestamp(hist["date"].iloc[-1]))
            ts_code = self._to_ts_code(code)
            hard_ok, hard_rows, metrics = self._evaluate_hard_filters(
                code=code,
                ts_code=ts_code,
                trade_date=trade_date,
                hist=hist,
                core=core,
                audit_level=audit_level,
            )
            if hard_rows:
                audit_rows.extend(hard_rows)
            if not hard_ok:
                continue
            hard_pass_count += 1

            mf_ok, mf_net_sum, _ = self._calc_moneyflow_sum(
                ts_code=ts_code,
                hist_dates=hist["date"],
                lookback_days=self.score_moneyflow_lookback_days,
            )
            industry_pct = float(metrics.get("industry_pct", float("nan")))
            if not np.isfinite(industry_pct):
                industry_name = self._code_to_industry_l1.get(ts_code, "")
                if industry_name:
                    industry_map, _ = self._get_sw_strength(trade_date)
                    industry_pct = float(industry_map.get(industry_name, float("nan")))

            candidates.append(
                {
                    "code": str(code),
                    "ts_code": ts_code,
                    "brick_strength": float(core["brick_strength"]),
                    "score": float("nan"),
                    "volume_ratio": float(metrics.get("volume_ratio", float("nan"))),
                    "moneyflow_net_sum_for_score": float(mf_net_sum if mf_ok else float("nan")),
                    "industry_pct": float(industry_pct),
                }
            )

        scored = self._score_candidates(candidates)
        scored_sorted = sorted(
            scored,
            key=lambda x: (-float(x.get("score", float("-inf"))), -float(x.get("brick_strength", float("-inf"))), str(x.get("code", ""))),
        )

        picks_scored: List[Dict[str, Any]] = []
        for rank, item in enumerate(scored_sorted, start=1):
            picks_scored.append(
                {
                    "code": item["code"],
                    "ts_code": item["ts_code"],
                    "score": float(item.get("score", float("nan"))),
                    "brick_strength": float(item.get("brick_strength", float("nan"))),
                    "factor_scores": dict(item.get("factor_scores", {})),
                    "volume_ratio": float(item.get("volume_ratio", float("nan"))),
                    "moneyflow_net_sum_for_score": float(item.get("moneyflow_net_sum_for_score", float("nan"))),
                    "industry_pct": float(item.get("industry_pct", float("nan"))),
                    "rank_before_cap": rank,
                }
            )

        ranked = picks_scored
        if self.top_n is not None:
            ranked = ranked[: self.top_n]

        cap_reject_count = 0
        if self.daily_trade_cap is not None and len(ranked) > self.daily_trade_cap:
            rejected = ranked[self.daily_trade_cap :]
            cap_reject_count = len(rejected)
            if audit_level != "off":
                for item in rejected:
                    audit_rows.append(
                        {
                            "stage": "execution",
                            "code": item["code"],
                            "ts_code": item["ts_code"],
                            "rule_code": "DAILY_CAP",
                            "rule_name": "每日交易上限",
                            "passed": False,
                            "actual_value": int(item["rank_before_cap"]),
                            "threshold_expr": f"rank <= {self.daily_trade_cap}",
                            "reason": "超过每日交易上限",
                            "score": float(item["score"]),
                            "brick_strength": float(item["brick_strength"]),
                            "rank_before_cap": int(item["rank_before_cap"]),
                            "reject_reason": "DAILY_CAP",
                        }
                    )
            ranked = ranked[: self.daily_trade_cap]

        picks_final = [str(x["code"]) for x in ranked]
        summary = {
            "core_pass_count": int(core_pass_count),
            "hard_pass_count": int(hard_pass_count),
            "selected_count": int(len(picks_final)),
            "cap_reject_count": int(cap_reject_count),
        }
        return {
            "picks_final": picks_final,
            "picks_scored": picks_scored,
            "audit_rows": audit_rows,
            "audit_summary": summary,
        }

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        result = self.select_with_audit(date, data, audit_level="off")
        return list(result.get("picks_final", []))


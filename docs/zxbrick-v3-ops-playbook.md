# ZXBrick v3 每日执行与复盘手册

更新时间：2026-03-04

一页版清单：`docs/zxbrick-v3-daily-checklist.md`

## 1. 执行口径（固定）

- 策略配置：`configs/backtest_brick_compare_aux_top2.json`
- 主策略：`搬砖_增强v3_top2`
- 交易纪律：每策略每日最多 2 笔（`daily_trade_cap=2`）
- 回测口径：`T+1 open` 买入，`T+2 close` 卖出

## 2. 每日执行流程

### 2.1 更新行情数据（`data`）

```bash
python scripts/fetch_kline.py \
  --start 20250101 \
  --end today \
  --stocklist ./configs/stocklist.csv \
  --out ./data
```

### 2.2 更新辅助数据（`data_aux`）

```bash
python scripts/fetch_aux_data.py \
  --start 20250101 \
  --end today \
  --data-dir ./data \
  --out-dir ./data_aux
```

### 2.3 当日选股（含审计）

```bash
python scripts/select_stock.py \
  --data-dir ./data \
  --config ./configs/backtest_brick_compare_aux_top2.json \
  --date 2026-03-03 \
  --audit failed_only \
  --out-dir ./out
```

> `--date` 建议填最新完整交易日；若不传默认使用数据中的最后交易日。

## 3. 每日看哪些输出

### 3.1 最终入选名单

- `out/select_results_summary_*.csv`
  - 关键列：`selected_count`、`cap_reject_count`
- `out/select_results_detail_*.csv`
  - 每条入选明细

### 3.2 过滤失败复盘

- `out/select_filter_audit_*.csv`
  - 关键列：`rule_code`、`reason`、`actual_value`、`threshold_expr`

### 3.3 超额淘汰复盘（>2笔被挤掉）

- `out/select_execution_audit_*.csv`
  - 关键列：`score`、`rank_before_cap`、`reject_reason=DAILY_CAP`

## 4. 每周检查（建议）

```bash
python scripts/backtest_selectors.py \
  --data-dir ./data \
  --config ./configs/backtest_brick_compare_aux_top2.json \
  --start-date 2025-01-01 \
  --end-date 2026-03-03 \
  --entry-price open \
  --exit-mode fixed \
  --hold-days 2 \
  --out-dir ./out
```

关注 `out/backtest_*_summary.csv`：

- `total_return`
- `max_drawdown`
- `trade_count`
- `avg_cap_reject_per_day`
- `cap_binding_ratio`

## 5. 每月优化（建议）

```bash
python scripts/grid_search_zxbrick_v3.py \
  --data-dir ./data \
  --config ./configs/backtest_brick_compare_aux_top2.json \
  --selector-alias 搬砖_增强v3_top2 \
  --start-date 2025-01-01 \
  --end-date 2026-03-03 \
  --entry-price open \
  --hold-days 2 \
  --daily-trade-cap 2 \
  --grid-values 0.1,0.15,0.2,0.25,0.3,0.35,0.4 \
  --top-k 30 \
  --audit-top-n 30 \
  --out-dir ./out
```

重点看：

- `out/grid_search_zxbrick_v3_*_top30.csv`
- `out/grid_search_zxbrick_v3_*_best.json`
- `out/grid_search_zxbrick_v3_*_hard_filter_rule_rank.csv`
- `out/grid_search_zxbrick_v3_*_cap_reject_code_rank.csv`

## 6. 异常排查

- 入选为 0：
  - 先看 `select_filter_audit` 的 Top 原因是否集中在单一规则（如 `HF_PCT_RANGE`）
- `cap_reject_count` 很高：
  - 先看 `select_execution_audit` 的 `score` 分布，再决定是否改权重或保留 top2 纪律
- 回测突然变差：
  - 对比最近两次 `backtest_*_summary.csv` 的 `cap_binding_ratio` 与 `trade_count` 是否发生结构变化

## 7. 操作建议

- 日常执行只看 `v3` 出单，但保留 `v2_top2` 作为对照回测。
- 任何阈值或权重调整后，至少做一次全区间回测再决定是否切换。

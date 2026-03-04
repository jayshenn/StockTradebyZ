# ZXBrick v3 每日操作清单（一页版）

更新时间：2026-03-04

## A. 每天固定 3 步

1. 更新行情

```bash
python scripts/fetch_kline.py --start 20250101 --end today --stocklist ./configs/stocklist.csv --out ./data
```

2. 更新辅助数据

```bash
python scripts/fetch_aux_data.py --start 20250101 --end today --data-dir ./data --out-dir ./data_aux
```

3. 运行选股（含审计）

```bash
python scripts/select_stock.py --data-dir ./data --config ./configs/backtest_brick_compare_aux_top2.json --date 2026-03-03 --audit failed_only --out-dir ./out
```

## B. 每天只看 5 个文件

1. `out/select_results_summary_*.csv`
2. `out/select_results_detail_*.csv`
3. `out/select_filter_audit_*.csv`
4. `out/select_execution_audit_*.csv`
5. `out/select_results_stockinfo_*.csv`

## C. 每天只看 3 个指标

1. `selected_count`（是否有票、是否稳定）
2. `cap_reject_count`（是否大量被 top2 纪律挤掉）
3. `hard_filter` Top 失败原因（是否集中在单一规则）

## D. 决策规则（最小化）

- `selected_count == 0`：看 `hard_filter` Top 原因，不改模型，先观察 3-5 天是否持续。
- `cap_reject_count` 连续偏高：说明候选拥挤，优先看 `execution_audit` 的 `score` 分布。
- 单一过滤规则长期占比过高：只微调该规则，不同时改多个参数。

## E. 每周一次（周末）

```bash
python scripts/backtest_selectors.py --data-dir ./data --config ./configs/backtest_brick_compare_aux_top2.json --start-date 2025-01-01 --end-date 2026-03-03 --entry-price open --exit-mode fixed --hold-days 2 --out-dir ./out
```

必看：

- `total_return`
- `max_drawdown`
- `trade_count`
- `cap_binding_ratio`

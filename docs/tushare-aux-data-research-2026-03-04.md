# 搬砖战法辅助数据调研（Tushare）- 2026-03-04

## 1. 目标

围绕 `ZXBrickTurnSelector（搬砖战法）` 做增强，确认：

1. 官方文档上可用的辅助数据接口有哪些；
2. 当前账号是否可访问这些接口（不只看文档积分门槛，做真实调用）；
3. 哪些接口最适合“只吃 T+1 / T+2 两根K线”的超短模型。

---

## 2. 测试方法与边界

- 测试日期：`2026-03-04`
- 运行环境：项目本地 `.venv` + `.env` 内 `TUSHARE_TOKEN`
- 验证方式：对候选接口做真实 API 调用，记录 `OK / ERR` 与返回信息
- 注意事项：
  - `limit_list_d` 实测存在“每小时最多访问 1 次”限制；
  - 同一接口“文档积分门槛”与“账号实际权限”可能不完全一致，以实测为准；
  - 账号信息接口返回：`到期积分=2000.0`（到期时间 `2027-03-01`）。

---

## 3. 接口可访问性实测结果

### 3.1 可访问（建议优先用于策略增强）

| 接口 | 用途 | 实测结果 |
|---|---|---|
| `daily_basic` | 换手、量比、流通市值、估值等 | OK |
| `moneyflow` | 个股资金流（大单/特大单/净流入） | OK |
| `margin_detail` | 融资买入额、融资余额等 | OK |
| `cyq_perf` | 每日筹码胜率/成本分位 | OK |
| `cyq_chips` | 每日筹码分布（价位-占比） | OK |
| `index_classify` | 申万行业分类字典 | OK |
| `index_member_all` | 个股所属申万行业 | OK |
| `sw_daily` | 申万行业日线强弱 | OK |
| `moneyflow_ind_ths` | THS 行业资金流 | OK |
| `moneyflow_cnt_ths` | THS 概念资金流 | OK |
| `moneyflow_dc` | 东财个股资金流 | OK |
| `dc_daily` | 东财概念板块日线 | OK |
| `moneyflow_hsgt` | 北/南向资金总流入 | OK |
| `hsgt_top10` | 沪深股通前十成交股净买入 | OK |

### 3.2 不可访问 / 受限（当前账号）

| 接口 | 状态 | 备注 |
|---|---|---|
| `limit_step` | 无权限 | 返回“没有接口访问权限” |
| `limit_cpt_list` | 无权限 | 返回“没有接口访问权限” |
| `ths_index` | 无权限 | 返回“没有接口访问权限” |
| `ths_daily` | 无权限 | 返回“没有接口访问权限” |
| `ths_member` | 无权限 | 返回“没有接口访问权限” |
| `dc_index` | 无权限 | 返回“没有接口访问权限” |
| `limit_list_d` | 受限 | 本账号每小时最多 1 次；已成功调通但同小时重复调用会被限频 |

---

## 4. 面向“两根K线”的增强建议

你的策略定位是：在 `T` 日捕捉转折点，主要吃 `T+1` 和 `T+2` 的延续爆发。  
因此更适合“中强启动 + 资金确认 + 阻力可控”，而不是追 `T` 日极限爆发。

### 4.1 建议保留的核心信号（已有）

- 砖转红强：`T-1 绿砖, T 红砖, 红砖强度达标`
- 趋势底座：`close > 黄线 && 白线 > 黄线`

### 4.2 建议新增硬过滤（先做这一层）

1. `T日不过热`
   - `T日涨幅`设区间（例如：`3% ~ 8%`）
   - `close / ZXDQ <= 1.08`（可再加 `close / MA20 <= 1.10`）
2. `K线质量`
   - 收盘接近日高：`(high-close)/(high-low) <= 0.2`
   - 实体占比下限（避免上影主导）
3. `资金确认`
   - `moneyflow`：`3日净流入和 > 0`
   - `大单+特大单净流入 > 0`
4. `筹码阻力`
   - `cyq_perf.winner_rate` 区间（例如 `20~70`）
   - `close <= cost_95pct * 1.03`
5. `板块共振`
   - 用 `index_member_all + sw_daily` 做行业强度过滤（近 3~5 日强于中位数）

### 4.3 建议新增打分项（第二阶段）

- `daily_basic.volume_ratio`：中高位加分，过低/过高降分
- `margin_detail.rzmre`：融资买入连续改善加分
- `moneyflow_ind_ths` / `moneyflow_cnt_ths`：行业与概念资金共振加分
- `moneyflow_hsgt` / `hsgt_top10`：外资风向加分

---

## 5. 落地优先级（建议）

1. 先实现“硬过滤”5项（避免策略复杂度失控）  
2. 跑 A/B：`原搬砖` vs `搬砖+硬过滤`  
3. 再叠加“打分层”做排序，而不是继续堆硬阈值  
4. 单独记录 `T+1`、`T+2` 胜率与盈亏分布，确保和策略目标一致

---

## 6. 参考文档（官方）

- 总目录：https://tushare.pro/document/2
- daily_basic：https://tushare.pro/document/2?doc_id=32
- moneyflow：https://tushare.pro/document/2?doc_id=170
- margin_detail：https://tushare.pro/document/2?doc_id=59
- cyq_perf：https://tushare.pro/document/2?doc_id=327
- cyq_chips：https://tushare.pro/document/2?doc_id=294
- limit_list_d：https://tushare.pro/document/2?doc_id=298
- limit_step：https://tushare.pro/document/2?doc_id=357
- limit_cpt_list：https://tushare.pro/document/2?doc_id=260
- index_classify：https://tushare.pro/document/2?doc_id=181
- index_member_all：https://tushare.pro/document/2?doc_id=362
- sw_daily：https://tushare.pro/document/2?doc_id=404
- ths_index：https://tushare.pro/document/2?doc_id=259
- ths_daily：https://tushare.pro/document/2?doc_id=328
- ths_member：https://tushare.pro/document/2?doc_id=261
- moneyflow_ind_ths：https://tushare.pro/document/2?doc_id=364
- moneyflow_cnt_ths：https://tushare.pro/document/2?doc_id=365
- moneyflow_dc：https://tushare.pro/document/2?doc_id=363
- dc_index：https://tushare.pro/document/2?doc_id=360
- dc_daily：https://tushare.pro/document/2?doc_id=361
- moneyflow_hsgt：https://tushare.pro/document/2?doc_id=47
- hsgt_top10：https://tushare.pro/document/2?doc_id=48

---

## 7. ZXBrick v3 执行纪律与审计（新增）

### 7.1 每策略每日最多 2 笔

- 回测与选股统一执行：`daily_trade_cap=2`
- 候选排序规则：`score desc` -> `brick_strength desc` -> `code asc`
- 当日超过 2 只时，仅保留前 2 只，其余记为执行淘汰（`reject_reason=DAILY_CAP`）

### 7.2 过滤与执行审计可见

`scripts/select_stock.py` 在原有结果外，新增四个文件：

- `select_filter_audit_*.csv`
- `select_filter_audit_*.json`
- `select_execution_audit_*.csv`
- `select_execution_audit_*.json`

其中：

- `filter_audit`：记录硬过滤失败原因（规则、阈值、实际值）
- `execution_audit`：记录因每日上限被淘汰的个股（含 `score` 与 `rank_before_cap`）

---

## 8. ZXBrick v3 权重网格与失败原因榜（2026-03-04）

### 8.1 实验口径

- 区间：`2025-01-01 ~ 2026-03-03`
- 入场/离场：`T+1 open` 入场，`T+2 close` 离场
- 纪律：`daily_trade_cap=2`
- 网格：`brick_strength/volume_ratio/moneyflow_strength/industry_strength`
  - 取值集合：`{0.10,0.15,0.20,0.25,0.30,0.35,0.40}`
  - 约束：四者和为 `1.0`
  - 组合数：`231`

### 8.2 最优权重（本轮）

- `brick_strength = 0.10`
- `volume_ratio = 0.35`
- `moneyflow_strength = 0.30`
- `industry_strength = 0.25`

对应回测结果：

- `trade_count = 500`
- `total_return = 112.62%`
- `max_drawdown = -10.66%`
- `trade_win_rate = 49.60%`
- `avg_trade_return = 0.503%`

对比当前 v3 默认权重 `0.35/0.25/0.20/0.20`：

- `total_return: 77.04% -> 112.62%`
- `max_drawdown: -14.34% -> -10.66%`

结论：当前样本下，排序层更偏好“量比+资金+行业”，而不是过度依赖砖强度。

### 8.3 高频失败原因榜（hard_filter）

按失败次数排序（Top）：

1. `HF_PCT_RANGE`（涨幅不在允许区间）：`28154`
2. `HF_CLOSE_NEAR_HIGH`（收盘距离最高点过远）：`5405`
3. `HF_SW_STRENGTH`（行业强度低于分位阈值或缺失）：`2737`
4. `HF_MONEYFLOW_3D_NET`（近3日净流入和不为正或缺失）：`2175`
5. `HF_VOLUME_RATIO`（量比低于下限）：`1947`

### 8.4 高频失败原因榜（cap_reject）

- 统一原因为：`DAILY_CAP`
- 总淘汰次数：`2474`
- 高频被挤出代码（Top）：
  - `680`：`7`
  - `2379`：`7`
  - `603986`：`6`
  - `2156`：`6`
  - `2801`：`6`

### 8.5 输出文件

- `out/grid_search_zxbrick_v3_20260304_181909_best.json`
- `out/grid_search_zxbrick_v3_20260304_181909_summary.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_top30.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_hard_filter_rule_rank.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_hard_filter_reason_rank.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_hard_filter_code_rank.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_cap_reject_reason_rank.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_cap_reject_code_rank.csv`
- `out/grid_search_zxbrick_v3_20260304_181909_cap_reject_date_rank.csv`

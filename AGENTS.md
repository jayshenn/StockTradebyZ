# Repository Guidelines

## Project Structure & Module Organization
This repository is a script-based Python project (no `src/` package yet). Key files live at the root:
- `fetch_kline.py`: downloads daily qfq K-line data from Tushare into `./data`.
- `select_stock.py`: runs selectors from `configs.json` against local CSV data.
- `Selector.py`: shared indicators and strategy classes.
- `SectorShift.py`: industry distribution analysis for J-value filters.
- `find_stock_by_price_concurrent.py`: concurrent historical price lookup.
- `configs.json`, `stocklist.csv`: runtime inputs.

Generated artifacts include `data/`, `fetch.log`, and `select_results.log`.

## Build, Test, and Development Commands
- `python -m venv .venv && source .venv/bin/activate`: create and activate a local env.
- `pip install -r requirements.txt`: install runtime dependencies.
- `python fetch_kline.py --start 20250101 --end today --stocklist ./stocklist.csv --out ./data`: fetch market data.
- `python select_stock.py --data-dir ./data --config ./configs.json --date 2025-09-10`: run configured strategies.
- `python SectorShift.py --data_dir ./data --stocklist stocklist.csv --j_threshold 15`: compute industry distribution.
- `python -m compileall .`: quick syntax sanity check before committing.

## Coding Style & Naming Conventions
Use Python 3.11+ style with 4-space indentation and type hints for new/changed functions. Follow existing naming:
- Functions/variables/files: `snake_case`
- Classes/selectors: `PascalCase` (in `Selector.py`)
Keep CLI args explicit (`--data-dir`, `--trade_date`, etc.), prefer `logging` over `print`, and keep data columns stable (`date, open, close, high, low, volume`).

## Testing Guidelines
There is no formal automated test suite yet. For every logic change, run at least:
1. `python -m compileall .`
2. A focused script smoke test (for example, `select_stock.py` on a known date).
3. A behavior check of logs/output counts.

If you add tests, place them under `tests/` and use `test_<module>.py` naming.

## Commit & Pull Request Guidelines
Current history favors short, focused commit messages (for example, `Update Selector.py`, `Update configs.json`, or concise Chinese summaries). Keep commits scoped to one concern.

For PRs, include:
- what changed and why,
- impacted scripts/config keys,
- exact commands run for verification,
- sample output/log snippets when strategy behavior changes.

## Security & Configuration Tips
Set `TUSHARE_TOKEN` via environment variables; never commit tokens or local secrets. Treat generated data/log files as runtime artifacts and avoid committing large, frequently changing outputs.

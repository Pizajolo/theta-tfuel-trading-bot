# THETA/TFUEL ratio trading bot (Binance Spot)

Trades the price ratio `R = price(THETA) / price(TFUEL)` on Binance Spot (`THETAUSDT`, `TFUELUSDT`)
with the goal of ending up with **more THETA and more TFUEL than simply holding**. It runs two
independent strategy instances, `s1k` (1,000 USD) and `s4k` (4,000 USD), logs everything to JSON
files and serves a local dashboard built only from those files.

**The default is PAPER mode**: market data is logged, signals are computed and fills are
simulated. An instance only places real orders when you explicitly enable it (see
[Going live](#going-live)).

> Not financial advice. Read [Known risks](#known-risks) before enabling live trading.

---

## Contents

- [How it works](#how-it-works)
- [Setup](#setup)
- [Running](#running)
- [Dashboard](#dashboard)
- [Replay / backtest](#replay--backtest)
- [Binance sub-accounts and API keys](#binance-sub-accounts-and-api-keys)
- [Testing live mode on the Spot testnet](#testing-live-mode-on-the-spot-testnet)
- [Going live](#going-live)
- [Kill switch](#kill-switch)
- [Files written to DATA_DIR](#files-written-to-data_dir)
- [Code layout](#code-layout)
- [Development and tests](#development-and-tests)
- [Known risks](#known-risks)

---

## How it works

**Headline metric:** `excess = V_strategy / V_hodl - 1`, both valued at current prices, where
`V_hodl` values the token amounts held at the benchmark start. `excess = +12%` means you could
convert back to the HODL mix and hold 12% more THETA **and** 12% more TFUEL.

The target THETA weight (THETA value / (THETA + TFUEL value)) combines two layers:

1. **Daily ladder (slow).** Once per day, right after 00:00 UTC, the previous day's closing log
   ratio updates a 60-day EMA. The deviation `D` moves the ladder weight with hysteresis:
   `|D| > 15%` -> 0.35 / 0.65, `|D| > 30%` -> 0.20 / 0.80, back to 0.50 when `|D| < 5%`; it never
   steps back halfway.
2. **Minute overlay (fast).** On every closed 1-minute bar the log ratio updates a 3-day EMA.
   Deviation `> +6%` (THETA rich) -> `pos = -1`, `< -6%` -> `pos = +1`, back to 0 when
   `|d| < 0.2%`, flips directly. A failsafe forces `pos = 0` after 4,320 minutes in a position.
   Bars where either symbol had zero volume are *stale*: EMAs update, positions never change.

`w_target = clip(w_ladder + pos * 0.25, 0.05, 0.95)`. The bot **only rebalances when
`w_target` changes** (never on drift); an incomplete rebalance is retried on the next closed
non-stale bar (up to 10 times).

**Execution** goes through USDT, sell leg first: sell the rich asset, buy the cheap one with the
USDT actually received. Orders are LIMIT IOC at the touch, at most 0.3% through it, in slices of
at most 500 USD at least 30 s apart, rounded to the exchange's `stepSize`/`tickSize`. If the buy
leg fails the USDT is kept and the buy is retried on the next bar. Client order ids are
deterministic (`{instance}-{decision_id}-{leg}-{slice}`) and persisted before sending, so a
restart looks an order up instead of sending it twice.

**Paper fills** are simulated three ways, each with its own shadow portfolio:
`mid` (optimistic), `touch` (bid/ask at decision time, the **primary** paper portfolio) and
`worst` (worst price of the next 1m bar, the pessimistic backtest assumption).

### Backtest reference (combined strategy, pessimistic .. optimistic)

| Instance | 2023 | 2024 | 2025 | 2026 YTD |
|---|---|---|---|---|
| s1k | +10..16% | +26..49% | +16..47% | +14..22% |
| s4k | +9..15% | +22..44% | +14..44% | +9..17% |

The dashboard shows where paper/live results sit relative to these ranges.

---

## Setup

Requires Python 3.11+ (3.12 in Docker).

```bash
git clone <this repo> && cd theta-tfuel-trading-bot
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt          # requirements-dev.txt adds pytest
cp .env.example .env                     # then edit .env
```

Every parameter is documented in [`.env.example`](.env.example). Any strategy, execution or
risk parameter can be overridden per instance with its prefix, e.g. `S1K_OVERLAY_ENTRY=0.05`.

Precedence is: process environment > `.env` > defaults. The bot re-reads `.env` every 60 seconds:
live flags, the kill switch, risk limits and execution settings apply immediately (logged as
events); strategy parameters, capital and API keys need a restart (the bot logs a warning).

### Docker

```bash
cp .env.example .env && mkdir -p data
sudo chown 1000:1000 data .env           # the containers run as uid 1000
sudo chmod 600 .env                      # secrets: readable only by uid 1000 (edit with sudo)
docker compose up -d --build             # services: bot + dashboard, sharing ./data
docker compose logs -f bot
```

`.env` is mounted read-only as a file (not as `env_file`) so the hot reload sees your edits.
The dashboard is published on `127.0.0.1:8050` only; use an SSH tunnel to view it remotely.

---

## Running

```bash
python -m bot run                         # paper mode unless live is explicitly enabled
python -m bot status                      # one-screen summary from DATA_DIR
python -m dashboard                       # http://127.0.0.1:8050
```

At startup the bot:

1. loads `state.json` of each instance (EMAs, positions, portfolios, live execution state);
2. warms up from REST: the overlay needs at least 3 x span (12,960) 1m bars and the ladder at
   least 180 daily closes; an instance whose state is recent continues from it and the gap is
   replayed bar by bar;
3. connects the combined WebSocket stream and emits closed bars strictly in order. Any gap
   (disconnects, Binance's forced 24 h disconnect, a missed close event) is backfilled from
   REST klines so bars and EMAs stay continuous. The connection is renewed proactively after
   23.5 h, reconnects use exponential backoff, and REST calls respect the
   `X-MBX-USED-WEIGHT-1M` header and back off on 429/418.

Paper portfolios start at 50/50 THETA/TFUEL at the first live bar; the HODL benchmark is the
same token amounts. Like live mode, they do not trade until `w_target` next changes.

Only one bot process may use a `DATA_DIR` at a time (`data/bot.lock`).

---

## Dashboard

`python -m dashboard` (FastAPI + one static page with Chart.js from a CDN, auto-refresh every
30 s) reads only the files in `DATA_DIR` and never calls Binance. It binds to `127.0.0.1` by
default. It shows:

- header: UTC time, age of the last bar (red if older than 3 minutes), WebSocket health, kill switch;
- one card per instance: PAPER/LIVE badge, excess vs HODL (primary variant plus the mid/touch/worst
  range), token counts vs HODL, value, THETA weight vs target, overlay position, ladder weight,
  trades, fees, average slippage and the backtest reference range for the current year
  (prorated to the period covered) with a green/amber/red indicator;
- the ratio with its 3-day EMA and ±entry bands, the 60-day EMA with the ±15%/±30% ladder
  bands and every rebalance per instance (24 h, 7 d, 30 d, all);
- excess vs HODL over time (all variants), THETA weight vs target over time;
- the last 50 decisions, orders (with slippage) and events (warnings and errors highlighted);
- live vs paper (touch) excess and the realised slippage distribution once an instance is live.

To look at a replay: `python -m dashboard --data-dir data_replay`.

---

## Replay / backtest

```bash
python -m bot replay --file theta_tfuel_1m_2023_2025.xlsx --file theta_tfuel_1m_2026.xlsx --instance all
python -m bot replay --file data.csv --instance s1k --out data_replay_s1k --start 2024-01-01 --end 2024-12-31
```

The replay feeds historical 1m bars through the identical `signals.py`, `instance.py` and
`execution_paper.py` code path and writes the same JSON layout into `data_replay/` (or `--out`),
so the dashboard can display it. Notes:

- Input columns: `time, THETA_open..THETA_volume, TFUEL_open..TFUEL_volume, THETA_per_TFUEL`.
  `THETA_per_TFUEL` is ignored - the ratio is recomputed from the closes (it is THETA price /
  TFUEL price, i.e. TFUEL per THETA, despite its name). All sheets of an `.xlsx` are read
  (needed above Excel's 1,048,576-row limit); several `--file`s are merged in time order.
- No historical order book exists, so `mid` and `touch` both fill at the close (the optimistic
  "close fill" case); `worst` fills at the next bar's high/low (the pessimistic case).
- Warm-up is the same as live: the overlay trades after 3 x span bars, the ladder moves after 180
  daily closes. Use `--start` to warm up on earlier data, or `--overlay-warmup-bars` /
  `--ladder-warmup-days` to change it. The data starts in 2023, so without overrides the ladder
  is neutral until mid-2023.
- The report (`replay_report.json`, also printed) gives per-year excess for every variant, both
  *rebased* (HODL reset to the holdings on 1 January) and *chained* (growth of V/V_hodl over the
  year), next to the reference ranges.
- The backtest's market-impact model is not reproduced; `PAPER_IMPACT_BPS_PER_1K` (optional, off
  by default) adds a simple size-dependent impact to paper fills if you want to approximate it.
  Without it, `s1k` and `s4k` give nearly identical replay results.

Large deviations from the reference ranges should be investigated (warm-up, impact model, the
per-year convention), not tuned away.

Resources: one year of 1m data (525,600 rows, both instances) takes about 3 minutes from `.xlsx`
(most of it is reading the workbook), about 100 MB of RAM and about 700 MB of output in the replay
directory - budget roughly 2-3 GB for the full 2023-2026 replay.

---

## Binance sub-accounts and API keys

Two live strategies cannot share balances, so **each live instance needs its own Binance
sub-account and its own API key**. The bot refuses to start if both live instances use the same
key.

1. Binance -> Account -> Sub Accounts -> create e.g. `ratio-s1k` and `ratio-s4k`.
2. Transfer the capital into each sub-account: THETA, TFUEL, a small USDT buffer (about 10 USD
   lets the buy leg spend the full sell proceeds) and a little BNB for fees if
   `USE_BNB_FEES=true` (enable "Use BNB to pay for fees" in the sub-account; the bot warns below
   `BNB_MIN_USD`).
3. In each sub-account create an API key (API management) with
   - **Enable Reading** and **Enable Spot & Margin Trading** only;
   - **withdrawals disabled** - the bot checks `GET /sapi/v1/account/apiRestrictions` and refuses
     to trade live with a key that can withdraw;
   - **restricted to your server's IP address** (strongly recommended; the bot logs a warning
     otherwise). Universal transfer / margin / futures permissions are not needed.
4. Put the keys into `.env` (`S1K_API_KEY` / `S1K_API_SECRET`, `S4K_API_KEY` / `S4K_API_SECRET`).
   Never commit `.env`; secrets are never written to logs.

---

## Testing live mode on the Spot testnet

1. Log in at <https://testnet.binance.vision> with GitHub and generate an HMAC API key (one
   testnet account per instance you want to test; the testnet has no sub-accounts and no `/sapi`
   endpoints, so the withdrawal-permission check is skipped there and only `canTrade` is checked).
2. In `.env`: `BINANCE_TESTNET=true`, `LIVE_CONFIRM=I_UNDERSTAND_REAL_MONEY`, `S1K_LIVE=true`,
   `S1K_API_KEY` / `S1K_API_SECRET` = the testnet key. Use a separate `DATA_DIR` (e.g.
   `./data_testnet`) so testnet state never mixes with real state.
3. Testnet THETA/TFUEL markets may not exist or may be thin. If they are missing the bot stops at
   `exchangeInfo`; then use the local fake exchange below to exercise live mode.
4. Exercise: a full rebalance cycle (lower `OVERLAY_ENTRY`/`OVERLAY_EMA_SPAN_MIN` temporarily to
   provoke one), partial fills (`MAX_SLIPPAGE=0`), the kill switch (`touch data/KILL`), risk
   limits (`MAX_ORDER_USD=5`), and a restart in the middle of a rebalance.

### Local fake exchange (no Binance access needed)

`tools/fake_binance.py` serves the REST endpoints and the combined WebSocket stream the bot uses,
with synthetic prices, per-key accounts and failure injection (development only):

```bash
FAKE_ACCOUNTS="k1:s1,k4:s4" python -m tools.fake_binance --port 8090
# .env: BINANCE_REST_URL=http://127.0.0.1:8090  BINANCE_WS_URL=ws://127.0.0.1:8090
#       S1K_API_KEY=k1 S1K_API_SECRET=s1  (+ LIVE_CONFIRM / S1K_LIVE for live mode)
curl -X POST "localhost:8090/admin/shock?pct=8"            # THETA jumps 8% -> overlay entry
curl -X POST "localhost:8090/admin/outage?seconds=300"     # 5 min network outage -> backfill
curl -X POST "localhost:8090/admin/partial_fill?ratio=0.5"
curl -X POST "localhost:8090/admin/fail_next?symbol=THETAUSDT"  # buy leg fails -> USDT kept, retried
curl localhost:8090/admin/state
```

---

## Going live

Only after paper mode has run for a while, the replay was reviewed and the testnet (or fake
exchange) cycle above worked:

1. Fund the `s1k` sub-account and create its trade-only, IP-restricted key (see above).
2. In `.env`:
   ```dotenv
   BINANCE_TESTNET=false
   LIVE_CONFIRM=I_UNDERSTAND_REAL_MONEY
   S1K_LIVE=true
   S1K_API_KEY=...
   S1K_API_SECRET=...
   ```
   Both `S1K_LIVE=true` **and** the exact confirmation phrase are required. The running bot picks
   this up within 60 s (or restart it).
3. At activation the bot verifies the key (trading allowed, **withdrawals disabled**), cancels
   stray open orders of the instance, snapshots the real balances as the new HODL benchmark,
   re-seeds the paper shadow portfolios from the same balances (so live and paper are compared
   from the same start) and logs the current THETA weight. **Balances are not converted**; the next
   `w_target` change rebalances naturally.
4. Optional, to start from 50/50: stop the bot, then
   `python -m bot init-balance --instance s1k` - it shows the plan, asks you to type the instance
   name, rebalances with the normal execution engine and re-snapshots the benchmark.
5. Watch the dashboard (LIVE badge, live vs paper section) and `events.jsonl`.

Hard limits per instance: `MAX_ORDER_USD` (1,500), `MAX_TRADES_PER_DAY` (30 filled orders, each
slice leg counts) and `MAX_DAILY_TURNOVER_PCT` (300% of the day's starting value). An order that
would breach any of them is not sent and blocks further live orders until the next UTC day (one
ERROR event). A rebalance interrupted by the block keeps its USDT and resumes the next day without
using up its retries. Raising a limit in `.env` does not lift a block that already happened today.

Other live safeguards worth knowing:

- If the bot is killed in the middle of a rebalance, the restart resolves the order that was in
  flight by its client order id, cancels stray open orders of the instance and finishes the
  remaining part from the real balances - nothing is sent twice.
- If an order's outcome cannot be determined (network loss right after sending), it stays "in
  flight" and nothing else is sent for that decision until it is resolved.
- A live decision taken while live execution was unavailable (e.g. failed startup checks, no
  prices yet) is re-queued as soon as the executor is back.
- Leftover USDT below `MIN_TRADE_USD` after a rebalance is treated as dust and spent first by the
  next rebalance.

Turning `S1K_LIVE` off (or removing `LIVE_CONFIRM`) ends the live period; paper continues. A later
re-activation starts a new benchmark.

---

## Kill switch

Either set `KILL_SWITCH=true` in `.env` (picked up within 60 s) or, faster:

```bash
touch data/KILL          # checked every 2 seconds
rm data/KILL             # resume
```

On activation every live instance stops its current execution, cancels all open orders of its
account and halts (badge `HALTED`, CRITICAL event). Paper simulation continues. When the switch
is cleared, live trading resumes with the same benchmark.

---

## Files written to DATA_DIR

Append-only streams are JSON Lines (one object per line), rotated daily by UTC date; state files
are written atomically (temp file + `os.replace`). Every record has `ts` (ISO-8601 UTC, always
the first key), `schema_version` and, where relevant, `instance`. Nothing is deleted
automatically.

```
data/
  market/bars_YYYY-MM-DD.jsonl         one per closed minute: ts (open time), theta{o,h,l,c,v,bid,ask},
                                       tfuel{...}, ratio, lr, ema3d (log), ema3d_ratio, dev, stale
  market/state.json                    market EMA state
  instances/<instance>/
    signals_YYYY-MM-DD.jsonl           one per minute: dev, entry, exit, pos, pos_prev, ladder_w,
                                       w_target, w_current, action (none|enter|exit|flip|failsafe|retry)
    decisions.jsonl                    each w_target change: decision_id, reason, w_from, w_target, dv_usd, mode
    orders.jsonl                       live order lifecycle (submitted/final) and simulated fills:
                                       client_order_id, symbol, side, type, tif, price, qty, quote_qty,
                                       status, filled_qty, avg_price, commission, commission_asset,
                                       mid_at_decision, slippage_bps_vs_mid, mode, variant
    equity_YYYY-MM-DD.jsonl            every 5 min per variant: bal, mid, value_usd, hodl_value_usd,
                                       excess, theta_equiv_tokens, tfuel_equiv_tokens, w, w_target
    ladder.jsonl                       one per day: date, lr_day, ema60 (log), D, ladder_w_prev, ladder_w
    state.json                         restart state (atomic)
    summary.json                       rolled-up stats for the dashboard (atomic, every minute)
  events.jsonl                         startups, mode changes, warnings, errors, reconnects, kill switch
  runtime.json                         process health for the dashboard header
  KILL                                 (optional) kill switch file
```

`theta_equiv_tokens = hodl_THETA * (1 + excess)` (same for TFUEL) - what "more tokens" means.
Log-space values (`lr`, `ema3d`, `ema60`, `D`, `dev`) are natural logs of the ratio.

---

## Code layout

```
bot/
  config.py           .env loading, typed parameters, per-instance overrides, live gating
  storage.py          JSON/JSONL writers, atomic state files, daily rotation
  binance_client.py   async REST client: HMAC signing, weight tracking, 429/418 back-off
  market_data.py      combined WebSocket stream, ordered bar emission, REST gap backfill
  signals.py          pure EMAs and overlay/ladder state machines (no I/O)
  portfolio.py        balances, weights, HODL benchmark, excess
  execution_paper.py  mid/touch/worst simulated fills
  execution_live.py   sliced two-leg LIMIT IOC execution, retries, reconciliation
  risk.py             daily hard limits
  symbol_filters.py   exchangeInfo filters, stepSize/tickSize rounding
  instance.py         one strategy instance (shared by the bot and the replay)
  replay.py           historical replay + per-year report
  reference.py        backtest reference ranges
  bot.py              orchestrator and CLI (run, status, init-balance, replay)
dashboard/            FastAPI app + static/index.html
tools/                synthetic data, in-process fake exchange, fake Binance server (dev only)
tests/                pytest suite
```

---

## Development and tests

```bash
pip install -r requirements-dev.txt
python -m pytest
python -m tools.synthetic --days 30 --out synthetic.csv     # synthetic data for trying the replay
```

The suite covers the overlay state machine (entry, exit, flip, stale bars, failsafe), ladder
hysteresis (escalate, flip, hold, neutral), weight clipping, trade sizing and
`stepSize`/`tickSize` rounding, sliced execution, partial fills, leg-failure handling, risk
limits, the kill switch, idempotent client order ids and restart reconciliation, atomic state
writes, gap backfill, a replay smoke test (CSV and multi-sheet XLSX) and the dashboard API.

---

## Known risks

- The edge was measured on 2023-2026 data with parameters chosen after seeing that data; expect
  live results toward the pessimistic end.
- Trending ratio regimes (e.g. early 2024) reduce returns.
- Combined weights can reach 5%/95% concentration (set `W_MIN=0.20`, `W_MAX=0.80` for a more
  conservative range).
- TFUEL/USDT liquidity is thin (2026 median about 187k USD per day); capacity falls if volume
  keeps declining.
- The strategy increases token counts relative to holding. It does not protect against USD price
  declines of THETA/TFUEL.
- Frequent automated trading may affect tax treatment (e.g. Swiss professional-trader
  classification); check with a tax advisor.
- Deposits or withdrawals in a live sub-account distort its HODL benchmark; avoid them while live
  (or end and restart the live period).
- This is not financial advice.

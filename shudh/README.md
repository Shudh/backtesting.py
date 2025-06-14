# Live Trading Tutorial

This directory demonstrates how to run a simple Exponential Moving Average (EMA) crossover strategy using the minimal live trading loop provided in `live_trade.py`.

## Use Case

1. **Backtest a strategy** on historical data to verify it behaves as expected.
2. **Start a live trading session** that can receive new price bars over time.
3. **Feed real or mocked market data** into the live session.
4. **Generate trade signals** when EMAs cross and record the resulting performance.

## Sequence Diagram

```
Historical Data -> Backtest.run()
                 -> statistics

Incoming Bar ---┐
                v
         LiveTrade.update_kline() ---> queue
                |                      |
                v                      |
         worker thread ---------------┘
                |
                v
        LiveTrade._process_kline()
                |
                v
          Strategy.next() -> Broker -> Trades
```

The `update_kline()` function enqueues new OHLCV rows. A background thread consumes the queue, appends the rows to its internal DataFrame and calls the strategy. Orders are executed by the in‑memory broker.

## Files

- **`live_trade.py`** – Minimal implementation of a live loop using `Backtest` components.
- **`ema_live.py`** – Example script that backtests and then runs the EMA crossover strategy live using mocked data.
- **`original_ref_live_trade.py`** – Reference implementation that includes extensive logging and data management features.
- **`deviations_from_reference.md`** – Notes on how `live_trade.py` differs from the reference implementation.

## Usage

Run the example from the repository root:

```bash
python -m shudh.ema_live
```

The script first prints the backtest results and then starts streaming the remaining rows of the GOOG dataset into the live engine. Once the feed finishes, the script prints live trading statistics.


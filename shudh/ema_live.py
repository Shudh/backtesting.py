import threading
import time

import pandas as pd

from backtesting import Backtest, Strategy
from backtesting.lib import crossover
from backtesting.test import GOOG

from .live_trade import LiveTrade


def EMA(series: pd.Series, period: int) -> pd.Series:
    """Return exponential moving average."""
    return series.ewm(span=period, adjust=False).mean()


class EmaCross(Strategy):
    def init(self):
        close = self.data.Close
        self.ema_fast = self.I(EMA, close, 10)
        self.ema_slow = self.I(EMA, close, 30)

    def next(self):
        if crossover(self.ema_fast, self.ema_slow):
            self.buy()
        elif crossover(self.ema_slow, self.ema_fast):
            self.sell()


def mock_stream(data: pd.DataFrame, live: LiveTrade, start: int, end: int, delay: float = 0.1):
    """Feed data rows to ``live`` with a time delay."""
    for i in range(start, end):
        row = data.iloc[i : i + 1]
        live.update_kline(row)
        time.sleep(delay)
    live.set_state(LiveTrade._ST_OVER)


if __name__ == "__main__":
    bt = Backtest(GOOG, EmaCross, commission=0.002, exclusive_orders=True)
    stats = bt.run()
    print("Backtest results:\n", stats)  # noqa: T201

    start_idx = 50
    bt_live = Backtest(GOOG.iloc[:start_idx], EmaCross, commission=0.002, exclusive_orders=True)
    live = LiveTrade(bt_live)
    live.run()

    feeder = threading.Thread(target=mock_stream, args=(GOOG, live, start_idx, len(GOOG)))
    feeder.start()
    feeder.join()
    live.stop()

    print("Live results:\n", live.get_stats())  # noqa: T201

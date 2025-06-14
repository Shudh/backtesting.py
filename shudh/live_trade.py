from __future__ import annotations

import queue
import threading

import numpy as np
import pandas as pd

from backtesting import Backtest
from backtesting._stats import compute_stats
from backtesting._util import (
    _Data,
    _indicator_warmup_nbars,
    _strategy_indicators,
    try_,
)
from backtesting.backtesting import Strategy, _Broker, _OutOfMoneyError


class _DataPatch(_Data):
    """Mutable wrapper around :class:`_Data` to append rows."""

    def append_data(self, new_df: pd.DataFrame) -> None:
        df = self.raw_data_df
        df.loc[new_df.index] = new_df
        self._set_length(len(df))
        self._update()

    @property
    def raw_data_df(self) -> pd.DataFrame:
        return self._Data__df


class LiveTrade:
    """Lightweight live-trading loop using `Backtest` components."""

    _ST_OVER = 1

    def __init__(self, backtest: Backtest) -> None:
        self.backtest = backtest
        self.queue: queue.Queue[pd.DataFrame] = queue.Queue()
        self._state = 0
        self.thread: threading.Thread | None = None

    def run(self, **kwargs) -> None:
        self.data = _DataPatch(self.backtest._data.copy(deep=False))
        self.broker: _Broker = self.backtest._broker(data=self.data)
        self.strategy: Strategy = self.backtest._strategy(self.broker, self.data, kwargs)
        self.strategy.init()
        self.data._update()

        indicator_attrs = _strategy_indicators(self.strategy)
        self._current_idx = 1 + _indicator_warmup_nbars(self.strategy)
        self._run_next(
            start=self._current_idx,
            end=len(self.data.raw_data_df),
            indicator_attrs=indicator_attrs,
        )

        self.thread = threading.Thread(target=self._run_thread, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Signal the worker thread to finish and wait for it."""
        self.set_state(self._ST_OVER)
        if self.thread is not None:
            self.thread.join()

    def _run_thread(self) -> None:
        while self._state != self._ST_OVER:
            try:
                kline = self.queue.get(timeout=1)
            except queue.Empty:
                continue
            self._process_kline(kline)

    def _process_kline(self, kline: pd.DataFrame) -> None:
        old_len = len(self.data.raw_data_df)
        self.data.append_data(kline)
        self.strategy.init()
        self.data._update()
        indicator_attrs = _strategy_indicators(self.strategy)

        added = len(self.data.raw_data_df) - old_len
        self.broker._equity = np.append(
            self.broker._equity,
            [self.broker._equity[-1]] * added,
        )
        self._run_next(
            start=old_len,
            end=len(self.data.raw_data_df),
            indicator_attrs=indicator_attrs,
        )

    def _run_next(self, start: int, end: int, indicator_attrs) -> None:
        with np.errstate(invalid="ignore"):
            for i in range(start, end):
                self.data._set_length(i + 1)
                for attr, indicator in indicator_attrs:
                    setattr(self.strategy, attr, indicator[..., : i + 1])
                try:
                    self.broker.next()
                except _OutOfMoneyError:
                    break
                self.strategy.next()
        self.data._set_length(len(self.data.raw_data_df))

    def update_kline(self, kline: pd.DataFrame) -> None:
        self.queue.put(kline)

    def set_state(self, state: int) -> None:
        self._state = state

    def get_stats(self):
        if self._current_idx < len(self.data.raw_data_df):
            try_(self.broker.next, exception=_OutOfMoneyError)
        equity = (
            pd.Series(self.broker._equity).bfill().fillna(self.broker._cash).values
        )
        return compute_stats(
            trades=self.broker.closed_trades,
            equity=equity,
            ohlc_data=self.data.raw_data_df,
            risk_free_rate=0.0,
            strategy_instance=self.strategy,
        )

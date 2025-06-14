# ruff: noqa
import threading
import time
import traceback
from collections import deque
from copy import copy

import numpy as np
import pandas as pd

from backtesting import Backtest
from backtesting._stats import compute_stats
from backtesting._util import (
    _Data,
    _indicator_warmup_nbars,
    _strategy_indicators,
    _tqdm,
    try_,
)
from backtesting.backtesting import Strategy, _Broker, _OutOfMoneyError

# \u914d\u7f6e\u65e5\u5fd7\u8bb0\u5f55\u5668
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


class _DataPatch(_Data):

    def update_data(self, new_df: pd.DataFrame, last_index=-1) -> int:
        """\u66f4\u65b0\u6570\u636e"""
        df = self.raw_data_df
        old_length = len(df)
        if not isinstance(new_df.index, pd.DatetimeIndex):
            raise ValueError("\u65b0\u6570\u636e\u7684\u7d22\u5f15\u5fc5\u987b\u662fpd.DatetimeIndex\u7c7b\u578b")
        if new_df.empty:
            logger.warning("\u6536\u5230\u7a7a\u7684K\u7ebf\u6570\u636e\uff0c\u8df3\u8fc7\u66f4\u65b0")
            return 0
        required_columns = set(df.columns)
        if not all(col in new_df.columns for col in required_columns):
            raise ValueError(f"\u65b0\u6570\u636e\u7f3a\u5c11\u5fc5\u8981\u7684\u5217: {required_columns}")
        last_ts = df.index[last_index]
        new_data = new_df[new_df.index > last_ts]
        if new_data.empty:
            return 0
        new_data = new_data.sort_index()
        for idx in new_data.index:
            if any(np.issubdtype(type(df.iloc[0, df.columns.get_loc(col)]), np.floating) for col in df.columns if len(df) > 0):
                df.loc[idx] = {k: float(v) for k, v in new_data.loc[idx].items() if k in df.columns}
            else:
                df.loc[idx] = {k: v for k, v in new_data.loc[idx].items() if k in df.columns}
        new_length = len(df)
        added_bars_len = new_length - old_length
        self._update()
        logger.debug(f"\u6570\u636e\u66f4\u65b0\u6210\u529f: \u539f\u957f\u5ea6={old_length}, \u65b0\u957f\u5ea6={new_length}, \u65b0\u589e={added_bars_len},raw_id={id(self.raw_data_df)} id={id(self.df)}")
        return added_bars_len

    def capture_data(self, last_index=-1) -> pd.DataFrame:
        logger.debug(f"\u6570\u636e\u622a\u53d6: raw_id={id(self.raw_data_df)} id={id(self.df)}")
        if last_index < -len(self.raw_data_df) or last_index >= len(self.raw_data_df):
            last_index = -1
        df = self.raw_data_df.iloc[last_index:]
        original_df = self.raw_data_df
        original_df.drop(labels=original_df.index, axis=0, inplace=True)
        for idx in df.index:
            original_df.loc[idx] = df.loc[idx]
        self._set_length(len(original_df))
        self._update()
        logger.debug(f"\u6570\u636e\u622a\u53d6\u6210\u529f: raw_id={id(self.raw_data_df)} id={id(self.df)}")
        return self.raw_data_df

    @property
    def raw_data_df(self) -> pd.DataFrame:
        return self._Data__df


class LiveTrade:
    _ST_START, _ST_LIVE, _ST_HISTORY, _ST_OVER = range(4)

    def __init__(self, backtest: Backtest) -> None:
        self.backtest = backtest
        self.backtest_data: pd.DataFrame = self.backtest._data.copy(deep=False)
        self.backtest_broker: _Broker = self.backtest._broker
        self.backtest_strategy: Strategy = self.backtest._strategy
        self._finalize_trades = bool(self.backtest._finalize_trades)
        self.q_live = deque()
        self.semaphore = threading.Semaphore(0)
        self._state = self._ST_START

    def run(self, **kwargs):
        self.data = _DataPatch(self.backtest_data)
        self.broker: _Broker = self.backtest_broker(data=self.data)
        self.strategy: Strategy = self.backtest_strategy(self.broker, self.data, kwargs)
        self._mirror_data = _DataPatch(self.backtest_data.copy(deep=False))
        self._mirror_strategy = self.backtest_strategy(self.broker, self._mirror_data, kwargs)
        self._mirror_strategy.init()
        self._mirror_data._update()
        indicator_attrs = _strategy_indicators(self._mirror_strategy)
        self.minimum_period = self._current_idx = 1 + _indicator_warmup_nbars(self._mirror_strategy)
        self._run_next(start=self._current_idx, end=len(self.data.raw_data_df), indicator_attrs=indicator_attrs)
        self.thread = threading.Thread(target=self._run_thread, kwargs=kwargs, daemon=True)
        self.thread.start()

    def _run_next(self, start: int, end: int, indicator_attrs):
        with np.errstate(invalid="ignore"):
            for i in _tqdm(range(start, end), desc=self.run.__qualname__, unit="bar", mininterval=2, miniters=100):
                self.data._set_length(self._current_idx + 1)
                self._current_idx = self._current_idx + 1
                for attr, indicator in indicator_attrs:
                    setattr(self.strategy, attr, indicator[..., : i + 1])
                try:
                    self.broker.next()
                except _OutOfMoneyError:
                    break
                self.strategy.next()
        self.data._set_length(len(self.data.raw_data_df))

    def _run_thread(self, **kwargs):
        logger.info("K\u7ebf\u5904\u7406\u7ebf\u7a0b\u5df2\u542f\u52a8")
        while True:
            try:
                if self._state == self._ST_OVER:
                    logger.info("\u6536\u5230\u9000\u51fa\u4fe1\u53f7\uff0cK\u7ebf\u5904\u7406\u7ebf\u7a0b\u7ed3\u675f")
                    return False
                try:
                    self.semaphore.acquire()
                    kline = self.q_live.popleft()
                    if isinstance(kline, pd.DataFrame) and not kline.empty:
                        logger.info(
                            f"\u4ece\u961f\u5217\u83b7\u53d6\u5230\u65b0\u7684K\u7ebf\u6570\u636e\uff0c\u65f6\u95f4\u8303\u56f4: {kline.index.min()} - {kline.index.max()}, \u6570\u636e\u91cf: {len(kline)}"
                        )
                    else:
                        logger.warning(f"\u4ece\u961f\u5217\u83b7\u53d6\u5230\u5f02\u5e38\u6570\u636e: {type(kline)}")
                except IndexError:
                    logger.debug("\u961f\u5217\u4e3a\u7a7a\uff0c\u7b49\u5f85\u65b0\u6570\u636e...")
                    time.sleep(1)
                    continue
                logger.info(f"\u5f00\u59cb\u5904\u7406K\u7ebf\u6570\u636e\uff0c\u6570\u636e\u91cf: {len(kline) if isinstance(kline, pd.DataFrame) else 'unknown'}")
                self._process_kline(kline)
                logger.info("K\u7ebf\u6570\u636e\u5904\u7406\u5b8c\u6210")
            except Exception as e:
                logger.exception(f"run_thread\u5f02\u5e38: {e}\n{traceback.format_exc()}")

    def _process_kline(self, kline: pd.DataFrame):
        logger.info(f"\u5f00\u59cb\u5904\u7406K\u7ebf\u6570\u636e\uff0c\u65f6\u95f4\u8303\u56f4: {kline.index.min()} - {kline.index.max()}")
        old_len = len(self.data.raw_data_df)
        self.data.update_data(kline)
        new_len = len(self.data.raw_data_df)
        logger.info(f"\u4e3b\u6570\u636e\u66f4\u65b0: \u539f\u957f\u5ea6={old_len}, \u65b0\u957f\u5ea6={new_len}, \u65b0\u589e={new_len - old_len}")
        old_mirror_len = len(self._mirror_data.raw_data_df)
        added_bars_len = self._mirror_data.update_data(kline)
        new_mirror_len = len(self._mirror_data.raw_data_df)
        logger.info(f"\u955c\u50cf\u6570\u636e\u66f4\u65b0: \u539f\u957f\u5ea6={old_mirror_len}, \u65b0\u957f\u5ea6={new_mirror_len}, \u65b0\u589e={added_bars_len}")
        self._mirror_data.capture_data(-(len(kline) + self.minimum_period))
        raw_data_df = self._mirror_data.raw_data_df
        logger.info(f"\u955c\u50cf\u6570\u636e\u622a\u53d6\u540e\u957f\u5ea6: {len(raw_data_df)}")
        self._mirror_strategy.init()
        indicator_attrs = _strategy_indicators(self._mirror_strategy)
        logger.info(f"\u6307\u6807\u91cd\u65b0\u8ba1\u7b97\u5b8c\u6210\uff0c\u6307\u6807\u6570\u91cf: {len(indicator_attrs)}")
        self.broker._equity = np.append(self.broker._equity, [self.broker._equity[-1]] * added_bars_len)
        logger.info(f"\u5f00\u59cb\u8fd0\u884c\u7b56\u7565\uff0c\u6570\u636e\u8303\u56f4: {self.minimum_period} - {len(raw_data_df)}")
        self._run_next(start=self.minimum_period, end=len(raw_data_df), indicator_attrs=indicator_attrs)
        logger.info("\u7b56\u7565\u8fd0\u884c\u5b8c\u6210")

    def update_kline(self, kline: pd.DataFrame):
        self.q_live.append(kline)
        self.semaphore.release()

    def set_state(self, state):
        self._state = state

    def get_stats(self):
        if self._finalize_trades is True:
            for trade in reversed(self.broker.trades):
                trade.close()
            if self._current_idx < len(self.data.raw_data_df):
                try_(self.broker.next, exception=_OutOfMoneyError)
        equity = pd.Series(self.broker._equity).bfill().fillna(self.broker._cash).values
        results = compute_stats(
            trades=self.broker.closed_trades,
            equity=equity,
            ohlc_data=self.backtest_data,
            risk_free_rate=0.0,
            strategy_instance=self.strategy,
        )
        return results

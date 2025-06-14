# Deviations from Reference Live Trading Code

This project includes two live trading implementations:

* `original_ref_live_trade.py` – verbose reference version with logging, data capture and a semaphore/deque queue.
* `live_trade.py` – simplified version used in the tutorial.

Key differences and their impact:

1. **Queue Handling**
   - *Reference:* uses a `deque` with a `Semaphore` to coordinate producer and consumer threads.
   - *Current:* relies on `queue.Queue` which provides built‑in blocking operations.
   - *Impact:* fewer lines of code and safer thread communication, though we lose explicit control over semaphore counts.

2. **Data Updates**
   - *Reference:* `_DataPatch.update_data` iterates row by row and performs type checks. It also includes a `capture_data` method to trim historical data.
   - *Current:* `_DataPatch.append_data` simply assigns new rows and refreshes cached arrays. No trimming is performed.
   - *Impact:* faster but lacks the ability to drop old data or validate column types.

3. **Indicator Recalculation**
   - *Reference:* maintains a mirror strategy, recalculating indicators each time new data arrives and capturing only a recent window of bars.
   - *Current:* reinitializes the existing strategy and recalculates indicators directly on the updated DataFrame.
   - *Impact:* less complexity but might be slower for very large datasets because indicators are recomputed from scratch.

4. **Logging and State Machine**
   - *Reference:* extensive logging statements and multiple states (`_ST_START`, `_ST_LIVE`, `_ST_HISTORY`, `_ST_OVER`).
   - *Current:* only two states (running and over) and minimal logging.
   - *Impact:* easier to read but provides less visibility into the live process.

5. **Shutdown Logic**
   - *Reference:* caller manually signals the thread and joins it.
   - *Current:* exposes a `stop()` helper that sets the state and joins the thread.
   - *Impact:* simpler API for stopping live trading.

Overall, `live_trade.py` aims to demonstrate the minimal steps needed to append bars and execute a strategy live. The reference version contains additional features that may be useful for production use, such as detailed logging and data management.


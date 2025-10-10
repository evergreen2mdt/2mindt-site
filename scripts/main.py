import os
import sys
import time
import math
from datetime import datetime

# === Gap Analysis ===
from gap_functions import get_ib_data, run_gap_analysis_for_contracts
from config import TICKER_MAP
# === Options / Market (use cowpie) ===
from options_functions import run_options, is_market_open
from config import DEFAULT_START_DATE
from scripts.config import TICKER_MAP
# === Volume / Timebands ===
from volume_functions import run_timebands_30m

# Loop cadence in seconds
from config import TARGET_LOOP_SECONDS



def main():
    # === Step 0: Historical gap analysis ===
    try:
        dfs = get_ib_data(list(TICKER_MAP.keys()), start_date=DEFAULT_START_DATE)
        results = run_gap_analysis_for_contracts(TICKER_MAP, dfs)
        print("Gap analysis complete:", results)
    except Exception as e:
        print(f"Gap analysis failed: {e}")

    # === Step 1: Market open check ===
    bypass = "n"
    if not is_market_open():
        bypass = input("Market Closed. Run Anyway? (y/n): ").strip().lower()
        if bypass != "y":
            print("Ending Program")
            return

    # === Unified loop ===
    while is_market_open() or bypass == "y":
        start = time.monotonic()

        # Step 2: Options pipeline (cowpie)
        try:
            run_options()
        except Exception as e:
            print(f"run_options() failed: {e}")


        # Step 3: Timebands
        try:
            for ticker in TICKER_MAP:
                run_timebands_30m(
                    ticker,
                    days=20,
                    include_rth=True,
                    include_eth=True,
                )
            print("Timebands updated for all tickers")
        except Exception as e:
            print(f"Timebands update failed: {e}")

        # === Sleep until next cadence ===
        loop_secs = time.monotonic() - start
        sleep_secs = max(0.0, TARGET_LOOP_SECONDS - loop_secs)
        print(f"Loop finished in {loop_secs:.2f} seconds. Sleeping for {sleep_secs:.2f} seconds...")

        remaining = int(math.ceil(sleep_secs))
        while remaining > 0 and (is_market_open() or bypass == "y"):
            sys.stdout.write(f"\rSleeping {remaining:>3d}s ")
            sys.stdout.flush()
            time.sleep(1)
            remaining -= 1
        sys.stdout.write("\rSleeping   0s \n")
        sys.stdout.flush()

    print("Market closed. Exiting script.")


if __name__ == "__main__":
    main()

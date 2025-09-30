# main_shit.py

import os
import sys
import time
import math
from datetime import datetime

# === Gap Analysis ===
from gap_functions import (get_ib_data, CONTRACT_MAP, run_gap_analysis_for_contracts)
from main import TARGET_LOOP_SECONDS

# === Options / Market (cowpie replaces options_functions) ===
from options_functions import run_options, is_market_open,

# === Volume / Timebands ===
from volume_functions import run_timebands_30m, VOLUME_PATH


def main():
    # === Step 0: Historical gap analysis ===
    try:
        dfs = get_ib_data(list(CONTRACT_MAP.keys()), start_date="2003-01-01")
        results = run_gap_analysis_for_contracts(CONTRACT_MAP, dfs)
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
            print(f"run_cowpie() failed: {e}")

        # Step 3: Timebands
        try:
            run_timebands_30m(
                path=VOLUME_PATH,
                days=20,
                include_rth=True,
                include_eth=True,
            )
            print(f"Timebands updated → {VOLUME_PATH}")
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

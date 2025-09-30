from ib_insync import IB
import pandas as pd
import os
from datetime import datetime
from archived_gap_functions import (
    create_days_to_target_summary_table_gap_types,
    get_ib_spy_data,
    compute_gap_data,
    calculate_statistics,
    calculate_drawdown,
    compute_gap_data_with_mam_and_days_to_mam,
    save_to_excel,
    commit_to_sqlite,
    #create_days_to_hit_summary_by_gap_type_structured
)
from dotenv import load_dotenv
load_dotenv()


def run_gap_analysis(start_date="2003-01-01"):
    ib = IB()

    # === Pull SPY OHLCV ===
    df = get_ib_spy_data(start_date=start_date)

    # === Correct known bad rows by date ===
    corrections = {
        "2004-04-19": {"open": 113.55, "high": 113.99, "low": 113.27, "close": 113.83, "volume": 28277600},
        "2004-04-27": {"open": 114.23, "high": 115.12, "low": 113.96, "close": 114.30, "volume": 43485500},
        "2004-05-05": {"open": 112.41, "high": 112.96, "low": 112.16, "close": 112.78, "volume": 34405000},
        "2006-03-03": {"open": 128.67, "high": 130.07, "low": 128.65, "close": 128.76, "volume": 73402496},
        "2006-05-18": {"open": 127.35, "high": 127.75, "low": 126.11, "close": 126.21, "volume": 37906304},
        "2006-03-23": {"open": 130.26, "high": 130.39, "low": 129.66, "close": 130.11, "volume": 46704200},
        "2007-04-02": {"open": 142.16, "high": 142.46, "low": 140.89, "close": 142.16, "volume": 79454680},
        "2007-05-25": {"open": 151.49, "high": 152.02, "low": 151.18, "close": 151.69, "volume": 33309621},
        "2007-06-29": {"open": 150.90, "high": 151.65, "low": 149.15, "close": 150.43, "volume": 39701730},
        "2007-07-23": {"open": 154.18, "high": 154.72, "low": 153.30, "close": 153.97, "volume": 21183836},
        "2007-07-26": {"open": 150.19, "high": 150.80, "low": 146.39, "close": 148.02, "volume": 57592490},
        "2007-08-10": {"open": 144.39, "high": 146.50, "low": 143.12, "close": 144.71, "volume": 11018370},
        "2022-01-24": {"open": 432.03, "high": 440.38, "low": 420.76, "close": 439.84, "volume": 52496719},
        "2022-01-26": {"open": 440.72, "high": 444.04, "low": 428.86, "close": 433.38, "volume": 36391088},
        "2022-02-24": {"open": 411.02, "high": 428.76, "low": 410.64, "close": 428.30, "volume": 13942946},
        "2022-05-04": {"open": 417.08, "high": 429.66, "low": 413.71, "close": 429.06, "volume": 44247895},
        "2022-05-05": {"open": 424.55, "high": 425.00, "low": 409.44, "close": 413.81, "volume": 72929106},
        "2022-05-20": {"open": 393.25, "high": 397.03, "low": 380.54, "close": 389.63, "volume": 31432197},
        "2022-10-13": {"open": 349.21, "high": 367.51, "low": 348.11, "close": 365.97, "volume": 47254518},
        "2024-08-01": {"open": 552.57, "high": 554.87, "low": 539.43, "close": 543.01, "volume": 76428732},
        "2024-09-11": {"open": 548.70, "high": 555.36, "low": 539.96, "close": 554.42, "volume": 75248608},
        "2024-12-18": {"open": 603.98, "high": 606.41, "low": 585.89, "close": 586.28, "volume": 38248729},
        "2025-03-03": {"open": 596.18, "high": 597.34, "low": 579.90, "close": 583.77, "volume": 74249199},
        "2025-04-04": {"open": 523.67, "high": 525.87, "low": 505.06, "close": 505.28, "volume": 17965131},
        "2025-04-07": {"open": 489.19, "high": 523.17, "low": 481.80, "close": 504.38, "volume": 56611355},
        "2025-04-08": {"open": 521.86, "high": 524.98, "low": 489.16, "close": 496.48, "volume": 55816581},
        "2025-04-09": {"open": 493.44, "high": 548.62, "low": 493.05, "close": 548.62, "volume": 11867317},
        "2025-04-10": {"open": 532.17, "high": 533.50, "low": 509.32, "close": 524.58, "volume": 52331225},
        "2025-04-11": {"open": 523.01, "high": 536.43, "low": 520.07, "close": 533.94, "volume": 37866334},
        "2025-04-30": {"open": 547.57, "high": 556.52, "low": 541.52, "close": 554.54, "volume": 33101463}
    }

    df["date"] = pd.to_datetime(df["date"])
    for date_str, values in corrections.items():
        mask = df["date"] == pd.to_datetime(date_str)
        for col, val in values.items():
            df.loc[mask, col] = val

    if df is None or df.empty:
        raise ValueError("SPY DataFrame is empty or None.")

    print("✅ SPY data retrieved from IB")

    # === Compute data ===
    df = compute_gap_data(df)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["target_achieved_date"] = pd.to_datetime(df["target_achieved_date"]).dt.normalize()
    df = calculate_drawdown(df)
    df = calculate_statistics(df)
    df = compute_gap_data_with_mam_and_days_to_mam(df)

    # === Save to Excel ===
    output_path = r"C:\2mdt\2mindt-site\public\spy gap analysis.xlsx"
    save_to_excel(df, output_path, calculate_statistics=lambda x: x)

    # === Summary tables ===
    #create_days_to_target_summary_table_cat_key(output_path)
    create_days_to_target_summary_table_gap_types(output_path)
    #create_days_to_hit_summary_by_gap_type_structured(output_path)

    # === Upload '%% Days to Hit by Gap Types' ===
    #print("✅ Uploading only '%% Days to Hit by Gap Types' to Supabase...")
    #df_days_to_hit = pd.read_excel(output_path, sheet_name="%% Days to Hit by Gap Types").dropna(how="all")
    #df_clean = df_days_to_hit.replace([float('inf'), float('-inf')], pd.NA)
    #df_clean = df_clean.where(pd.notna(df_clean), None)

    #supabase.table("gaps_days_to_hit_by_gap_type").delete().neq("gap_type", "").execute()
    #supabase.table("gaps_days_to_hit_by_gap_type").insert(df_clean.to_dict(orient="records")).execute()

   #print("✅ Upload complete.")

    # === Rolling stats and SQLite ===
    #compute_dynamic_index_rolling_stats(output_path)
    #create_cat_key_summary_sheets_with_tables(df, output_path)
    ib.disconnect()
    commit_to_sqlite(df, r"C:\2mdt\2mindt-site\data\spy_gap_analysis.db", table_name="spy_gap_analysis")




# === Example Call ===
if __name__ == "__main__":
    run_gap_analysis()

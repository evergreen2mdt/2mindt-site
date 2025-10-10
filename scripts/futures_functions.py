import os

from ib_insync import IB, Future
import pandas as pd
import time

def fetch_volume_bars(ib, contract, duration='20 D', bar_size='30 mins'):
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1
        )
        df = pd.DataFrame(bars)
        if df.empty:
            return None
        return df
    except Exception:
        return None

def list_all_es_contracts_with_volume_to_excel(output_path="es_futures_volume_summary.xlsx"):
    ib = IB()
    cid = abs(hash("MES")) % 9000
    ib.connect("127.0.0.1", 7496, clientId=cid)

    es = Future(symbol='MES', exchange='CME', currency='USD')
    contracts = ib.reqContractDetails(es)
    if not contracts:
        print("No ES futures contracts found.")
        return

    summary_rows = []
    no_volume_list = []
    excel_writer = pd.ExcelWriter(output_path, engine='openpyxl')

    for detail in contracts:
        c = detail.contract
        print(f"Checking volume for {c.localSymbol}")
        df = fetch_volume_bars(ib, c)
        time.sleep(0.5)

        if df is not None:
            # Drop timezone info so Excel can handle it
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)

            total_volume = df['volume'].sum()
            summary_rows.append({
                "Symbol": c.symbol,
                "LocalSymbol": c.localSymbol,
                "Expiry": c.lastTradeDateOrContractMonth,
                "Exchange": c.exchange,
                "Currency": c.currency,
                "ConId": c.conId,
                "Total_Volume": total_volume
            })
            df[['date', 'volume']].to_excel(excel_writer,
                                            sheet_name=c.localSymbol[:31],
                                            index=False)
            print(f"[{c.localSymbol}] Total Volume: {total_volume}")
        else:
            no_volume_list.append(c.localSymbol)
            print(f"[{c.localSymbol}] Failed: no volume")

    if summary_rows:
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_excel(excel_writer, sheet_name="Volume Summary", index=False)
        print(f"\nTotal contracts with volume: {len(df_summary)}")
        print(df_summary[['LocalSymbol', 'Total_Volume']])

    if no_volume_list:
        df_no_vol = pd.DataFrame({"No Volume Contracts": no_volume_list})
        df_no_vol.to_excel(excel_writer, sheet_name="No Volume", index=False)
        print(f"\nContracts with no volume: {len(no_volume_list)}")
        print(no_volume_list)

    excel_writer.close()
    os.startfile(output_path)
    ib.disconnect()
    print(f"\n✅ Results saved to: {output_path}")

if __name__ == "__main__":
    list_all_es_contracts_with_volume_to_excel("es_futures_volume_summary.xlsx")

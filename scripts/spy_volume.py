import yfinance as yf
import pandas as pd
from datetime import datetime

def get_spy_10min_volume():
    """
    Returns the total SPY volume over the most recent 10 minutes,
    along with the current spot price and timestamp.
    """
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1d", interval="1m")
        if len(hist) < 10:
            return None, None, None

        last_10min = hist.iloc[-10:]
        volume_10min = last_10min["Volume"].sum()
        spot = last_10min["Close"].iloc[-1]
        timestamp = last_10min.index[-1].to_pydatetime()

        return volume_10min, spot, timestamp

    except Exception as e:
        print(f"[Error fetching SPY volume]: {e}")
        return None, None, None

if __name__ == "__main__":
    vol, spot, time = get_spy_10min_volume()
    print(f"Time: {time}, Spot: {spot}, 10-min Volume: {vol}")
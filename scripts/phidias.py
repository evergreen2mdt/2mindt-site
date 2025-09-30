import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# === Config ===
START_BALANCE = 50_000
MAX_DRAWDOWN = 2_500
MES_PER_10K = 1
POINT_VALUE = 5  # $ per point per MES

# === Helpers ===
def get_spy_closes(start_date: str, end_date: str) -> pd.DataFrame:
    spy = yf.Ticker("SPY")
    hist = spy.history(start=start_date, end=end_date, interval="1d")
    hist = hist.tz_localize(None)
    hist = hist[['Close']].rename(columns={'Close': 'close'})
    hist.index = pd.to_datetime(hist.index.date)
    return hist

class PhidiasTracker:
    def __init__(self, start_balance=START_BALANCE):
        self.start_balance = start_balance
        self.trades = []
        self.balance = start_balance
        self.threshold = start_balance - MAX_DRAWDOWN
        self.equity_high = start_balance

    def log_trade(self, entry_date, entry_price, exit_date, exit_price, contracts):
        entry_date = pd.to_datetime(entry_date).date()
        exit_date = pd.to_datetime(exit_date).date()

        # get closes during trade period
        closes = get_spy_closes(entry_date, exit_date)
        closes = closes.loc[(closes.index >= pd.to_datetime(entry_date)) & (closes.index <= pd.to_datetime(exit_date))]

        # P&L (short if entry > exit)
        pts = entry_price - exit_price
        pnl = pts * contracts * POINT_VALUE

        # drawdown = worst close against position
        if entry_price > exit_price:  # short trade
            adverse = closes['close'].max() - entry_price
        else:  # long trade
            adverse = entry_price - closes['close'].min()
        drawdown = max(0, adverse * contracts * POINT_VALUE)

        # update account
        self.balance += pnl
        self.equity_high = max(self.equity_high, self.balance)
        self.threshold = self.equity_high - MAX_DRAWDOWN

        self.trades.append({
            'entry_date': entry_date,
            'exit_date': exit_date,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'contracts': contracts,
            'pts': pts,
            'pnl': pnl,
            'days_held': (exit_date - entry_date).days,
            'drawdown': drawdown,
            'end_balance': self.balance,
            'threshold': self.threshold
        })

    def evaluate(self):
        df = pd.DataFrame(self.trades)
        total_pnl = df['pnl'].sum() if not df.empty else 0
        rule_checks = {
            'profit_target_met': total_pnl >= 4000,
            'max_drawdown_ok': all(df['end_balance'] >= df['threshold']),
            'min_trading_days': len(df) >= 3,
            'contract_limit': all(df['contracts'] <= 100),
        }
        return df, rule_checks


if __name__ == "__main__":
    tracker = PhidiasTracker()
    # Example usage
    tracker.log_trade("2025-08-12", 6456.75, "2025-08-20", 6288.00, contracts=5)
    tracker.log_trade("2025-08-22", 6485.00, "2025-08-27", 6391.75, contracts=5)

    df, rules = tracker.evaluate()
    print(df)
    print("\nRule Checks:")
    for k, v in rules.items():
        print(f"{k}: {v}")

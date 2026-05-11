# scratch/xqq_random_forest_strategy.py

import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report


# =========================
# 1. LOAD DATA
# =========================

df = pd.read_csv("./xqq_full_history_with_vix_spy.csv")

df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date").reset_index(drop=True)


# =========================
# 2. FEATURE ENGINEERING
# =========================

df["Return_1D"] = df["Close"].pct_change(1)
df["Return_5D"] = df["Close"].pct_change(5)
df["Return_20D"] = df["Close"].pct_change(20)
df["Return_30D"] = df["Close"].pct_change(30)
df["Future_Return_30D"] = df["Close"].shift(-30) / df["Close"] - 1

df["MA20"] = df["Close"].rolling(20).mean()
df["MA60"] = df["Close"].rolling(60).mean()
df["MA120"] = df["Close"].rolling(120).mean()

df["Price_vs_MA20"] = df["Close"] / df["MA20"]
df["Price_vs_MA60"] = df["Close"] / df["MA60"]
df["Price_vs_MA120"] = df["Close"] / df["MA120"]

df["Volatility_20"] = df["Return_1D"].rolling(20).std()

df["Volume_MA20"] = df["Volume"].rolling(20).mean()
df["Volume_Ratio"] = df["Volume"] / df["Volume_MA20"]

df["VIX_MA20"] = df["VIX"].rolling(20).mean()
df["VIX_Ratio"] = df["VIX_Ratio"]
df["VIX_Change_10D"] = df["VIX"].pct_change(10)
df["VIX_Zscore"] = df["VIX_Zscore"]

df["SPY_MA200"] = df["SPY_MA200"]
df["SPY_Trend"] = df["SPY_Trend"]
df["SPY_Return_20D"] = df["SPY_Return_20D"]

df["Momentum_10"] = df["Close"] / df["Close"].shift(10)
df["Momentum_30"] = df["Close"] / df["Close"].shift(30)

# RSI
delta = df["Close"].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

avg_gain = gain.rolling(14).mean()
avg_loss = loss.rolling(14).mean()

rs = avg_gain / avg_loss
df["RSI14"] = 100 - (100 / (1 + rs))


# =========================
# 3. TARGET
# =========================

# 未来30天收益
df["Target"] = np.nan
df["Future_Return_30D"] = df["Close"].shift(-30) / df["Close"] - 1

# 用 rolling threshold（动态）
df["Rolling_Mean"] = df["Future_Return_30D"].rolling(252).mean()

df["Target"] = (df["Future_Return_30D"] > df["Rolling_Mean"]).astype(int)


# =========================
# 4. CLEAN DATA
# =========================

features = [
    "Return_1D",
    "Return_5D",
    "Return_20D",
    "Return_30D",
    "Price_vs_MA20",
    "Price_vs_MA60",
    "Price_vs_MA120",
    "Volatility_20",
    "Volume_Ratio",
    "Momentum_10",
    "Momentum_30",
    "VIX_Ratio",
    "VIX_MA20",
    "VIX_Change_10D",
    "VIX_Zscore",
    # "SPY_MA200",
    # "SPY_Trend",
    # "SPY_Return_20D",
    "RSI14",
]

raw_df = df.copy()

model_df = raw_df.dropna(
    subset=features + ["Future_Return_30D", "Target", "Rolling_Mean"]
).reset_index(drop=True)


# =========================
# 5. TRAIN / TEST SPLIT
# =========================

split_index = int(len(model_df) * 0.8)

train_df = model_df.iloc[:split_index].copy()
test_df = model_df.iloc[split_index:].copy()

X_train = train_df[features]
y_train = train_df["Target"]

X_test = test_df[features]
y_test = test_df["Target"]


# =========================
# 6. RANDOM FOREST MODEL
# =========================

model = RandomForestClassifier(
    n_estimators=500,
    max_depth=6,
    min_samples_leaf=10,
    random_state=42,
)

model.fit(X_train, y_train)

pred = model.predict(X_test)
proba = model.predict_proba(X_test)[:, 1]

print("\n=== Classification Report ===\n")
print(classification_report(y_test, pred))


# =========================
# 7. SIGNALS / POSITION SIZING
# =========================

# =========================
# 7. SIGNALS / POSITION SIZING
# =========================

bt = test_df.copy().reset_index(drop=True)

bt["Prediction"] = pred
bt["Up_Probability"] = proba

# bt["Signal"] = "NEUTRAL"
# bt["Position"] = 0.8

# bt.loc[bt["Up_Probability"] >= 0.60, "Signal"] = "STRONG_BUY"
# bt.loc[bt["Up_Probability"] >= 0.60, "Position"] = 1.0

# bt.loc[
#     (bt["Up_Probability"] >= 0.40) & (bt["Up_Probability"] < 0.60),
#     "Signal"
# ] = "BUY"
# bt.loc[
#     (bt["Up_Probability"] >= 0.40) & (bt["Up_Probability"] < 0.60),
#     "Position"
# ] = 0.9

# bt.loc[
#     (bt["Up_Probability"] >= 0.30) & (bt["Up_Probability"] < 0.40),
#     "Signal"
# ] = "NEUTRAL"
# bt.loc[
#     (bt["Up_Probability"] >= 0.30) & (bt["Up_Probability"] < 0.40),
#     "Position"
# ] = 0.8

# bt.loc[
#     (bt["Up_Probability"] >= 0.20) & (bt["Up_Probability"] < 0.30),
#     "Signal"
# ] = "REDUCE"
# bt.loc[
#     (bt["Up_Probability"] >= 0.20) & (bt["Up_Probability"] < 0.30),
#     "Position"
# ] = 0.5

# bt.loc[bt["Up_Probability"] < 0.20, "Signal"] = "RISK_OFF"
# bt.loc[bt["Up_Probability"] < 0.20, "Position"] = 0.3

bt["Position"] = (bt["Up_Probability"] > 0.5).astype(int)
bt["Signal"] = np.where(bt["Position"] == 1, "BUY", "SELL")

# =========================
# 8. BACKTEST
# =========================

bt["Market_Return"] = bt["Close"].pct_change()

# 用昨天的仓位吃今天的收益，避免未来函数
bt["Strategy_Return"] = bt["Position"].shift(1) * bt["Market_Return"]

bt["Cumulative_Market"] = (1 + bt["Market_Return"]).cumprod()
bt["Cumulative_Strategy"] = (1 + bt["Strategy_Return"]).cumprod()


# =========================
# 9. PERFORMANCE METRICS
# =========================

def max_drawdown(cumulative_returns):
    running_max = cumulative_returns.cummax()
    drawdown = cumulative_returns / running_max - 1
    return drawdown.min()


def annualized_return(cumulative_returns, periods_per_year=252):
    total_return = cumulative_returns.iloc[-1]
    n_periods = len(cumulative_returns)
    return total_return ** (periods_per_year / n_periods) - 1


def annualized_volatility(daily_returns, periods_per_year=252):
    return daily_returns.std() * np.sqrt(periods_per_year)


def sharpe_ratio(daily_returns, periods_per_year=252):
    vol = annualized_volatility(daily_returns, periods_per_year)
    if vol == 0 or np.isnan(vol):
        return np.nan
    return daily_returns.mean() * periods_per_year / vol


market_ann_return = annualized_return(bt["Cumulative_Market"].dropna())
strategy_ann_return = annualized_return(bt["Cumulative_Strategy"].dropna())

market_vol = annualized_volatility(bt["Market_Return"].dropna())
strategy_vol = annualized_volatility(bt["Strategy_Return"].dropna())

market_sharpe = sharpe_ratio(bt["Market_Return"].dropna())
strategy_sharpe = sharpe_ratio(bt["Strategy_Return"].dropna())

market_mdd = max_drawdown(bt["Cumulative_Market"].dropna())
strategy_mdd = max_drawdown(bt["Cumulative_Strategy"].dropna())


# =========================
# 10. FEATURE IMPORTANCE
# =========================

importance_df = pd.DataFrame({
    "Feature": features,
    "Importance": model.feature_importances_
}).sort_values("Importance", ascending=False)

print("\n=== Feature Importance ===\n")
print(importance_df)


# =========================
# 11. SAVE RESULTS
# =========================

output_cols = [
    "Date",
    "Close",
    "Prediction",
    "Up_Probability",
    "Signal",
    "Position",
    "Future_Return_30D",
    "Market_Return",
    "Strategy_Return",
    "Cumulative_Market",
    "Cumulative_Strategy",
]

output_file = "./xqq_rf_strategy_results.csv"

bt[output_cols].to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig"
)

importance_df.to_csv(
    "./xqq_rf_feature_importance.csv",
    index=False,
    encoding="utf-8-sig"
)

print(f"\nSaved strategy results to: {output_file}")
print("Saved feature importance to: ./xqq_rf_feature_importance.csv")


# =========================
# 12. FINAL PERFORMANCE
# =========================

final_market = bt["Cumulative_Market"].iloc[-1]
final_strategy = bt["Cumulative_Strategy"].iloc[-1]

print("\n=== Final Performance ===")
print(f"Buy & Hold Test Period: {final_market:.2f}x")
print(f"RF Strategy Test Period: {final_strategy:.2f}x")

print("\n=== Risk Metrics ===")
print(f"Buy & Hold Annual Return: {market_ann_return:.2%}")
print(f"RF Strategy Annual Return: {strategy_ann_return:.2%}")

print(f"Buy & Hold Volatility: {market_vol:.2%}")
print(f"RF Strategy Volatility: {strategy_vol:.2%}")

print(f"Buy & Hold Sharpe: {market_sharpe:.2f}")
print(f"RF Strategy Sharpe: {strategy_sharpe:.2f}")

print(f"Buy & Hold Max Drawdown: {market_mdd:.2%}")
print(f"RF Strategy Max Drawdown: {strategy_mdd:.2%}")

print("\n=== Signal Counts ===")
print(bt["Signal"].value_counts())

print("\n=== Average Position ===")
print(f"{bt['Position'].mean():.2%}")
# =========================
# 13. LIVE PREDICTION（最后30天）
# =========================

# =========================
# 13. LIVE PREDICTION（最后30天）
# =========================

live_df = raw_df.dropna(subset=features).copy()
live_df = live_df.tail(30).copy().reset_index(drop=True)

live_df["Up_Probability"] = model.predict_proba(live_df[features])[:, 1]

live_df["Signal"] = "NEUTRAL"
live_df["Position"] = 0.8

live_df.loc[live_df["Up_Probability"] >= 0.60, "Signal"] = "STRONG_BUY"
live_df.loc[live_df["Up_Probability"] >= 0.60, "Position"] = 1.0

live_df.loc[
    (live_df["Up_Probability"] >= 0.40) & (live_df["Up_Probability"] < 0.60),
    "Signal"
] = "BUY"
live_df.loc[
    (live_df["Up_Probability"] >= 0.40) & (live_df["Up_Probability"] < 0.60),
    "Position"
] = 0.9

live_df.loc[
    (live_df["Up_Probability"] >= 0.30) & (live_df["Up_Probability"] < 0.40),
    "Signal"
] = "NEUTRAL"
live_df.loc[
    (live_df["Up_Probability"] >= 0.30) & (live_df["Up_Probability"] < 0.40),
    "Position"
] = 0.8

live_df.loc[
    (live_df["Up_Probability"] >= 0.20) & (live_df["Up_Probability"] < 0.30),
    "Signal"
] = "REDUCE"
live_df.loc[
    (live_df["Up_Probability"] >= 0.20) & (live_df["Up_Probability"] < 0.30),
    "Position"
] = 0.5

live_df.loc[live_df["Up_Probability"] < 0.20, "Signal"] = "RISK_OFF"
live_df.loc[live_df["Up_Probability"] < 0.20, "Position"] = 0.3


# =========================
# 14. 保存最后30天预测
# =========================

live_output_cols = [
    "Date",
    "Close",
    "Up_Probability",
    "Signal",
    "Position",
]

live_output_file = "./xqq_rf_live_predictions.csv"

live_df[live_output_cols].to_csv(
    live_output_file,
    index=False,
    encoding="utf-8-sig"
)

print("\n=== Last 30 Days Predictions ===")
print(live_df[live_output_cols].tail())

print(f"\nSaved live predictions to: {live_output_file}")

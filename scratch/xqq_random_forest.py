# scratch/xqq_random_forest_strategy.py

import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split


# =========================
# 1. LOAD DATA
# =========================

df = pd.read_csv("/Users/wentaoxie/Stock-Monitor/scratch/xqq_full_history.csv")

df["Date"] = pd.to_datetime(df["Date"])

df = df.sort_values("Date").reset_index(drop=True)


# =========================
# 2. FEATURE ENGINEERING
# =========================

# Returns
df["Return_1D"] = df["Close"].pct_change(1)
df["Return_5D"] = df["Close"].pct_change(5)
df["Return_20D"] = df["Close"].pct_change(20)

# Moving averages
df["MA20"] = df["Close"].rolling(20).mean()
df["MA60"] = df["Close"].rolling(60).mean()

# MA ratios
df["Price_vs_MA20"] = df["Close"] / df["MA20"]
df["Price_vs_MA60"] = df["Close"] / df["MA60"]

# Volatility
df["Volatility_20"] = df["Return_1D"].rolling(20).std()

# Volume trend
df["Volume_MA20"] = df["Volume"].rolling(20).mean()
df["Volume_Ratio"] = df["Volume"] / df["Volume_MA20"]

# Momentum
df["Momentum_10"] = df["Close"] / df["Close"].shift(10)

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

# 未来5天收益
df["Future_Return_5D"] = df["Close"].shift(-5) / df["Close"] - 1

# 分类目标
# 1 = future up
# 0 = future down
df["Target"] = (df["Future_Return_5D"] > 0).astype(int)


# =========================
# 4. CLEAN DATA
# =========================

features = [
    "Return_1D",
    "Return_5D",
    "Return_20D",
    "Price_vs_MA20",
    "Price_vs_MA60",
    "Volatility_20",
    "Volume_Ratio",
    "Momentum_10",
    "RSI14",
]

df = df.dropna().reset_index(drop=True)


# =========================
# 5. TRAIN / TEST SPLIT
# =========================

split_index = int(len(df) * 0.8)

train_df = df.iloc[:split_index]
test_df = df.iloc[split_index:]

X_train = train_df[features]
y_train = train_df["Target"]

X_test = test_df[features]
y_test = test_df["Target"]


# =========================
# 6. RANDOM FOREST MODEL
# =========================

model = RandomForestClassifier(
    n_estimators=300,
    max_depth=8,
    min_samples_leaf=5,
    random_state=42,
)

model.fit(X_train, y_train)

pred = model.predict(X_test)

print("\n=== Classification Report ===\n")
print(classification_report(y_test, pred))


# =========================
# 7. GENERATE SIGNALS
# =========================

df.loc[test_df.index, "Prediction"] = pred

# Trading signal
df["Signal"] = np.where(df["Prediction"] == 1, "BUY", "SELL")


# =========================
# 8. BACKTEST
# =========================

# 持仓:
# BUY = 1
# SELL = 0
df["Position"] = np.where(df["Signal"] == "BUY", 1, 0)

# 下一天收益
df["Market_Return"] = df["Close"].pct_change()

# 策略收益
df["Strategy_Return"] = (
    df["Position"].shift(1) * df["Market_Return"]
)

# 累计收益
df["Cumulative_Market"] = (
    1 + df["Market_Return"]
).cumprod()

df["Cumulative_Strategy"] = (
    1 + df["Strategy_Return"]
).cumprod()


# =========================
# 9. FEATURE IMPORTANCE
# =========================

importance_df = pd.DataFrame({
    "Feature": features,
    "Importance": model.feature_importances_
}).sort_values("Importance", ascending=False)

print("\n=== Feature Importance ===\n")
print(importance_df)


# =========================
# 10. SAVE RESULTS
# =========================

output_cols = [
    "Date",
    "Close",
    "Prediction",
    "Signal",
    "Position",
    "Future_Return_5D",
    "Market_Return",
    "Strategy_Return",
    "Cumulative_Market",
    "Cumulative_Strategy",
]

output_file = "/Users/wentaoxie/Stock-Monitor/scratch/xqq_rf_strategy_results.csv"

df[output_cols].to_csv(
    output_file,
    index=False,
    encoding="utf-8-sig"
)

print(f"\nSaved strategy results to: {output_file}")


# =========================
# 11. FINAL PERFORMANCE
# =========================

final_market = df["Cumulative_Market"].iloc[-1]
final_strategy = df["Cumulative_Strategy"].iloc[-1]

print("\n=== Final Performance ===")
print(f"Buy & Hold: {final_market:.2f}x")
print(f"RF Strategy: {final_strategy:.2f}x")
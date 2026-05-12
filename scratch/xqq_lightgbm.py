# scratch/xqq_lgbm_rolling_strategy.py

import pandas as pd
import numpy as np

from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report


# =========================
# 1. LOAD DATA
# =========================

df = pd.read_csv("./xqq_full_history_macro.csv")

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
df["EWMA120"] = df["Close"].ewm(span=120,adjust=False).mean()

df["Price_vs_MA20"] = df["Close"] / df["MA20"]
df["Price_vs_MA60"] = df["Close"] / df["MA60"]
df["Price_vs_MA120"] = df["Close"] / df["MA120"]
df["Price_vs_EWMA120"] = df["Close"] / df["EWMA120"]

df["Volatility_20"] = df["Return_1D"].rolling(20).std()
df["Volatility_60"] = df["Return_1D"].rolling(60).std()
df["Volatility_Regime"] = df["Volatility_20"] / df["Volatility_60"]

df["VIX_MA20"] = df["VIX"].rolling(20).mean()
df["VIX_MA60"] = df["VIX"].rolling(60).mean()
df["VIX_MA120"] = df["VIX"].rolling(120).mean()
df["VIX_Regime"] = df["VIX_MA20"] / df["VIX_MA60"]
df["VIX_Zscore"] = df["VIX_Zscore"]

df["SPY_Trend"] = df["SPY_Trend"]
df["TNX_Change"] = df["TNX_Change"]

df["Yield_Spread"] = df["TNX"] - df["IRX"]
df["HYG_IEF_Ratio"] = df["HYG_IEF_Ratio"]
df["DXY_Trend"] = df["DXY_Trend"]

# =========================
# 3. TARGET
# =========================

df["Rolling_Mean"] = df["Future_Return_30D"].rolling(252).mean()

df["Target"] = np.nan
df["Target"] = np.nan

df.loc[
    (df["Future_Return_30D"] > 0) |
    (df["Future_Return_30D"] > df["Rolling_Mean"]),
    "Target"
] = 1

df.loc[
    (df["Future_Return_30D"] <= 0) &
    (df["Future_Return_30D"] <= df["Rolling_Mean"]),
    "Target"
] = 0


# =========================
# 4. CLEAN DATA
# =========================

features = [
    "Price_vs_MA120",
    "Volatility_20",
    # "Volatility_Regime",
    # "Volatility_60",
    "VIX_MA20",
    "VIX_MA60",
    "SPY_Trend",
    "TNX_Change",
    "Yield_Spread",
    "HYG_IEF_Ratio",
    "DXY_Trend",
]

model_df = df.dropna(
    subset=features + ["Future_Return_30D", "Rolling_Mean", "Target"]
).reset_index(drop=True)


# =========================
# 5. ROLLING TEST
# =========================

TRAIN_WINDOW = 1000   # roughly 4 years
TEST_WINDOW = 60      # roughly 3 months
PROBA_THRESHOLD = 0.5

all_results = []
all_y_true = []
all_y_pred = []
feature_importance_list = []
for start in range(0, len(model_df) - TRAIN_WINDOW - TEST_WINDOW, TEST_WINDOW):
    train_df = model_df.iloc[start:start + TRAIN_WINDOW].copy()
    test_df = model_df.iloc[start + TRAIN_WINDOW:start + TRAIN_WINDOW + TEST_WINDOW].copy()

    X_train = train_df[features]
    y_train = train_df["Target"]

    X_test = test_df[features]
    y_test = test_df["Target"]

    model = LGBMClassifier(
        class_weight={
        0: 2,
        1: 1},
        n_estimators=300,
        learning_rate=0.03,
        max_depth=3,
        num_leaves=8,
        min_child_samples=30,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=1.0,
        reg_lambda=1.0,
        random_state=42,
        verbose=-1,
    )

    model.fit(X_train, y_train)

    pred = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]

    test_df["Prediction"] = pred
    test_df["Up_Probability"] = proba

    # Binary position
    test_df["Position"] = (test_df["Up_Probability"] > PROBA_THRESHOLD).astype(int)
    test_df["Signal"] = np.where(test_df["Position"] == 1, "BUY", "SELL")

    all_results.append(test_df)
    all_y_true.extend(y_test.tolist())
    all_y_pred.extend(pred.tolist())
    feature_importance_list.append(model.feature_importances_)

# =========================
# 6. BACKTEST - more realistic open execution
# =========================

bt = pd.concat(all_results).reset_index(drop=True)

# =========================
# 6. BACKTEST - open execution, correct overnight/intraday handling
# =========================

bt = pd.concat(all_results).reset_index(drop=True)

bt["Overnight_Return"] = bt["Open"] / bt["Close"].shift(1) - 1
bt["Intraday_Return"] = bt["Close"] / bt["Open"] - 1
bt["Market_Return"] = bt["Close"].pct_change()

# Close[t-1] -> Open[t]

# 这段隔夜仓位来自更早一天已经执行过的仓位

bt["Overnight_Position"] = bt["Position"].shift(2)

# Open[t] -> Close[t]

# 今天开盘执行昨天收盘信号

bt["Intraday_Position"] = bt["Position"].shift(1)
bt["Overnight_Position"] = bt["Overnight_Position"].fillna(0)
bt["Intraday_Position"] = bt["Intraday_Position"].fillna(0)
bt["Strategy_Return"] = (
    bt["Overnight_Position"] * bt["Overnight_Return"]
    + bt["Intraday_Position"] * bt["Intraday_Return"]
)
bt["Cumulative_Market"] = (1 + bt["Market_Return"]).cumprod()
bt["Cumulative_Strategy"] = (1 + bt["Strategy_Return"]).cumprod()


# =========================
# 7. PERFORMANCE METRICS
# =========================

def max_drawdown(cumulative_returns):
    running_max = cumulative_returns.cummax()
    drawdown = cumulative_returns / running_max - 1
    return drawdown.min()


def annualized_return(cumulative_returns, periods_per_year=252):
    cumulative_returns = cumulative_returns.dropna()
    total_return = cumulative_returns.iloc[-1]
    n_periods = len(cumulative_returns)
    return total_return ** (periods_per_year / n_periods) - 1


def annualized_volatility(daily_returns, periods_per_year=252):
    return daily_returns.dropna().std() * np.sqrt(periods_per_year)


def sharpe_ratio(daily_returns, periods_per_year=252):
    daily_returns = daily_returns.dropna()
    vol = annualized_volatility(daily_returns, periods_per_year)
    if vol == 0 or np.isnan(vol):
        return np.nan
    return daily_returns.mean() * periods_per_year / vol

def feature_importance_summary(feature_importance_list, feature_names):
    avg_importance = np.mean(feature_importance_list, axis=0)
    importance_df = pd.DataFrame({
        "Feature": feature_names,
        "Average_Importance": avg_importance
    }).sort_values("Average_Importance", ascending=False)
    return importance_df

feature_importance_df = feature_importance_summary(feature_importance_list, features)

market_ann_return = annualized_return(bt["Cumulative_Market"])
strategy_ann_return = annualized_return(bt["Cumulative_Strategy"])

market_vol = annualized_volatility(bt["Market_Return"])
strategy_vol = annualized_volatility(bt["Strategy_Return"])

market_sharpe = sharpe_ratio(bt["Market_Return"])
strategy_sharpe = sharpe_ratio(bt["Strategy_Return"])

market_mdd = max_drawdown(bt["Cumulative_Market"].dropna())
strategy_mdd = max_drawdown(bt["Cumulative_Strategy"].dropna())


# =========================
# 8. OUTPUT
# =========================
print("\n=== Feature Importance Summary ===\n")
print(feature_importance_df)

print("\n=== Rolling Classification Report ===\n")
print(classification_report(all_y_true, all_y_pred))

print("\n=== Rolling Final Performance ===")
print(f"Buy & Hold Rolling Period: {bt['Cumulative_Market'].iloc[-1]:.2f}x")
print(f"LGBM Strategy Rolling Period: {bt['Cumulative_Strategy'].iloc[-1]:.2f}x")

print("\n=== Rolling Risk Metrics ===")
print(f"Buy & Hold Annual Return: {market_ann_return:.2%}")
print(f"LGBM Strategy Annual Return: {strategy_ann_return:.2%}")

print(f"Buy & Hold Volatility: {market_vol:.2%}")
print(f"LGBM Strategy Volatility: {strategy_vol:.2%}")

print(f"Buy & Hold Sharpe: {market_sharpe:.2f}")
print(f"LGBM Strategy Sharpe: {strategy_sharpe:.2f}")

print(f"Buy & Hold Max Drawdown: {market_mdd:.2%}")
print(f"LGBM Strategy Max Drawdown: {strategy_mdd:.2%}")

print("\n=== Signal Counts ===")
print(bt["Signal"].value_counts())

print("\n=== Average Position ===")
print(f"{bt['Position'].mean():.2%}")


# =========================
# 9. SAVE RESULTS
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

bt[output_cols].to_csv(
    "./xqq_lgbm_rolling_results.csv",
    index=False,
    encoding="utf-8-sig"
)

print("\nSaved rolling results to: ./xqq_lgbm_rolling_results.csv")

# =========================
# 10. LIVE PREDICTION（最后60天）
# =========================

live_df = df.dropna(subset=features).copy()
live_df = live_df.tail(60).copy().reset_index(drop=True)

live_df["Up_Probability"] = model.predict_proba(live_df[features])[:, 1]

live_df["Position"] = (live_df["Up_Probability"] > PROBA_THRESHOLD).astype(int)
live_df["Signal"] = np.where(live_df["Position"] == 1, "BUY", "SELL")

live_output_cols = [
    "Date",
    "Close",
    "Up_Probability",
    "Signal",
    "Position",
]

live_output_file = "./xqq_lgbm_live_predictions.csv"

live_df[live_output_cols].to_csv(
    live_output_file,
    index=False,
    encoding="utf-8-sig"
)

# print("\n=== Last 30 Days Live Predictions ===")
# print(live_df[live_output_cols].tail(60))

# print(f"\nSaved live predictions to: {live_output_file}")

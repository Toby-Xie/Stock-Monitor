# scratch/xqq_inflation_same_day_sentiment_model.py

import pandas as pd
import numpy as np

from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# =========================================================
# CONFIG
# =========================================================

INPUT_FILE = "./xqq_full_history_macro.csv"

OUTPUT_EVENT_FILE = "./xqq_inflation_same_day_sentiment_events.csv"
OUTPUT_PRED_FILE = "./xqq_inflation_same_day_sentiment_predictions.csv"
OUTPUT_IMPORTANCE_FILE = "./xqq_inflation_same_day_sentiment_feature_importance.csv"

# Main same-day OHLCV sentiment target
# Options:
#   "Event_Intraday_Return"       = Close / Open - 1
#   "Event_Close_Return"          = Close / Previous Close - 1
#   "Event_Gap_Return"            = Open / Previous Close - 1
#   "Event_Range_Pct"             = (High - Low) / Previous Close
#   "Event_Close_Location_Value"  = close position inside daily range, -1 to +1
#   "Event_Volume_Zscore"         = event volume abnormality
#   "Event_Sentiment_Score"       = composite same-day sentiment score
MAIN_TARGET = "Event_Range_Pct"

TRAIN_EVENTS = 80
TEST_EVENTS = 10

RANDOM_STATE = 42


# =========================================================
# HELPERS
# =========================================================

def safe_div(a, b):
    return np.where(b == 0, np.nan, a / b)


def max_abs_row(df, cols):
    """
    Return the original signed value with the largest absolute value across cols.
    All-NA rows return NaN.
    """
    existing = [c for c in cols if c in df.columns]
    if not existing:
        return pd.Series(np.nan, index=df.index)

    numeric = df[existing].apply(pd.to_numeric, errors="coerce")
    values = numeric.abs()

    result = pd.Series(np.nan, index=df.index, dtype="float64")

    valid_rows = values.notna().any(axis=1)
    if valid_rows.any():
        idx = values.loc[valid_rows].idxmax(axis=1)
        result.loc[valid_rows] = [
            numeric.loc[row_idx, col_name]
            for row_idx, col_name in idx.items()
        ]

    return result


def build_inflation_sentiment_features(df):
    """
    Build event-level data for same-day market sentiment analysis.

    Difference vs previous shock model:
    - Still uses CPI/PPI release days as event rows.
    - Still uses shock + macro state + market state as features.
    - Regression target is NOT future return.
    - Regression target is same-day OHLCV reaction.
    """

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)

    # -----------------------------------------------------
    # Basic market returns / positioning
    # -----------------------------------------------------
    df["Prev_Close"] = df["Close"].shift(1)

    df["Return_1D"] = df["Close"].pct_change(1)
    df["Return_3D"] = df["Close"].pct_change(3)
    df["Return_5D"] = df["Close"].pct_change(5)
    df["Return_10D"] = df["Close"].pct_change(10)
    df["Return_20D"] = df["Close"].pct_change(20)

    df["XQQ_Vol_10D"] = df["Return_1D"].rolling(10).std()
    df["XQQ_Vol_20D"] = df["Return_1D"].rolling(20).std()
    df["XQQ_Vol_60D"] = df["Return_1D"].rolling(60).std()
    df["XQQ_Vol_Regime"] = safe_div(df["XQQ_Vol_20D"], df["XQQ_Vol_60D"])

    if "MA20" not in df.columns:
        df["MA20"] = df["Close"].rolling(20).mean()
    if "MA60" not in df.columns:
        df["MA60"] = df["Close"].rolling(60).mean()
    if "MA120" not in df.columns:
        df["MA120"] = df["Close"].rolling(120).mean()

    df["Price_vs_MA20"] = df["Close"] / df["MA20"]
    df["Price_vs_MA60"] = df["Close"] / df["MA60"]
    df["Price_vs_MA120"] = df["Close"] / df["MA120"]

    # -----------------------------------------------------
    # Macro / risk state before event
    # -----------------------------------------------------
    if "VIX" in df.columns:
        df["VIX_MA20"] = df["VIX"].rolling(20).mean()
        df["VIX_MA60"] = df["VIX"].rolling(60).mean()
        df["VIX_Regime"] = df["VIX_MA20"] / df["VIX_MA60"]
        if "VIX_Zscore" not in df.columns:
            df["VIX_Zscore"] = (
                df["VIX"] - df["VIX"].rolling(60).mean()
            ) / df["VIX"].rolling(60).std()

    if "SPY" in df.columns:
        df["SPY_Return_5D"] = df["SPY"].pct_change(5)
        df["SPY_Return_20D"] = df["SPY"].pct_change(20)
        if "SPY_Trend" not in df.columns:
            df["SPY_MA200"] = df["SPY"].rolling(200).mean()
            df["SPY_Trend"] = df["SPY"] / df["SPY_MA200"]

    if "TNX" in df.columns:
        df["TNX_Change_5D"] = df["TNX"].diff(5)
        df["TNX_Change_20D"] = df["TNX"].diff(20)
        if "TNX_Change" not in df.columns:
            df["TNX_Change"] = df["TNX"].pct_change(20)

    if "IRX" in df.columns and "TNX" in df.columns:
        df["Yield_Spread"] = df["TNX"] - df["IRX"]
        df["Yield_Spread_Change_20D"] = df["Yield_Spread"].diff(20)

    if "HYG" in df.columns and "IEF" in df.columns:
        df["HYG_IEF"] = df["HYG"] / df["IEF"]
        df["HYG_IEF_MA50"] = df["HYG_IEF"].rolling(50).mean()
        df["HYG_IEF_Ratio"] = df["HYG_IEF"] / df["HYG_IEF_MA50"]
        df["HYG_IEF_Change_20D"] = df["HYG_IEF"].pct_change(20)

    if "DXY" in df.columns:
        df["DXY_MA50"] = df["DXY"].rolling(50).mean()
        df["DXY_Trend"] = df["DXY"] / df["DXY_MA50"]
        df["DXY_Return_5D"] = df["DXY"].pct_change(5)
        df["DXY_Return_20D"] = df["DXY"].pct_change(20)

    # -----------------------------------------------------
    # Inflation shock columns
    # -----------------------------------------------------
    surprise_cols = [
        c for c in df.columns
        if c.endswith("_Surprise")
        or c in [
            "CPI_Surprise_Mean",
            "PPI_Surprise_Mean",
            "Inflation_Surprise_Mean",
            "Inflation_Surprise_Sum",
            "Inflation_Surprise_MaxAbs",
        ]
    ]
    surprise_cols = list(dict.fromkeys(surprise_cols))

    for col in surprise_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Inflation_Release_Flag" not in df.columns:
        possible = [
            c for c in ["CPI_Surprise_Mean", "PPI_Surprise_Mean", "Inflation_Surprise_Mean"]
            if c in df.columns
        ]
        if possible:
            df["Inflation_Release_Flag"] = df[possible].notna().any(axis=1).astype(int)
        else:
            df["Inflation_Release_Flag"] = 0

    df["Inflation_Release_Flag"] = df["Inflation_Release_Flag"].fillna(0).astype(int)

    event_shock_cols = [
        "CPI_Surprise_Mean",
        "PPI_Surprise_Mean",
        "Inflation_Surprise_Mean",
        "Inflation_Surprise_Sum",
        "Inflation_Surprise_MaxAbs",
    ]

    for col in event_shock_cols:
        if col not in df.columns:
            df[col] = np.nan
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col + "_Event"] = df[col].fillna(0)

    df["Inflation_Strongest_Event_Surprise"] = max_abs_row(
        df,
        [c for c in surprise_cols if c.endswith("_Surprise")]
    ).fillna(0)

    df["CPI_Shock_Positive"] = (df["CPI_Surprise_Mean_Event"] > 0).astype(int)
    df["CPI_Shock_Negative"] = (df["CPI_Surprise_Mean_Event"] < 0).astype(int)
    df["PPI_Shock_Positive"] = (df["PPI_Surprise_Mean_Event"] > 0).astype(int)
    df["PPI_Shock_Negative"] = (df["PPI_Surprise_Mean_Event"] < 0).astype(int)
    df["Inflation_Shock_Positive"] = (df["Inflation_Surprise_Mean_Event"] > 0).astype(int)
    df["Inflation_Shock_Negative"] = (df["Inflation_Surprise_Mean_Event"] < 0).astype(int)

    for col in ["CPI_Surprise_Mean", "PPI_Surprise_Mean", "Inflation_Surprise_Mean"]:
        df[col + "_Last"] = df[col].ffill()

    last_release_date = df["Date"].where(df["Inflation_Release_Flag"] == 1).ffill()
    df["Days_Since_Inflation"] = (df["Date"] - last_release_date).dt.days

    df["CPI_Surprise_60D_Sum"] = df["CPI_Surprise_Mean_Event"].rolling(60).sum()
    df["PPI_Surprise_60D_Sum"] = df["PPI_Surprise_Mean_Event"].rolling(60).sum()
    df["Inflation_Surprise_60D_Sum"] = df["Inflation_Surprise_Mean_Event"].rolling(60).sum()

    df["CPI_Surprise_120D_Sum"] = df["CPI_Surprise_Mean_Event"].rolling(120).sum()
    df["PPI_Surprise_120D_Sum"] = df["PPI_Surprise_Mean_Event"].rolling(120).sum()
    df["Inflation_Surprise_120D_Sum"] = df["Inflation_Surprise_Mean_Event"].rolling(120).sum()

    # -----------------------------------------------------
    # Interactions
    # -----------------------------------------------------
    df["CPI_Shock_x_VIX"] = df["CPI_Surprise_Mean_Event"] * df.get("VIX_Zscore", 0)
    df["PPI_Shock_x_VIX"] = df["PPI_Surprise_Mean_Event"] * df.get("VIX_Zscore", 0)
    df["Inflation_Shock_x_VIX"] = df["Inflation_Surprise_Mean_Event"] * df.get("VIX_Zscore", 0)

    df["CPI_Shock_x_TNX_Change"] = df["CPI_Surprise_Mean_Event"] * df.get("TNX_Change", 0)
    df["PPI_Shock_x_TNX_Change"] = df["PPI_Surprise_Mean_Event"] * df.get("TNX_Change", 0)

    df["CPI_Shock_x_SPY_Trend"] = df["CPI_Surprise_Mean_Event"] * df.get("SPY_Trend", 0)
    df["PPI_Shock_x_SPY_Trend"] = df["PPI_Surprise_Mean_Event"] * df.get("SPY_Trend", 0)

    # -----------------------------------------------------
    # Same-day OHLCV sentiment targets
    # -----------------------------------------------------
    df["Event_Gap_Return"] = df["Open"] / df["Prev_Close"] - 1
    df["Event_Intraday_Return"] = df["Close"] / df["Open"] - 1
    df["Event_Close_Return"] = df["Close"] / df["Prev_Close"] - 1
    df["Event_Range_Pct"] = (df["High"] - df["Low"]) / df["Prev_Close"]

    df["Event_Close_Location_Value"] = (
        2 * ((df["Close"] - df["Low"]) / (df["High"] - df["Low"])) - 1
    )

    df["Volume_MA20"] = df["Volume"].rolling(20).mean()
    df["Volume_STD20"] = df["Volume"].rolling(20).std()
    df["Event_Volume_Ratio"] = df["Volume"] / df["Volume_MA20"]
    df["Event_Volume_Zscore"] = (df["Volume"] - df["Volume_MA20"]) / df["Volume_STD20"]

    df["Event_Sentiment_Score"] = (
        df["Event_Intraday_Return"].rank(pct=True)
        + df["Event_Close_Return"].rank(pct=True)
        + df["Event_Close_Location_Value"].rank(pct=True)
        + df["Event_Volume_Zscore"].rank(pct=True)
    ) / 4

    event_df = df[df["Inflation_Release_Flag"] == 1].copy()

    return df, event_df


def get_feature_columns(df):
    candidates = [
    # Shock（核心）
    "Inflation_Strongest_Event_Surprise",
    "Inflation_Surprise_Mean_Event",

    # Inflation regime
    "Inflation_Surprise_60D_Sum",

    # Market state（最关键）
    "XQQ_Vol_Regime",
    "Price_vs_MA120",
    "Return_20D",

    # Macro state
    "VIX_Zscore",
    "DXY_Trend",
    "Yield_Spread",
    "HYG_IEF_Ratio",
]

    return [c for c in candidates if c in df.columns]


def rolling_event_regression(event_df, features, target_col):
    model_df = event_df.dropna(
        subset=features + [target_col]
    ).reset_index(drop=True)

    if len(model_df) < TRAIN_EVENTS + TEST_EVENTS:
        raise ValueError(
            f"Not enough inflation event rows. "
            f"Have {len(model_df)}, need at least {TRAIN_EVENTS + TEST_EVENTS}."
        )

    all_results = []
    feature_importance_list = []

    for start in range(0, len(model_df) - TRAIN_EVENTS - TEST_EVENTS + 1, TEST_EVENTS):
        train_df = model_df.iloc[start:start + TRAIN_EVENTS].copy()
        test_df = model_df.iloc[start + TRAIN_EVENTS:start + TRAIN_EVENTS + TEST_EVENTS].copy()

        reg = LGBMRegressor(
            n_estimators=300,
            learning_rate=0.03,
            max_depth=3,
            num_leaves=8,
            min_child_samples=3,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.0,
            reg_lambda=0.0,
            random_state=RANDOM_STATE,
            verbose=-1,
        )

        reg.fit(train_df[features], train_df[target_col])

        test_df["Predicted_" + target_col] = reg.predict(test_df[features])
        test_df["Prediction_Error"] = test_df["Predicted_" + target_col] - test_df[target_col]

        all_results.append(test_df)
        feature_importance_list.append(reg.feature_importances_)

    results = pd.concat(all_results).reset_index(drop=True)

    importance = pd.DataFrame({
        "Feature": features,
        "Average_Importance": np.mean(feature_importance_list, axis=0)
    }).sort_values("Average_Importance", ascending=False)

    return results, importance


def print_sentiment_diagnostics(event_df):
    print("\n=== Same-Day Inflation Event Sentiment Diagnostics ===")
    print(f"Event rows: {len(event_df)}")
    print(f"Date range: {event_df['Date'].min().date()} -> {event_df['Date'].max().date()}")

    target_cols = [
        "Event_Gap_Return",
        "Event_Intraday_Return",
        "Event_Close_Return",
        "Event_Range_Pct",
        "Event_Close_Location_Value",
        "Event_Volume_Ratio",
        "Event_Volume_Zscore",
        "Event_Sentiment_Score",
    ]

    existing = [c for c in target_cols if c in event_df.columns]

    print("\nSame-day OHLCV sentiment target summary:")
    print(event_df[existing].describe())

    print("\nAverage same-day reaction:")
    for col in existing:
        if "Return" in col or "Range" in col:
            print(f"{col}: {event_df[col].mean():.2%}")
        else:
            print(f"{col}: {event_df[col].mean():.4f}")


def evaluate_regression(results, target_col):
    pred_col = "Predicted_" + target_col

    y_true = results[target_col]
    y_pred = results[pred_col]

    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    corr = pd.Series(y_true).corr(pd.Series(y_pred))

    print("\n=== Rolling Regression Report ===")
    print(f"Target: {target_col}")
    print(f"MAE: {mae:.4%}")
    print(f"RMSE: {rmse:.4%}")
    print(f"R2: {r2:.4f}")
    print(f"Prediction / Actual Correlation: {corr:.4f}")

    if "Return" in target_col or "Location" in target_col or "Sentiment" in target_col:
        directional_accuracy = (np.sign(y_true) == np.sign(y_pred)).mean()
        print(f"Directional Accuracy: {directional_accuracy:.2%}")

    print("\nPrediction quantile check:")
    tmp = results[[target_col, pred_col]].dropna().copy()
    tmp["Pred_Quantile"] = pd.qcut(tmp[pred_col], q=5, duplicates="drop")
    print(tmp.groupby("Pred_Quantile", observed=True)[target_col].agg(["count", "mean", "median"]))


# =========================================================
# MAIN
# =========================================================

def main():
    print(f"Loading {INPUT_FILE}...")

    df = pd.read_csv(INPUT_FILE)
    df, event_df = build_inflation_sentiment_features(df)

    features = get_feature_columns(df)

    print_sentiment_diagnostics(event_df)

    print("\n=== Regression Target ===")
    print(MAIN_TARGET)

    print("\n=== Features Used ===")
    for f in features:
        print(f"- {f}")

    event_df.to_csv(OUTPUT_EVENT_FILE, index=False, encoding="utf-8-sig")
    print(f"\nSaved event dataset to: {OUTPUT_EVENT_FILE}")

    results, importance = rolling_event_regression(
        event_df=event_df,
        features=features,
        target_col=MAIN_TARGET,
    )

    evaluate_regression(results, MAIN_TARGET)

    print("\n=== Feature Importance ===")
    print(importance)

    results.to_csv(OUTPUT_PRED_FILE, index=False, encoding="utf-8-sig")
    importance.to_csv(OUTPUT_IMPORTANCE_FILE, index=False, encoding="utf-8-sig")

    print(f"\nSaved rolling predictions to: {OUTPUT_PRED_FILE}")
    print(f"Saved feature importance to: {OUTPUT_IMPORTANCE_FILE}")

    clean_event_df = event_df.dropna(subset=features).copy()
    if len(clean_event_df) >= TRAIN_EVENTS:
        latest_train = clean_event_df.dropna(subset=[MAIN_TARGET]).iloc[-TRAIN_EVENTS:].copy()

        latest_reg = LGBMRegressor(
            n_estimators=300,
            learning_rate=0.03,
            max_depth=3,
            num_leaves=8,
            min_child_samples=10,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=1.0,
            reg_lambda=1.0,
            random_state=RANDOM_STATE,
            verbose=-1,
        )

        latest_reg.fit(latest_train[features], latest_train[MAIN_TARGET])

        latest_row = clean_event_df.tail(1).copy()
        latest_row["Latest_Predicted_" + MAIN_TARGET] = latest_reg.predict(latest_row[features])

        print("\n=== Latest Inflation Same-Day Sentiment Prediction ===")
        cols = [
            "Date",
            "Close",
            "CPI_Surprise_Mean_Event",
            "PPI_Surprise_Mean_Event",
            "Inflation_Surprise_Mean_Event",
            MAIN_TARGET,
            "Latest_Predicted_" + MAIN_TARGET,
        ]
        print(latest_row[[c for c in cols if c in latest_row.columns]])


if __name__ == "__main__":
    main()

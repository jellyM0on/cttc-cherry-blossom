import numpy as np
import pandas as pd

def clean_text_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df

def valid_nonempty_mask(series: pd.Series) -> pd.Series:
    return (
        series.notna()
        & (series.astype(str).str.strip() != "")
        & (series.astype(str).str.strip().str.lower() != "nan")
        & (series.astype(str).str.strip().str.lower() != "none")
    )

def compute_linear_trend(df: pd.DataFrame, x_col: str, y_col: str) -> tuple[float, float]:
    valid = df[[x_col, y_col]].dropna()
    if len(valid) < 2:
        return np.nan, np.nan
    slope, intercept = np.polyfit(valid[x_col], valid[y_col], 1)
    return slope, intercept
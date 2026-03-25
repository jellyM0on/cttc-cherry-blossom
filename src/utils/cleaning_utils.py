import pandas as pd

def validate_columns(df: pd.DataFrame, required_columns: set[str], dataset_name: str) -> None:
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {dataset_name}: {sorted(missing)}")

def standardize_id(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().replace(
        {
            "": pd.NA,
            "nan": pd.NA,
            "None": pd.NA,
        }
    )

def standardize_text_field(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()

def standardize_numeric_field(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def standardize_date_field(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")
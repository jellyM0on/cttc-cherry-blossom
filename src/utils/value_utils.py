import pandas as pd

def safe_str(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None

def is_empty_value(value) -> bool:
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "none", "null"}

def first_non_empty(*values):
    for value in values:
        if not is_empty_value(value):
            return value
    return None
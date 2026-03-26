import pandas as pd

from src.config import (
    BLOSSOM_FILE,
    WMO_FILE,
    CHERRY_BLOSSOM_CLEANED_FILE,
    PROCESSED_DATA_DIR,
    BLOSSOM_REQUIRED_COLUMNS,
    WMO_REQUIRED_COLUMNS,
)
from src.utils.cleaning_utils import (
    validate_columns,
    standardize_id,
    standardize_text_field,
    standardize_numeric_field,
    standardize_date_field,
)

def prepare_wmo_station_data(wmo_df: pd.DataFrame) -> pd.DataFrame:
    validate_columns(wmo_df, WMO_REQUIRED_COLUMNS, "WMO station dataset")

    wmo_df = wmo_df[list(WMO_REQUIRED_COLUMNS)].copy()

    wmo_df["wmo_index"] = standardize_id(wmo_df["wmo_index"])
    wmo_df["station_name"] = standardize_text_field(wmo_df["station_name"])
    wmo_df["country_area"] = standardize_text_field(wmo_df["country_area"])
    wmo_df["station_name_ja"] = standardize_text_field(wmo_df["station_name_ja"])

    wmo_df["latitude_deg"] = standardize_numeric_field(wmo_df["latitude_deg"])
    wmo_df["longitude_deg"] = standardize_numeric_field(wmo_df["longitude_deg"])

    wmo_df = wmo_df[wmo_df["country_area"].str.upper() == "JAPAN"].copy()
    wmo_df = wmo_df.drop_duplicates(subset=["wmo_index"], keep="first").copy()

    wmo_df = wmo_df.rename(
        columns={
            "wmo_index": "wmo_station_id",
            "station_name": "station_name_en",
        }
    )

    return wmo_df

def prepare_blossom_data(blossom_df: pd.DataFrame) -> pd.DataFrame:
    validate_columns(blossom_df, BLOSSOM_REQUIRED_COLUMNS, "blossom dataset")

    blossom_df = blossom_df[list(BLOSSOM_REQUIRED_COLUMNS)].copy()

    blossom_df["year"] = standardize_numeric_field(blossom_df["year"])
    blossom_df["jma_station_code"] = standardize_id(blossom_df["jma_station_code"])
    blossom_df["wmo_station_id"] = standardize_id(blossom_df["wmo_station_id"])
    blossom_df["station_name_jp"] = standardize_text_field(blossom_df["station_name_jp"])
    blossom_df["event"] = standardize_text_field(blossom_df["event"]).str.lower()
    blossom_df["date"] = standardize_date_field(blossom_df["date"])

    blossom_df = blossom_df.dropna(
        subset=["year", "date", "jma_station_code", "station_name_jp", "event"]
    ).copy()

    blossom_df["year"] = blossom_df["year"].astype(int)

    blossom_df = blossom_df[blossom_df["event"].isin(["flowering", "full_bloom"])].copy()
    blossom_df = blossom_df[blossom_df["date"].dt.year == blossom_df["year"]].copy()

    blossom_df = blossom_df.drop_duplicates(
        subset=[
            "year",
            "station_name_jp",
            "jma_station_code",
            "wmo_station_id",
            "event",
            "date",
        ]
    ).copy()

    blossom_df["day_of_year"] = blossom_df["date"].dt.dayofyear
    blossom_df["month"] = blossom_df["date"].dt.month
    blossom_df["day"] = blossom_df["date"].dt.day

    return blossom_df

def merge_blossom_with_wmo(blossom_df: pd.DataFrame, wmo_df: pd.DataFrame) -> pd.DataFrame:
    merged_df = blossom_df.merge(
        wmo_df,
        on="wmo_station_id",
        how="left",
        validate="many_to_one",
    )

    ordered_columns = [
        "year",
        "date",
        "day_of_year",
        "month",
        "day",
        "event",
        "station_name_jp",
        "station_name_en",
        "jma_station_code",
        "wmo_station_id",
        "country_area",
        "latitude_deg",
        "longitude_deg",
    ]

    for column in ordered_columns:
        if column not in merged_df.columns:
            merged_df[column] = pd.NA

    return (
        merged_df[ordered_columns]
        .sort_values(["station_name_jp", "event", "year"])
        .reset_index(drop=True)
    )

def clean_initial_blossom_data() -> None:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Reading blossom data from: {BLOSSOM_FILE}")
    blossom_df = pd.read_csv(BLOSSOM_FILE)

    print(f"Reading WMO station data from: {WMO_FILE}")
    wmo_df = pd.read_csv(WMO_FILE)

    blossom_clean = prepare_blossom_data(blossom_df)
    wmo_clean = prepare_wmo_station_data(wmo_df)
    merged_df = merge_blossom_with_wmo(blossom_clean, wmo_clean)

    merged_df.to_csv(CHERRY_BLOSSOM_CLEANED_FILE, index=False)

    print("\nInitial cleaning complete.")
    print(f"Saved cleaned dataset to: {CHERRY_BLOSSOM_CLEANED_FILE}")
    print(f"Rows: {len(merged_df):,}")
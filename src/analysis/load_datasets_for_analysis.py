import pandas as pd

from src.config import (
    CHERRY_BLOSSOM_FULL_BLOOM_FILE,
    FULL_BLOOM_NATIONAL_YEARLY_FILE,
    FULL_BLOOM_BY_CLIMATE_YEARLY_FILE ,
    FULL_BLOOM_BY_PROVINCE_YEARLY_FILE,
    FULL_BLOOM_BY_STATION_YEARLY_FILE ,
    FULL_BLOOM_STATION_COVERAGE_FILE,
    SELECTED_STATIONS_FULL_BLOOM_FILE,
    SELECTED_STATIONS_SUMMARY_FILE,
)
from src.utils.analysis_utils import clean_text_columns

def load_full_bloom() -> pd.DataFrame:
    df = pd.read_csv(CHERRY_BLOSSOM_FULL_BLOOM_FILE)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["day_of_year"] = pd.to_numeric(df["day_of_year"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = clean_text_columns(
        df,
        [
            "station_name_jp",
            "station_name_en",
            "jma_station_code",
            "wmo_station_id",
            "city",
            "subprovince",
            "province",
            "climate_classification_koppen",
        ],
    )
    df = df.dropna(subset=["year", "day_of_year"]).copy()
    df["year"] = df["year"].astype(int)
    df["day_of_year"] = df["day_of_year"].astype(int)
    return df

def load_national_yearly() -> pd.DataFrame:
    df = pd.read_csv(FULL_BLOOM_NATIONAL_YEARLY_FILE)
    numeric_cols = [
        "year", "mean_day_of_year", "median_day_of_year", "std_day_of_year",
        "min_day_of_year", "max_day_of_year", "station_count", "record_count"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["year"]).sort_values("year").copy()
    df["year"] = df["year"].astype(int)
    return df

def load_climate_yearly() -> pd.DataFrame:
    df = pd.read_csv(FULL_BLOOM_BY_CLIMATE_YEARLY_FILE)
    df = clean_text_columns(df, ["climate_classification_koppen"])
    numeric_cols = [
        "year", "mean_day_of_year", "median_day_of_year", "std_day_of_year",
        "min_day_of_year", "max_day_of_year", "station_count", "record_count"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)
    return df

def load_province_yearly() -> pd.DataFrame:
    df = pd.read_csv(FULL_BLOOM_BY_PROVINCE_YEARLY_FILE)
    df = clean_text_columns(df, ["province"])
    numeric_cols = [
        "year", "mean_day_of_year", "median_day_of_year", "std_day_of_year",
        "min_day_of_year", "max_day_of_year", "station_count", "record_count"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["year"]).copy()
    df["year"] = df["year"].astype(int)
    return df

def load_station_yearly() -> pd.DataFrame:
    df = pd.read_csv(FULL_BLOOM_BY_STATION_YEARLY_FILE)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["day_of_year"] = pd.to_numeric(df["day_of_year"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = clean_text_columns(
        df,
        [
            "jma_station_code",
            "wmo_station_id",
            "station_name_jp",
            "station_name_en",
            "province",
            "climate_classification_koppen",
            "city",
            "subprovince",
        ],
    )
    df = df.dropna(subset=["year", "day_of_year"]).copy()
    df["year"] = df["year"].astype(int)
    return df

def load_station_coverage() -> pd.DataFrame:
    df = pd.read_csv(FULL_BLOOM_STATION_COVERAGE_FILE)
    df = clean_text_columns(
        df,
        [
            "jma_station_code",
            "wmo_station_id",
            "station_name_jp",
            "station_name_en",
            "province",
            "climate_classification_koppen",
            "city",
            "subprovince",
        ],
    )
    numeric_cols = [
        "first_year", "last_year", "total_years", "total_records",
        "mean_day_of_year", "median_day_of_year", "std_day_of_year",
        "earliest_day_of_year", "latest_day_of_year",
        "latitude_deg", "longitude_deg",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def load_selected_full_bloom() -> pd.DataFrame:
    df = pd.read_csv(SELECTED_STATIONS_FULL_BLOOM_FILE)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["day_of_year"] = pd.to_numeric(df["day_of_year"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = clean_text_columns(
        df,
        [
            "jma_station_code",
            "wmo_station_id",
            "station_name_jp",
            "station_name_en",
            "province",
            "climate_classification_koppen",
            "city",
            "subprovince",
        ],
    )
    df = df.dropna(subset=["year", "day_of_year"]).copy()
    df["year"] = df["year"].astype(int)
    return df

def load_selected_summary() -> pd.DataFrame:
    df = pd.read_csv(SELECTED_STATIONS_SUMMARY_FILE)
    df = clean_text_columns(
        df,
        [
            "jma_station_code",
            "station_name_jp",
            "station_name_en",
            "wmo_station_id",
            "province",
            "climate_classification_koppen",
            "city",
            "subprovince",
        ],
    )
    numeric_cols = [
        "total_years", "total_records", "first_year", "last_year",
        "mean_day_of_year", "median_day_of_year", "std_day_of_year",
        "earliest_day_of_year", "latest_day_of_year",
        "latitude_deg", "longitude_deg",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def load_analysis_inputs() -> dict[str, pd.DataFrame]:
    return {
        "full_bloom": load_full_bloom(),
        "national_yearly": load_national_yearly(),
        "climate_yearly": load_climate_yearly(),
        "province_yearly": load_province_yearly(),
        "station_yearly": load_station_yearly(),
        "station_coverage": load_station_coverage(),
        "selected_full_bloom": load_selected_full_bloom(),
        "selected_summary": load_selected_summary(),
    }
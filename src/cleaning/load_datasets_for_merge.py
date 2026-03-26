import pandas as pd

from src.config import (
    CHERRY_BLOSSOM_CLEANED_FILE,
    JMA_STATION_GEOLOCATION_DETAILS_FILE,
    CLEANED_REQUIRED_COLUMNS,
    GEO_REQUIRED_COLUMNS,
)
from src.utils.cleaning_utils import (
    validate_columns,
    standardize_id,
    standardize_text_field,
    standardize_numeric_field,
    standardize_date_field,
)

def load_cleaned_dataset() -> pd.DataFrame:
    df = pd.read_csv(CHERRY_BLOSSOM_CLEANED_FILE)
    validate_columns(df, CLEANED_REQUIRED_COLUMNS, "cherry_blossom_cleaned.csv")

    df = df.copy()
    df["jma_station_code"] = standardize_id(df["jma_station_code"])
    df["wmo_station_id"] = standardize_id(df["wmo_station_id"])

    for col in ["station_name_jp", "station_name_en", "event", "country_area"]:
        df[col] = standardize_text_field(df[col])

    df["year"] = standardize_numeric_field(df["year"])
    df["day_of_year"] = standardize_numeric_field(df["day_of_year"])
    df["month"] = standardize_numeric_field(df["month"])
    df["day"] = standardize_numeric_field(df["day"])
    df["latitude_deg"] = standardize_numeric_field(df["latitude_deg"])
    df["longitude_deg"] = standardize_numeric_field(df["longitude_deg"])
    df["date"] = standardize_date_field(df["date"])

    df = df.dropna(subset=["year", "date", "day_of_year"]).copy()
    df["year"] = df["year"].astype(int)
    df["day_of_year"] = df["day_of_year"].astype(int)
    df["month"] = df["month"].astype("Int64")
    df["day"] = df["day"].astype("Int64")

    return df

def load_geolocation_dataset() -> pd.DataFrame:
    geo = pd.read_csv(JMA_STATION_GEOLOCATION_DETAILS_FILE)
    validate_columns(geo, GEO_REQUIRED_COLUMNS, "jma_station_geolocation_details.csv")

    geo = geo.copy()
    geo["jma_station_code"] = standardize_id(geo["jma_station_code"])

    text_cols = [
        "station_name_jp",
        "station_name_en",
        "display_name",
        "osm_type",
        "country",
        "country_code",
        "neighbourhood",
        "quarter",
        "city",
        "subprovince",
        "province",
        "postcode",
        "climate_classification_koppen",
    ]
    for col in text_cols:
        geo[col] = standardize_text_field(geo[col])

    geo["latitude_deg"] = standardize_numeric_field(geo["latitude_deg"])
    geo["longitude_deg"] = standardize_numeric_field(geo["longitude_deg"])
    geo["osm_id"] = pd.to_numeric(geo["osm_id"], errors="coerce")

    geo = geo.drop_duplicates(subset=["jma_station_code"], keep="first").copy()
    return geo
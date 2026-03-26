import pandas as pd

from src.utils.value_utils import is_empty_value

EXPECTED_OUTPUT_COLUMNS = [
    "jma_station_code",
    "wmo_station_id",
    "station_name_jp",
    "station_name_en",
    "latitude_deg",
    "longitude_deg",
    "display_name",
    "osm_type",
    "osm_id",
    "country",
    "country_code",
    "neighbourhood",
    "quarter",
    "city",
    "subprovince",
    "province",
    "postcode",
    "climate_classification_koppen",
    "climate_lookup_note",
    "geocode_error",
]

def ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in EXPECTED_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[EXPECTED_OUTPUT_COLUMNS]

def merge_station_fields_into_output(
    output_df: pd.DataFrame,
    station_lookup: pd.DataFrame,
) -> pd.DataFrame:
    merged = output_df.merge(
        station_lookup,
        on="jma_station_code",
        how="left",
        suffixes=("", "_input"),
    )

    fields_to_fill = [
        "wmo_station_id",
        "station_name_jp",
        "station_name_en",
        "latitude_deg",
        "longitude_deg",
    ]

    for field in fields_to_fill:
        input_field = f"{field}_input"
        if input_field not in merged.columns:
            continue
        merged[field] = merged[field].where(
            ~merged[field].apply(is_empty_value),
            merged[input_field],
        )

    drop_cols = [c for c in merged.columns if c.endswith("_input")]
    merged = merged.drop(columns=drop_cols)

    return ensure_output_columns(merged)
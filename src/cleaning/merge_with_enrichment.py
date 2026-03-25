import pandas as pd

def merge_cleaned_with_geolocation(cleaned_df: pd.DataFrame, geo_df: pd.DataFrame) -> pd.DataFrame:
    geo_cols = [
        "jma_station_code",
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
    ]

    merged = cleaned_df.merge(
        geo_df[geo_cols],
        on="jma_station_code",
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
        "station_name_ja",
        "jma_station_code",
        "wmo_station_id",
        "country_area",
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
    ]

    return (
        merged[ordered_columns]
        .sort_values(["station_name_en", "event", "year"])
        .reset_index(drop=True)
    )

def split_event_datasets(enriched_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    full_bloom_df = enriched_df[enriched_df["event"] == "full_bloom"].copy()
    flowering_df = enriched_df[enriched_df["event"] == "flowering"].copy()
    return full_bloom_df, flowering_df
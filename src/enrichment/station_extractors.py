import pandas as pd

REQUIRED_STATION_COLUMNS = [
    "jma_station_code",
    "wmo_station_id",
    "station_name_jp",
    "station_name_en",
    "station_name_ja",
    "latitude_deg",
    "longitude_deg",
]

def _validate_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_STATION_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def _prepare_base_station_df(df: pd.DataFrame) -> pd.DataFrame:
    _validate_required_columns(df)

    stations = df[REQUIRED_STATION_COLUMNS].copy()
    stations["jma_station_code"] = stations["jma_station_code"].astype(str).str.strip()
    stations["wmo_station_id"] = stations["wmo_station_id"].astype(str).str.strip()
    stations["latitude_deg"] = pd.to_numeric(stations["latitude_deg"], errors="coerce")
    stations["longitude_deg"] = pd.to_numeric(stations["longitude_deg"], errors="coerce")

    stations = stations[stations["jma_station_code"] != ""]
    stations = (
        stations.sort_values(
            ["jma_station_code", "station_name_en", "station_name_jp", "wmo_station_id"]
        )
        .drop_duplicates(subset=["jma_station_code"], keep="first")
        .reset_index(drop=True)
    )
    return stations

def extract_unique_stations(df: pd.DataFrame) -> pd.DataFrame:
    stations = _prepare_base_station_df(df)
    stations = stations.dropna(subset=["latitude_deg", "longitude_deg"]).reset_index(drop=True)
    return stations

def build_station_lookup(df: pd.DataFrame) -> pd.DataFrame:
    return _prepare_base_station_df(df)
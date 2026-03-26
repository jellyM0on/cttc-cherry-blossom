import sys
import time

import pandas as pd
import requests

from src.config import (
    HEADERS,
    NOMINATIM_REVERSE_URL,
    NOMINATIM_SEARCH_URL,
    GEOCODE_WAIT_SECONDS,
)
from src.utils.value_utils import safe_str, is_empty_value

def reverse_geocode_nominatim(lat: float, lon: float, session: requests.Session) -> dict:
    params = {
        "lat": lat,
        "lon": lon,
        "format": "jsonv2",
        "addressdetails": 1,
        "namedetails": 1,
        "extratags": 1,
        "zoom": 18,
    }

    response = session.get(
        NOMINATIM_REVERSE_URL,
        params=params,
        headers=HEADERS,
        timeout=60,
    )

    if response.status_code == 403:
        print("\nERROR: Received 403 Forbidden from Nominatim")
        print(f"Failed at coordinates: lat={lat}, lon={lon}")
        sys.exit(1)

    response.raise_for_status()
    return response.json()

def search_geocode_nominatim(query: str, session: requests.Session) -> dict | None:
    params = {
        "q": query,
        "format": "jsonv2",
        "limit": 1,
        "addressdetails": 1,
        "namedetails": 1,
        "countrycodes": "jp",
    }

    response = session.get(
        NOMINATIM_SEARCH_URL,
        params=params,
        headers=HEADERS,
        timeout=60,
    )

    if response.status_code == 403:
        print("\nERROR: Received 403 Forbidden from Nominatim search")
        print(f"Failed search query: {query}")
        sys.exit(1)

    response.raise_for_status()
    results = response.json()
    return results[0] if results else None

def build_location_queries(row: pd.Series) -> list[str]:
    station_name_jp = safe_str(row.get("station_name_jp"))
    station_name_en = safe_str(row.get("station_name_en"))
    city = safe_str(row.get("city"))
    province = safe_str(row.get("province"))
    subprovince = safe_str(row.get("subprovince"))
    country = safe_str(row.get("country")) or "Japan"
    display_name = safe_str(row.get("display_name"))

    queries: list[str] = []
    parts_sets = [
        [city, province, country],
        [city, subprovince, province, country],
        [station_name_en, city, province, country],
        [station_name_jp, city, province, country],
        [station_name_en, province, country],
        [station_name_jp, province, country],
        [display_name],
        [station_name_en, country],
        [station_name_jp, country],
    ]

    seen = set()
    for parts in parts_sets:
        query = ", ".join([p for p in parts if p and not is_empty_value(p)]).strip(", ").strip()
        if query and query not in seen:
            seen.add(query)
            queries.append(query)

    return queries

def enrich_station_row(row: pd.Series, session: requests.Session) -> dict:
    lat = float(row["latitude_deg"])
    lon = float(row["longitude_deg"])

    result = {
        "jma_station_code": safe_str(row["jma_station_code"]),
        "wmo_station_id": safe_str(row.get("wmo_station_id")),
        "station_name_jp": safe_str(row.get("station_name_jp")),
        "station_name_en": safe_str(row.get("station_name_en")),
        "latitude_deg": lat,
        "longitude_deg": lon,
        "display_name": None,
        "osm_type": None,
        "osm_id": None,
        "country": None,
        "country_code": None,
        "neighbourhood": None,
        "quarter": None,
        "city": None,
        "subprovince": None,
        "province": None,
        "postcode": None,
        "climate_classification_koppen": None,
        "climate_lookup_note": None,
        "geocode_error": None,
    }

    try:
        geo = reverse_geocode_nominatim(lat, lon, session)
        address = geo.get("address", {}) or {}

        result.update(
            {
                "display_name": geo.get("display_name"),
                "osm_type": geo.get("osm_type"),
                "osm_id": geo.get("osm_id"),
                "country": address.get("country"),
                "country_code": safe_str(address.get("country_code")),
                "neighbourhood": address.get("neighbourhood"),
                "quarter": address.get("quarter"),
                "city": address.get("city"),
                "subprovince": address.get("subprovince"),
                "province": address.get("province"),
                "postcode": address.get("postcode"),
            }
        )
    except Exception as e:
        result["geocode_error"] = f"{type(e).__name__}: {e}"

    return result
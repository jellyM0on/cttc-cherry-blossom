from __future__ import annotations

import argparse
import time

import pandas as pd
import requests

from src.config import (
    CHERRY_BLOSSOM_CLEANED_FILE,
    JMA_STATION_GEOLOCATION_DETAILS_FILE,
    PROCESSED_DATA_DIR,
    GEOCODE_WAIT_SECONDS,
    CLIMATE_WAIT_SECONDS,
    CLIMATE_COOLDOWN_REQUESTS,
)
from src.utils.io_utils import append_row_to_csv, load_completed_station_codes
from src.utils.value_utils import safe_str, is_empty_value
from src.enrichment.station_extractors import extract_unique_stations, build_station_lookup
from src.enrichment.output_schema import ensure_output_columns, merge_station_fields_into_output
from src.enrichment.geocoding import enrich_station_row
from src.enrichment.climate import fetch_climate_classification, fetch_koppen_from_meteostat_station_id

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only-modify-climate",
        action="store_true",
        help=(
            "Only update existing rows in the output CSV where "
            "'climate_classification_koppen' is empty."
        ),
    )
    return parser.parse_args()

def update_missing_climate_rows(output_file, station_lookup: pd.DataFrame) -> None:
    if not output_file.exists():
        raise FileNotFoundError(
            f"{output_file} does not exist. Run the normal mode first to create it."
        )

    df = pd.read_csv(
        output_file,
        dtype={"jma_station_code": str, "wmo_station_id": str},
    )
    df = ensure_output_columns(df)
    df = merge_station_fields_into_output(df, station_lookup)

    missing_mask = df["climate_classification_koppen"].apply(is_empty_value)
    rows_to_update = df[missing_mask].copy()

    print(f"Rows with empty climate_classification_koppen: {len(rows_to_update)}")

    if rows_to_update.empty:
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print("Nothing to update.")
        return

    for update_i, idx in enumerate(rows_to_update.index, start=1):
        station_code = safe_str(df.at[idx, "jma_station_code"])
        wmo_station_id = safe_str(df.at[idx, "wmo_station_id"])
        latitude = pd.to_numeric(df.at[idx, "latitude_deg"], errors="coerce")

        print(
            f"Updating climate with Meteostat for station {station_code} "
            f"(WMO={wmo_station_id}) ({update_i}/{len(rows_to_update)})..."
        )

        if is_empty_value(wmo_station_id):
            koppen = None
            note = "Skipped Meteostat lookup because wmo_station_id is missing. Derived koppen=None"
        elif pd.isna(latitude):
            koppen = None
            note = (
                f"Skipped Meteostat lookup for station {station_code} "
                f"because latitude is missing. Derived koppen=None"
            )
        else:
            koppen, note = fetch_koppen_from_meteostat_station_id(
                str(wmo_station_id),
                float(latitude),
            )

        df.at[idx, "climate_classification_koppen"] = koppen
        df.at[idx, "climate_lookup_note"] = note

        print(f"Station {station_code} (WMO={wmo_station_id}) -> derived koppen: {koppen}")

        df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"Climate-only Meteostat update complete. Saved to: {output_file.resolve()}")

def run_fetch_geolocation_data() -> None:
    args = parse_args()
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    input_df = pd.read_csv(CHERRY_BLOSSOM_CLEANED_FILE)
    station_lookup = build_station_lookup(input_df)

    if args.only_modify_climate:
        update_missing_climate_rows(JMA_STATION_GEOLOCATION_DETAILS_FILE, station_lookup)
        return

    stations = extract_unique_stations(input_df)

    if JMA_STATION_GEOLOCATION_DETAILS_FILE.exists():
        existing_output_df = pd.read_csv(
            JMA_STATION_GEOLOCATION_DETAILS_FILE,
            dtype={"jma_station_code": str, "wmo_station_id": str},
        )
        existing_output_df = ensure_output_columns(existing_output_df)
        existing_output_df = merge_station_fields_into_output(existing_output_df, station_lookup)
        existing_output_df.to_csv(
            JMA_STATION_GEOLOCATION_DETAILS_FILE,
            index=False,
            encoding="utf-8-sig",
        )

    completed_station_codes = load_completed_station_codes(JMA_STATION_GEOLOCATION_DETAILS_FILE)
    if completed_station_codes:
        print(
            f"Found {len(completed_station_codes)} existing processed stations in "
            f"{JMA_STATION_GEOLOCATION_DETAILS_FILE.name}"
        )

    stations_to_process = stations[
        ~stations["jma_station_code"].astype(str).str.strip().isin(completed_station_codes)
    ].reset_index(drop=True)

    print(f"Found {len(stations)} unique JMA stations total.")
    print(f"Remaining stations to process: {len(stations_to_process)}")

    session = requests.Session()
    climate_cooldown_remaining = 0

    for i, (_, row) in enumerate(stations_to_process.iterrows(), start=1):
        station_code = str(row["jma_station_code"]).strip()
        print(f"[{i}/{len(stations_to_process)}] Processing station {station_code}...")

        enriched = enrich_station_row(row, session)
        time.sleep(GEOCODE_WAIT_SECONDS)

        if enriched["geocode_error"] is not None:
            enriched["climate_lookup_note"] = "Skipped climate lookup because geocoding failed."

        elif climate_cooldown_remaining > 0:
            enriched["climate_lookup_note"] = (
                "Skipped climate lookup due to cooldown. "
                f"{climate_cooldown_remaining} cooldown station(s) remaining before this skip."
            )
            climate_cooldown_remaining -= 1
            print(
                f"Climate cooldown active. Skipping climate lookup for station {station_code}. "
                f"Remaining cooldown skips: {climate_cooldown_remaining}"
            )

        else:
            koppen, note, hit_429_limit = fetch_climate_classification(
                float(row["latitude_deg"]),
                float(row["longitude_deg"]),
                session,
            )
            enriched["climate_classification_koppen"] = koppen
            enriched["climate_lookup_note"] = (
                note
                if note is not None
                else f"Derived from climate API (1991-2020). Derived koppen={koppen}"
            )

            print(f"Station {station_code} -> derived koppen: {koppen}")

            if hit_429_limit:
                climate_cooldown_remaining = CLIMATE_COOLDOWN_REQUESTS
                print(
                    "Climate API hit repeated 429s. "
                    f"Skipping climate lookup for the next {CLIMATE_COOLDOWN_REQUESTS} stations."
                )

            time.sleep(CLIMATE_WAIT_SECONDS)

        append_row_to_csv(enriched, JMA_STATION_GEOLOCATION_DETAILS_FILE)
        print(f"Saved station {station_code} to {JMA_STATION_GEOLOCATION_DETAILS_FILE.name}")

    print(f"Done. Output saved to: {JMA_STATION_GEOLOCATION_DETAILS_FILE.resolve()}")
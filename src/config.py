import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

EMAIL = os.getenv("EMAIL", "").strip()

HEADERS = {
    "User-Agent": (
        f"cherry-blossom-station-enrichment/1.0 ({EMAIL})"
        if EMAIL
        else "cherry-blossom-station-enrichment/1.0"
    ),
    "Accept": "application/json",
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUTS_DIR / "figures"
TABLES_DIR = OUTPUTS_DIR / "tables"

# Input files
BLOSSOM_FILE = RAW_DATA_DIR / "blossom.csv"
WMO_FILE = RAW_DATA_DIR / "station.csv"

# STAGE 1
CHERRY_BLOSSOM_CLEANED_FILE = PROCESSED_DATA_DIR / "cherry_blossom_cleaned.csv"

BLOSSOM_REQUIRED_COLUMNS = {
    "year",
    "station_name_jp",
    "jma_station_code",
    "wmo_station_id",
    "event",
    "date",
}

WMO_REQUIRED_COLUMNS = {
    "wmo_index",
    "station_name",
    "country_area",
    "latitude_deg",
    "longitude_deg",
    "station_name_ja",
}

# STAGE 2
JMA_STATION_GEOLOCATION_DETAILS_FILE = PROCESSED_DATA_DIR / "jma_station_geolocation_details.csv"

HEADERS = {
    "User-Agent": "cherry-blossom-station-enrichment/1.0 (anonisrina07@gmail.com)",
    "Accept": "application/json",
}

NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

GEOCODE_WAIT_SECONDS = 1.2
CLIMATE_WAIT_SECONDS = 5.0

CLIMATE_MAX_RETRIES = 5
CLIMATE_RETRY_BASE_SLEEP = 5.0
CLIMATE_COOLDOWN_REQUESTS = 10

METEOSTAT_START = datetime(1991, 1, 1)
METEOSTAT_END = datetime(2020, 12, 31)
METEOSTAT_MIN_MONTHS = 120

# STAGE 3
CHERRY_BLOSSOM_ENRICHED_FILE = PROCESSED_DATA_DIR / "cherry_blossom_enriched.csv"
CHERRY_BLOSSOM_FULL_BLOOM_FILE = PROCESSED_DATA_DIR / "cherry_blossom_full_bloom.csv"
CHERRY_BLOSSOM_FLOWERING_FILE = PROCESSED_DATA_DIR / "cherry_blossom_flowering.csv"

FULL_BLOOM_NATIONAL_YEARLY_FILE = PROCESSED_DATA_DIR / "full_bloom_national_yearly.csv"
FULL_BLOOM_BY_CLIMATE_YEARLY_FILE = PROCESSED_DATA_DIR / "full_bloom_by_climate_yearly.csv"
FULL_BLOOM_BY_PROVINCE_YEARLY_FILE = PROCESSED_DATA_DIR / "full_bloom_by_province_yearly.csv"
FULL_BLOOM_BY_STATION_YEARLY_FILE = PROCESSED_DATA_DIR / "full_bloom_by_station_yearly.csv"
FULL_BLOOM_STATION_COVERAGE_FILE = PROCESSED_DATA_DIR / "full_bloom_station_coverage.csv"
FULL_BLOOM_DATA_QUALITY_FILE = PROCESSED_DATA_DIR / "full_bloom_data_quality_report.csv"

CLEANED_REQUIRED_COLUMNS = {
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
}

GEO_REQUIRED_COLUMNS = {
    "jma_station_code",
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
}

SELECTED_STATIONS_FULL_BLOOM_FILE = PROCESSED_DATA_DIR / "selected_stations_full_bloom.csv"
SELECTED_STATIONS_SUMMARY_FILE = PROCESSED_DATA_DIR / "selected_stations_summary.csv"

MIN_GROUP_RECORDS = 10
TOP_N_PROVINCES = 10
TOP_N_STATIONS = 10

MIN_SELECTED_STATION_YEARS = 20

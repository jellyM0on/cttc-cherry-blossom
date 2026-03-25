import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.enrichment.fetch_geolocation_data import run_fetch_geolocation_data

def main() -> None:
    run_fetch_geolocation_data()

if __name__ == "__main__":
    main()
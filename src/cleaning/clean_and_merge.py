from src.config import (
    PROCESSED_DATA_DIR,
    CHERRY_BLOSSOM_ENRICHED_FILE,
    CHERRY_BLOSSOM_FULL_BLOOM_FILE,
    CHERRY_BLOSSOM_FLOWERING_FILE,
    FULL_BLOOM_NATIONAL_YEARLY_FILE,
    FULL_BLOOM_BY_CLIMATE_YEARLY_FILE,
    FULL_BLOOM_BY_PROVINCE_YEARLY_FILE,
    FULL_BLOOM_BY_STATION_YEARLY_FILE,
    FULL_BLOOM_STATION_COVERAGE_FILE,
    FULL_BLOOM_DATA_QUALITY_FILE,
    SELECTED_STATIONS_FULL_BLOOM_FILE,
    SELECTED_STATIONS_SUMMARY_FILE,
    MIN_SELECTED_STATION_YEARS,
)
from src.cleaning.load_datasets_for_merge import (
    load_cleaned_dataset,
    load_geolocation_dataset,
)
from src.cleaning.merge_with_enrichment import (
    merge_cleaned_with_geolocation,
    split_event_datasets,
)
from src.cleaning.summary_builders import (
    build_national_yearly,
    build_climate_yearly,
    build_province_yearly,
    build_station_yearly,
    build_station_coverage,
    build_data_quality_report,
    build_selected_station_summary,
    filter_selected_stations,
)

def run_cleaning_and_merging() -> None:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Reading cleaned blossom data...")
    cleaned_df = load_cleaned_dataset()

    print("Reading geolocation details...")
    geo_df = load_geolocation_dataset()

    print("Merging cleaned blossom data with geolocation details...")
    enriched_df = merge_cleaned_with_geolocation(cleaned_df, geo_df)

    full_bloom_df, flowering_df = split_event_datasets(enriched_df)

    selected_station_summary_df = build_selected_station_summary(full_bloom_df)
    selected_stations_full_bloom_df, selected_stations_summary_df = filter_selected_stations(
        full_bloom_df=full_bloom_df,
        station_summary_df=selected_station_summary_df,
        min_years=MIN_SELECTED_STATION_YEARS,
    )

    national_yearly_df = build_national_yearly(full_bloom_df)
    climate_yearly_df = build_climate_yearly(full_bloom_df)
    province_yearly_df = build_province_yearly(full_bloom_df)
    station_yearly_df = build_station_yearly(full_bloom_df)
    station_coverage_df = build_station_coverage(full_bloom_df)
    quality_df = build_data_quality_report(enriched_df, full_bloom_df)

    enriched_df.to_csv(CHERRY_BLOSSOM_ENRICHED_FILE, index=False)
    full_bloom_df.to_csv(CHERRY_BLOSSOM_FULL_BLOOM_FILE, index=False)
    flowering_df.to_csv(CHERRY_BLOSSOM_FLOWERING_FILE, index=False)
    national_yearly_df.to_csv(FULL_BLOOM_NATIONAL_YEARLY_FILE, index=False)
    climate_yearly_df.to_csv(FULL_BLOOM_BY_CLIMATE_YEARLY_FILE, index=False)
    province_yearly_df.to_csv(FULL_BLOOM_BY_PROVINCE_YEARLY_FILE, index=False)
    station_yearly_df.to_csv(FULL_BLOOM_BY_STATION_YEARLY_FILE, index=False)
    station_coverage_df.to_csv(FULL_BLOOM_STATION_COVERAGE_FILE, index=False)
    quality_df.to_csv(FULL_BLOOM_DATA_QUALITY_FILE, index=False)

    selected_stations_full_bloom_df.to_csv(SELECTED_STATIONS_FULL_BLOOM_FILE, index=False)
    selected_stations_summary_df.to_csv(SELECTED_STATIONS_SUMMARY_FILE, index=False)

    print("\nDone.")
    print(f"Saved enriched dataset: {CHERRY_BLOSSOM_ENRICHED_FILE}")
    print(f"Saved full bloom dataset: {CHERRY_BLOSSOM_FULL_BLOOM_FILE}")
    print(f"Saved flowering dataset: {CHERRY_BLOSSOM_FLOWERING_FILE}")
    print(f"Saved national yearly summary: {FULL_BLOOM_NATIONAL_YEARLY_FILE}")
    print(f"Saved climate yearly summary: {FULL_BLOOM_BY_CLIMATE_YEARLY_FILE}")
    print(f"Saved province yearly summary: {FULL_BLOOM_BY_PROVINCE_YEARLY_FILE}")
    print(f"Saved station yearly dataset: {FULL_BLOOM_BY_STATION_YEARLY_FILE}")
    print(f"Saved station coverage summary: {FULL_BLOOM_STATION_COVERAGE_FILE}")
    print(f"Saved data quality report: {FULL_BLOOM_DATA_QUALITY_FILE}")

    print("\nData quality report:")
    print(quality_df)

    print(f"Saved selected stations full bloom dataset: {SELECTED_STATIONS_FULL_BLOOM_FILE}")
    print(f"Saved selected stations summary: {SELECTED_STATIONS_SUMMARY_FILE}")
    print(f"Selected stations meeting threshold ({MIN_SELECTED_STATION_YEARS}+ years): {len(selected_stations_summary_df):,}")
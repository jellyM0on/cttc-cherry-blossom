import pandas as pd

def _round_summary_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["mean_day_of_year", "median_day_of_year", "std_day_of_year"]:
        if col in df.columns:
            df[col] = df[col].round(2)
    return df

def build_national_yearly(full_bloom_df: pd.DataFrame) -> pd.DataFrame:
    out = (
        full_bloom_df.groupby("year", dropna=False)
        .agg(
            mean_day_of_year=("day_of_year", "mean"),
            median_day_of_year=("day_of_year", "median"),
            std_day_of_year=("day_of_year", "std"),
            min_day_of_year=("day_of_year", "min"),
            max_day_of_year=("day_of_year", "max"),
            station_count=("jma_station_code", "nunique"),
            record_count=("day_of_year", "size"),
        )
        .reset_index()
        .sort_values("year")
    )
    return _round_summary_columns(out)

def build_climate_yearly(full_bloom_df: pd.DataFrame) -> pd.DataFrame:
    climate_df = full_bloom_df[
        full_bloom_df["climate_classification_koppen"].notna()
        & (full_bloom_df["climate_classification_koppen"].astype(str).str.strip() != "")
    ].copy()

    out = (
        climate_df.groupby(["climate_classification_koppen", "year"], dropna=False)
        .agg(
            mean_day_of_year=("day_of_year", "mean"),
            median_day_of_year=("day_of_year", "median"),
            std_day_of_year=("day_of_year", "std"),
            min_day_of_year=("day_of_year", "min"),
            max_day_of_year=("day_of_year", "max"),
            station_count=("jma_station_code", "nunique"),
            record_count=("day_of_year", "size"),
        )
        .reset_index()
        .sort_values(["climate_classification_koppen", "year"])
    )
    return _round_summary_columns(out)

def build_province_yearly(full_bloom_df: pd.DataFrame) -> pd.DataFrame:
    province_df = full_bloom_df[
        full_bloom_df["province"].notna()
        & (full_bloom_df["province"].astype(str).str.strip() != "")
    ].copy()

    out = (
        province_df.groupby(["province", "year"], dropna=False)
        .agg(
            mean_day_of_year=("day_of_year", "mean"),
            median_day_of_year=("day_of_year", "median"),
            std_day_of_year=("day_of_year", "std"),
            min_day_of_year=("day_of_year", "min"),
            max_day_of_year=("day_of_year", "max"),
            station_count=("jma_station_code", "nunique"),
            record_count=("day_of_year", "size"),
        )
        .reset_index()
        .sort_values(["province", "year"])
    )
    return _round_summary_columns(out)

def build_station_yearly(full_bloom_df: pd.DataFrame) -> pd.DataFrame:
    return (
        full_bloom_df.groupby(
            [
                "jma_station_code",
                "wmo_station_id",
                "station_name_jp",
                "station_name_en",
                "station_name_ja",
                "province",
                "climate_classification_koppen",
                "year",
            ],
            dropna=False,
        )
        .agg(
            day_of_year=("day_of_year", "first"),
            date=("date", "first"),
            latitude_deg=("latitude_deg", "first"),
            longitude_deg=("longitude_deg", "first"),
            city=("city", "first"),
            subprovince=("subprovince", "first"),
        )
        .reset_index()
        .sort_values(["station_name_en", "year"])
    )

def build_station_coverage(full_bloom_df: pd.DataFrame) -> pd.DataFrame:
    out = (
        full_bloom_df.groupby(
            [
                "jma_station_code",
                "wmo_station_id",
                "station_name_jp",
                "station_name_en",
                "station_name_ja",
                "province",
                "climate_classification_koppen",
            ],
            dropna=False,
        )
        .agg(
            first_year=("year", "min"),
            last_year=("year", "max"),
            total_years=("year", "nunique"),
            total_records=("year", "size"),
            mean_day_of_year=("day_of_year", "mean"),
            median_day_of_year=("day_of_year", "median"),
            std_day_of_year=("day_of_year", "std"),
            earliest_day_of_year=("day_of_year", "min"),
            latest_day_of_year=("day_of_year", "max"),
            latitude_deg=("latitude_deg", "first"),
            longitude_deg=("longitude_deg", "first"),
            city=("city", "first"),
            subprovince=("subprovince", "first"),
        )
        .reset_index()
        .sort_values(["total_years", "station_name_en"], ascending=[False, True])
    )
    return _round_summary_columns(out)

def build_data_quality_report(
    enriched_df: pd.DataFrame,
    full_bloom_df: pd.DataFrame,
) -> pd.DataFrame:
    metrics = [
        ("rows_enriched_total", len(enriched_df)),
        ("rows_full_bloom", len(full_bloom_df)),
        ("unique_stations_full_bloom", full_bloom_df["jma_station_code"].nunique()),
        ("unique_years_full_bloom", full_bloom_df["year"].nunique()),
        ("rows_missing_province_full_bloom", full_bloom_df["province"].isna().sum()),
        (
            "rows_missing_climate_full_bloom",
            full_bloom_df["climate_classification_koppen"].isna().sum()
            + full_bloom_df["climate_classification_koppen"].astype(str).str.strip().eq("").sum(),
        ),
        ("unique_provinces_full_bloom", full_bloom_df["province"].dropna().nunique()),
        (
            "unique_climate_classes_full_bloom",
            full_bloom_df.loc[
                full_bloom_df["climate_classification_koppen"].notna()
                & (full_bloom_df["climate_classification_koppen"].astype(str).str.strip() != ""),
                "climate_classification_koppen",
            ].nunique(),
        ),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value"])

def build_selected_station_summary(
    full_bloom_df: pd.DataFrame,
) -> pd.DataFrame:
    summary = (
        full_bloom_df.groupby(
            [
                "jma_station_code",
                "station_name_jp",
                "station_name_en",
                "station_name_ja",
                "wmo_station_id",
                "province",
                "climate_classification_koppen",
            ],
            dropna=False,
        )
        .agg(
            total_years=("year", "nunique"),
            total_records=("year", "size"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            mean_day_of_year=("day_of_year", "mean"),
            median_day_of_year=("day_of_year", "median"),
            std_day_of_year=("day_of_year", "std"),
            earliest_day_of_year=("day_of_year", "min"),
            latest_day_of_year=("day_of_year", "max"),
            latitude_deg=("latitude_deg", "first"),
            longitude_deg=("longitude_deg", "first"),
            city=("city", "first"),
            subprovince=("subprovince", "first"),
        )
        .reset_index()
        .sort_values(["total_years", "station_name_en"], ascending=[False, True])
    )

    for col in ["mean_day_of_year", "median_day_of_year", "std_day_of_year"]:
        if col in summary.columns:
            summary[col] = summary[col].round(2)

    return summary

def filter_selected_stations(
    full_bloom_df: pd.DataFrame,
    station_summary_df: pd.DataFrame,
    min_years: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected_station_codes = (
        station_summary_df.loc[
            station_summary_df["total_years"] >= min_years,
            "jma_station_code",
        ]
        .dropna()
        .astype(str)
        .str.strip()
    )

    selected_df = full_bloom_df[
        full_bloom_df["jma_station_code"].astype(str).str.strip().isin(selected_station_codes)
    ].copy()

    selected_df = (
        selected_df.sort_values(["station_name_en", "year"])
        .reset_index(drop=True)
    )

    selected_summary_df = station_summary_df[
        station_summary_df["total_years"] >= min_years
    ].copy()

    selected_summary_df = (
        selected_summary_df.sort_values(["total_years", "station_name_en"], ascending=[False, True])
        .reset_index(drop=True)
    )

    return selected_df, selected_summary_df
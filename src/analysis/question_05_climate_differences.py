import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR, MIN_GROUP_RECORDS
from src.utils.analysis_utils import valid_nonempty_mask

def run(full_bloom: pd.DataFrame, climate_yearly: pd.DataFrame) -> pd.DataFrame:
    climate_summary = climate_yearly[
        valid_nonempty_mask(climate_yearly["climate_classification_koppen"])
    ].copy()

    summary = (
        climate_summary.groupby("climate_classification_koppen", as_index=False)
        .agg(
            mean_day_of_year=("mean_day_of_year", "mean"),
            median_day_of_year=("median_day_of_year", "mean"),
            mean_std_day_of_year=("std_day_of_year", "mean"),
            earliest_day_of_year=("min_day_of_year", "min"),
            latest_day_of_year=("max_day_of_year", "max"),
            total_years=("year", "nunique"),
            total_station_observations=("station_count", "sum"),
            total_records=("record_count", "sum"),
        )
        .sort_values("mean_day_of_year")
    )

    summary = summary[summary["total_records"] >= MIN_GROUP_RECORDS].copy()
    summary.to_csv(TABLES_DIR / "05_climate_influence_summary.csv", index=False)

    climate_df = full_bloom[valid_nonempty_mask(full_bloom["climate_classification_koppen"])].copy()
    counts = climate_df["climate_classification_koppen"].value_counts()
    valid_classes = counts[counts >= MIN_GROUP_RECORDS].index.tolist()
    climate_df = climate_df[climate_df["climate_classification_koppen"].isin(valid_classes)].copy()

    groups = []
    labels = []
    for climate in sorted(valid_classes):
        vals = climate_df.loc[
            climate_df["climate_classification_koppen"] == climate, "day_of_year"
        ].dropna()
        if len(vals) > 0:
            groups.append(vals)
            labels.append(climate)

    if groups:
        plt.figure(figsize=(11, 6))
        plt.boxplot(groups, tick_labels=labels)
        plt.xlabel("Köppen climate classification")
        plt.ylabel("Day of year")
        plt.title("Full Bloom Timing by Climate Classification")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "05_climate_distribution.png", bbox_inches="tight")
        plt.close()

    return summary
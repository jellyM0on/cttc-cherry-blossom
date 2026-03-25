import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR, MIN_GROUP_RECORDS
from src.utils.analysis_utils import compute_linear_trend, valid_nonempty_mask

def run(climate_yearly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    climate_yearly = climate_yearly[
        valid_nonempty_mask(climate_yearly["climate_classification_koppen"])
    ].copy()

    rows = []
    for climate, group in climate_yearly.groupby("climate_classification_koppen"):
        total_records = group["record_count"].sum()
        if total_records < MIN_GROUP_RECORDS:
            continue

        slope_mean, intercept_mean = compute_linear_trend(group, "year", "mean_day_of_year")
        slope_median, intercept_median = compute_linear_trend(group, "year", "median_day_of_year")

        rows.append(
            {
                "climate_classification_koppen": climate,
                "trend_slope_mean_days_per_year": slope_mean,
                "trend_intercept_mean": intercept_mean,
                "trend_slope_median_days_per_year": slope_median,
                "trend_intercept_median": intercept_median,
                "first_year": group["year"].min(),
                "last_year": group["year"].max(),
                "year_count": group["year"].nunique(),
                "total_records": total_records,
                "total_station_observations": group["station_count"].sum(),
            }
        )

    summary = pd.DataFrame(rows).sort_values("climate_classification_koppen")
    climate_yearly.to_csv(TABLES_DIR / "06_climate_trends_yearly_copy.csv", index=False)
    summary.to_csv(TABLES_DIR / "06_climate_trends_summary.csv", index=False)

    totals = climate_yearly.groupby("climate_classification_koppen")["record_count"].sum()
    valid_classes = totals[totals >= MIN_GROUP_RECORDS].index.tolist()
    climate_plot_df = climate_yearly[
        climate_yearly["climate_classification_koppen"].isin(valid_classes)
    ].copy()

    plt.figure(figsize=(11, 6))
    for climate, group in climate_plot_df.groupby("climate_classification_koppen"):
        plt.plot(group["year"], group["mean_day_of_year"], label=climate)

    plt.xlabel("Year")
    plt.ylabel("Mean day of year")
    plt.title("Full Bloom Trends by Climate Classification")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "06_climate_trends.png", bbox_inches="tight")
    plt.close()

    return climate_yearly, summary
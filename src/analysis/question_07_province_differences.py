import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR, MIN_GROUP_RECORDS, TOP_N_PROVINCES
from src.utils.analysis_utils import compute_linear_trend, valid_nonempty_mask

def run(full_bloom: pd.DataFrame, province_yearly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    province_yearly = province_yearly[valid_nonempty_mask(province_yearly["province"])].copy()

    summary = (
        province_yearly.groupby("province", as_index=False)
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

    trend_rows = []
    for province, group in province_yearly.groupby("province"):
        total_records = group["record_count"].sum()
        if total_records < MIN_GROUP_RECORDS:
            continue

        slope_mean, intercept_mean = compute_linear_trend(group, "year", "mean_day_of_year")
        slope_median, intercept_median = compute_linear_trend(group, "year", "median_day_of_year")

        trend_rows.append(
            {
                "province": province,
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

    trend_summary = pd.DataFrame(trend_rows).sort_values("province")

    top_provinces = (
        summary.sort_values("total_records", ascending=False)
        .head(TOP_N_PROVINCES)[["province", "total_records", "mean_day_of_year"]]
        .copy()
    )

    summary.to_csv(TABLES_DIR / "07_province_summary.csv", index=False)
    trend_summary.to_csv(TABLES_DIR / "08_province_trends_summary.csv", index=False)
    top_provinces.to_csv(TABLES_DIR / "08_top_provinces_for_visuals.csv", index=False)

    province_df = full_bloom[valid_nonempty_mask(full_bloom["province"])].copy()
    province_counts = province_df["province"].value_counts()
    top_plot_provinces = province_counts.head(TOP_N_PROVINCES).index.tolist()
    province_df = province_df[province_df["province"].isin(top_plot_provinces)].copy()

    groups = []
    labels = []
    for province in top_plot_provinces:
        vals = province_df.loc[province_df["province"] == province, "day_of_year"].dropna()
        if len(vals) > 0:
            groups.append(vals)
            labels.append(province)

    if groups:
        plt.figure(figsize=(12, 6))
        plt.boxplot(groups, tick_labels=labels)
        plt.xlabel("Province")
        plt.ylabel("Day of year")
        plt.title(f"Full Bloom Timing by Province (Top {TOP_N_PROVINCES})")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "07_province_distribution.png", bbox_inches="tight")
        plt.close()

    province_plot_df = province_yearly[province_yearly["province"].isin(top_plot_provinces)].copy()
    plt.figure(figsize=(12, 6))
    for province, group in province_plot_df.groupby("province"):
        plt.plot(group["year"], group["mean_day_of_year"], label=province)

    plt.xlabel("Year")
    plt.ylabel("Mean day of year")
    plt.title(f"Full Bloom Trends by Province (Top {TOP_N_PROVINCES})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "08_province_trends.png", bbox_inches="tight")
    plt.close()

    return summary, trend_summary, top_provinces
import matplotlib.pyplot as plt
import pandas as pd

from src.config import FIGURES_DIR, TABLES_DIR, TOP_N_STATIONS

def run(
    station_yearly: pd.DataFrame,
    station_coverage: pd.DataFrame,
    selected_full_bloom: pd.DataFrame,
    selected_summary: pd.DataFrame,
) -> pd.DataFrame:
    station_yearly.to_csv(TABLES_DIR / "09_station_yearly_copy.csv", index=False)
    station_coverage.to_csv(TABLES_DIR / "09_station_coverage_copy.csv", index=False)
    selected_full_bloom.to_csv(TABLES_DIR / "09_selected_stations_full_bloom_copy.csv", index=False)
    selected_summary.to_csv(TABLES_DIR / "09_selected_stations_summary_copy.csv", index=False)

    station_order = (
        selected_summary.sort_values(
            ["total_years", "station_name_en"], ascending=[False, True]
        )["station_name_en"]
        .head(TOP_N_STATIONS)
        .tolist()
    )

    plt.figure(figsize=(12, 6))
    for station in station_order:
        group = selected_full_bloom[selected_full_bloom["station_name_en"] == station].copy()
        if group.empty:
            continue
        yearly = group.groupby("year", as_index=False)["day_of_year"].mean().sort_values("year")
        plt.plot(yearly["year"], yearly["day_of_year"], label=station)

    plt.xlabel("Year")
    plt.ylabel("Day of year")
    plt.title(f"Selected Long-Record Station Trends (Top {TOP_N_STATIONS})")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "09_selected_station_trends.png", bbox_inches="tight")
    plt.close()

    return selected_summary
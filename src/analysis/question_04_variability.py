import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR
from src.utils.analysis_utils import compute_linear_trend
from src.analysis.plotting import add_trend_line

def run(national_yearly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    yearly = national_yearly.copy()
    yearly["range_day_of_year"] = yearly["max_day_of_year"] - yearly["min_day_of_year"]

    slope_std, intercept_std = compute_linear_trend(yearly, "year", "std_day_of_year")
    slope_range, intercept_range = compute_linear_trend(yearly, "year", "range_day_of_year")

    summary = pd.DataFrame(
        [
            {"metric": "mean_yearly_std_day_of_year", "value": yearly["std_day_of_year"].mean()},
            {"metric": "median_yearly_std_day_of_year", "value": yearly["std_day_of_year"].median()},
            {"metric": "mean_yearly_range_day_of_year", "value": yearly["range_day_of_year"].mean()},
            {"metric": "std_trend_slope_days_per_year", "value": slope_std},
            {"metric": "std_trend_intercept", "value": intercept_std},
            {"metric": "range_trend_slope_days_per_year", "value": slope_range},
            {"metric": "range_trend_intercept", "value": intercept_range},
        ]
    )

    yearly.to_csv(TABLES_DIR / "04_variability_yearly.csv", index=False)
    summary.to_csv(TABLES_DIR / "04_variability_summary.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.plot(yearly["year"], yearly["std_day_of_year"], label="Std dev")
    plt.plot(yearly["year"], yearly["range_day_of_year"], label="Range")
    add_trend_line(yearly["year"], yearly["std_day_of_year"])
    plt.xlabel("Year")
    plt.ylabel("Days")
    plt.title("Variability in Full Bloom Timing Over Time")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "04_variability_over_time.png", bbox_inches="tight")
    plt.close()

    return yearly, summary
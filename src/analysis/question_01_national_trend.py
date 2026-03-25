import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR
from src.utils.analysis_utils import compute_linear_trend
from src.analysis.plotting import add_trend_line

def run(national_yearly: pd.DataFrame) -> pd.DataFrame:
    slope_mean, intercept_mean = compute_linear_trend(
        national_yearly, "year", "mean_day_of_year"
    )
    slope_median, intercept_median = compute_linear_trend(
        national_yearly, "year", "median_day_of_year"
    )

    summary = pd.DataFrame(
        [
            {"metric": "mean_trend_slope_days_per_year", "value": slope_mean},
            {"metric": "mean_trend_intercept", "value": intercept_mean},
            {"metric": "median_trend_slope_days_per_year", "value": slope_median},
            {"metric": "median_trend_intercept", "value": intercept_median},
            {"metric": "first_year", "value": national_yearly["year"].min()},
            {"metric": "last_year", "value": national_yearly["year"].max()},
            {
                "metric": "interpretation",
                "value": "Negative slope indicates earlier full bloom over time.",
            },
        ]
    )

    summary.to_csv(TABLES_DIR / "01_national_trend_summary.csv", index=False)
    national_yearly.to_csv(TABLES_DIR / "01_national_trend_yearly_copy.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.plot(national_yearly["year"], national_yearly["mean_day_of_year"], label="Yearly mean")
    plt.plot(national_yearly["year"], national_yearly["median_day_of_year"], label="Yearly median")
    add_trend_line(national_yearly["year"], national_yearly["mean_day_of_year"])
    plt.xlabel("Year")
    plt.ylabel("Day of year")
    plt.title("Japan Full Bloom Trend Over Time")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "01_national_trend.png", bbox_inches="tight")
    plt.close()

    return summary
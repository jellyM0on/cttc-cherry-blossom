import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR

def run(full_bloom: pd.DataFrame, national_yearly: pd.DataFrame) -> pd.DataFrame:
    summary = pd.DataFrame(
        [
            {"metric": "overall_mean_day_of_year", "value": full_bloom["day_of_year"].mean()},
            {"metric": "overall_median_day_of_year", "value": full_bloom["day_of_year"].median()},
            {"metric": "overall_std_day_of_year", "value": full_bloom["day_of_year"].std()},
            {"metric": "q1_day_of_year", "value": full_bloom["day_of_year"].quantile(0.25)},
            {"metric": "q3_day_of_year", "value": full_bloom["day_of_year"].quantile(0.75)},
            {"metric": "mean_of_yearly_means", "value": national_yearly["mean_day_of_year"].mean()},
            {"metric": "mean_of_yearly_medians", "value": national_yearly["median_day_of_year"].mean()},
        ]
    )

    summary.to_csv(TABLES_DIR / "02_bloom_timing_summary.csv", index=False)

    plt.figure(figsize=(10, 6))
    plt.hist(full_bloom["day_of_year"].dropna(), bins=30)
    plt.xlabel("Day of year")
    plt.ylabel("Frequency")
    plt.title("Distribution of Full Bloom Dates")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "02_bloom_distribution.png", bbox_inches="tight")
    plt.close()

    return summary
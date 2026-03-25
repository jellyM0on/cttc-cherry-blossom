import pandas as pd
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, TABLES_DIR

def run(full_bloom: pd.DataFrame, national_yearly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    earliest_record = full_bloom.loc[full_bloom["day_of_year"].idxmin()]
    latest_record = full_bloom.loc[full_bloom["day_of_year"].idxmax()]

    record_extremes = pd.DataFrame(
        [
            {
                "type": "earliest_record",
                "year": earliest_record["year"],
                "date": earliest_record["date"],
                "day_of_year": earliest_record["day_of_year"],
                "station_name_en": earliest_record["station_name_en"],
                "station_name_jp": earliest_record["station_name_jp"],
                "province": earliest_record["province"],
                "climate_classification_koppen": earliest_record["climate_classification_koppen"],
            },
            {
                "type": "latest_record",
                "year": latest_record["year"],
                "date": latest_record["date"],
                "day_of_year": latest_record["day_of_year"],
                "station_name_en": latest_record["station_name_en"],
                "station_name_jp": latest_record["station_name_jp"],
                "province": latest_record["province"],
                "climate_classification_koppen": latest_record["climate_classification_koppen"],
            },
        ]
    )

    earliest_mean_year = national_yearly.loc[national_yearly["mean_day_of_year"].idxmin()]
    latest_mean_year = national_yearly.loc[national_yearly["mean_day_of_year"].idxmax()]
    earliest_median_year = national_yearly.loc[national_yearly["median_day_of_year"].idxmin()]
    latest_median_year = national_yearly.loc[national_yearly["median_day_of_year"].idxmax()]

    year_summary = pd.DataFrame(
        [
            {"type": "earliest_mean_bloom_year", "year": earliest_mean_year["year"], "mean_day_of_year": earliest_mean_year["mean_day_of_year"]},
            {"type": "latest_mean_bloom_year", "year": latest_mean_year["year"], "mean_day_of_year": latest_mean_year["mean_day_of_year"]},
            {"type": "earliest_median_bloom_year", "year": earliest_median_year["year"], "median_day_of_year": earliest_median_year["median_day_of_year"]},
            {"type": "latest_median_bloom_year", "year": latest_median_year["year"], "median_day_of_year": latest_median_year["median_day_of_year"]},
        ]
    )

    record_extremes.to_csv(TABLES_DIR / "03_record_extremes.csv", index=False)
    year_summary.to_csv(TABLES_DIR / "03_year_extremes_summary.csv", index=False)
    national_yearly.to_csv(TABLES_DIR / "03_national_yearly_extremes_context.csv", index=False)

    plt.figure(figsize=(11, 6))
    plt.plot(national_yearly["year"], national_yearly["min_day_of_year"], label="Earliest")
    plt.plot(national_yearly["year"], national_yearly["max_day_of_year"], label="Latest")
    plt.plot(national_yearly["year"], national_yearly["mean_day_of_year"], label="Mean")
    plt.xlabel("Year")
    plt.ylabel("Day of year")
    plt.title("Earliest, Latest, and Mean Full Bloom by Year")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "03_extremes_by_year.png", bbox_inches="tight")
    plt.close()

    return record_extremes, year_summary
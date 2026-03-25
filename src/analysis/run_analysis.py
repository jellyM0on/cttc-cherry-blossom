from src.config import FIGURES_DIR, TABLES_DIR
from src.analysis.load_datasets_for_analysis import load_analysis_inputs
from src.analysis.plotting import configure_matplotlib_for_japanese

from src.analysis.question_01_national_trend import run as run_q01
from src.analysis.question_02_typical_bloom_timing import run as run_q02
from src.analysis.question_03_extremes import run as run_q03
from src.analysis.question_04_variability import run as run_q04
from src.analysis.question_05_climate_differences import run as run_q05
from src.analysis.question_06_climate_trends import run as run_q06
from src.analysis.question_07_province_differences import run as run_q07
from src.analysis.question_08_station_trends import run as run_q08

def run_analysis() -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    selected_font = configure_matplotlib_for_japanese()
    if selected_font is not None:
        print(f"Using matplotlib font: {selected_font}")
    else:
        print(
            "Warning: no Japanese-capable matplotlib font was found. "
            "Japanese labels may not render correctly."
        )

    data = load_analysis_inputs()

    print("\n--- 1. NATIONAL TREND ---")
    print(
        run_q01(
            national_yearly=data["national_yearly"],
        )
    )

    print("\n--- 2. TYPICAL BLOOM TIMING ---")
    print(
        run_q02(
            full_bloom=data["full_bloom"],
            national_yearly=data["national_yearly"],
        )
    )

    print("\n--- 3. EXTREMES ---")
    record_extremes, year_summary = run_q03(
        full_bloom=data["full_bloom"],
        national_yearly=data["national_yearly"],
    )
    print(record_extremes)
    print(year_summary)

    print("\n--- 4. VARIABILITY ---")
    _, variability_summary = run_q04(
        national_yearly=data["national_yearly"],
    )
    print(variability_summary)

    print("\n--- 5. CLIMATE DIFFERENCES ---")
    print(
        run_q05(
            full_bloom=data["full_bloom"],
            climate_yearly=data["climate_yearly"],
        )
    )

    print("\n--- 6. CLIMATE TRENDS ---")
    _, climate_trend_summary = run_q06(
        climate_yearly=data["climate_yearly"],
    )
    print(climate_trend_summary)

    print("\n--- 7. PROVINCE DIFFERENCES / TRENDS ---")
    province_summary, province_trend_summary, top_provinces = run_q07(
        full_bloom=data["full_bloom"],
        province_yearly=data["province_yearly"],
    )
    print(province_summary.head(10))
    print(province_trend_summary.head(10))
    print(top_provinces)

    print("\n--- 8. STATION TRENDS ---")
    print(
        run_q08(
            station_yearly=data["station_yearly"],
            station_coverage=data["station_coverage"],
            selected_full_bloom=data["selected_full_bloom"],
            selected_summary=data["selected_summary"],
        ).head(10)
    )

    print("\nAll analysis tables and figures saved.")
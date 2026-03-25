import time

import numpy as np
import pandas as pd
import requests
from meteostat import monthly

from src.config import (
    HEADERS,
    OPEN_METEO_ARCHIVE_URL,
    CLIMATE_MAX_RETRIES,
    CLIMATE_RETRY_BASE_SLEEP,
    CLIMATE_COOLDOWN_REQUESTS,
    METEOSTAT_START,
    METEOSTAT_END,
    METEOSTAT_MIN_MONTHS,
)
from src.utils.value_utils import first_non_empty

def classify_koppen(
    monthly_temp_c: np.ndarray,
    monthly_precip_mm: np.ndarray,
    latitude: float,
) -> str | None:
    if len(monthly_temp_c) != 12 or len(monthly_precip_mm) != 12:
        return None
    if np.isnan(monthly_temp_c).any() or np.isnan(monthly_precip_mm).any():
        return None

    annual_temp = float(np.mean(monthly_temp_c))
    annual_precip = float(np.sum(monthly_precip_mm))
    t_cold = float(np.min(monthly_temp_c))
    t_hot = float(np.max(monthly_temp_c))
    months_gt_10 = int(np.sum(monthly_temp_c > 10))

    if latitude >= 0:
        summer_idx = np.array([3, 4, 5, 6, 7, 8])
        winter_idx = np.array([9, 10, 11, 0, 1, 2])
    else:
        summer_idx = np.array([9, 10, 11, 0, 1, 2])
        winter_idx = np.array([3, 4, 5, 6, 7, 8])

    p_summer = float(np.sum(monthly_precip_mm[summer_idx]))
    p_dry = float(np.min(monthly_precip_mm))
    p_dry_summer = float(np.min(monthly_precip_mm[summer_idx]))
    p_wet_winter = float(np.max(monthly_precip_mm[winter_idx]))
    p_dry_winter = float(np.min(monthly_precip_mm[winter_idx]))
    p_wet_summer = float(np.max(monthly_precip_mm[summer_idx]))

    summer_fraction = p_summer / annual_precip if annual_precip > 0 else 0.0

    if summer_fraction >= 0.70:
        p_threshold = 20 * annual_temp + 280
    elif summer_fraction >= 0.30:
        p_threshold = 20 * annual_temp + 140
    else:
        p_threshold = 20 * annual_temp

    if annual_precip < p_threshold:
        main = "BW" if annual_precip < 0.5 * p_threshold else "BS"
        thermal = "h" if annual_temp >= 18 else "k"
        return main + thermal

    if t_cold >= 18:
        if p_dry >= 60:
            return "Af"
        elif p_dry >= 100 - (annual_precip / 25):
            return "Am"
        return "Aw"

    if t_hot < 10:
        return "EF" if t_hot < 0 else "ET"

    climate_group = "C" if t_cold > 0 else "D"

    if p_dry_summer < 40 and p_dry_summer < (p_wet_winter / 3):
        season = "s"
    elif p_dry_winter < (p_wet_summer / 10):
        season = "w"
    else:
        season = "f"

    if t_hot >= 22:
        heat = "a"
    elif months_gt_10 >= 4:
        heat = "b"
    elif 1 <= months_gt_10 <= 3:
        heat = "c"
    else:
        heat = "d"

    return climate_group + season + heat

def monthly_df_to_koppen(monthly_df: pd.DataFrame, latitude: float) -> str | None:
    if monthly_df is None or monthly_df.empty:
        return None

    df = monthly_df.copy()

    if not isinstance(df.index, pd.DatetimeIndex):
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df = df.dropna(subset=["time"]).set_index("time")
        else:
            return None

    temp_col = first_non_empty(
        "tavg" if "tavg" in df.columns else None,
        "temp" if "temp" in df.columns else None,
    )
    precip_col = first_non_empty(
        "prcp" if "prcp" in df.columns else None,
        "precip" if "precip" in df.columns else None,
    )

    if temp_col is None or precip_col is None:
        return None

    usable = df[[temp_col, precip_col]].copy()
    usable[temp_col] = pd.to_numeric(usable[temp_col], errors="coerce")
    usable[precip_col] = pd.to_numeric(usable[precip_col], errors="coerce")
    usable = usable.dropna(subset=[temp_col, precip_col])

    if len(usable) < METEOSTAT_MIN_MONTHS:
        return None

    usable["month"] = usable.index.month
    climatology = usable.groupby("month").agg(
        temp_mean_c=(temp_col, "mean"),
        precip_mm=(precip_col, "mean"),
    ).reindex(range(1, 13))

    monthly_temp = climatology["temp_mean_c"].to_numpy(dtype=float)
    monthly_precip = climatology["precip_mm"].to_numpy(dtype=float)

    return classify_koppen(monthly_temp, monthly_precip, latitude)

def fetch_koppen_from_meteostat_station_id(
    wmo_station_id: str,
    latitude: float,
) -> tuple[str | None, str]:
    try:
        monthly_df = monthly(str(wmo_station_id), METEOSTAT_START, METEOSTAT_END).fetch()
        koppen = monthly_df_to_koppen(monthly_df, latitude)

        if koppen is not None:
            return koppen, (
                f"Derived from Meteostat station {wmo_station_id} monthly data "
                f"(1991-2020). Derived koppen={koppen}"
            )

        return None, (
            f"Meteostat station {wmo_station_id} monthly data was insufficient for "
            f"Köppen classification. Derived koppen=None"
        )
    except Exception as e:
        return None, (
            f"Meteostat station {wmo_station_id} lookup failed: "
            f"{type(e).__name__}: {e}. Derived koppen=None"
        )

def fetch_climate_classification(
    lat: float,
    lon: float,
    session: requests.Session,
) -> tuple[str | None, str | None, bool]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": "1991-01-01",
        "end_date": "2020-12-31",
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "GMT",
    }

    saw_429 = False

    for attempt in range(1, CLIMATE_MAX_RETRIES + 1):
        try:
            response = session.get(
                OPEN_METEO_ARCHIVE_URL,
                params=params,
                headers=HEADERS,
                timeout=120,
            )

            if response.status_code == 429:
                saw_429 = True
                wait_s = CLIMATE_RETRY_BASE_SLEEP * attempt
                print(
                    f"Climate API 429 for lat={lat}, lon={lon}. "
                    f"Retry {attempt}/{CLIMATE_MAX_RETRIES} after {wait_s:.1f}s."
                )
                time.sleep(wait_s)
                continue

            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})
            dates = daily.get("time")
            temps = daily.get("temperature_2m_mean")
            precip = daily.get("precipitation_sum")

            if not dates or temps is None or precip is None:
                return None, "No daily climate data returned.", False

            climate_df = pd.DataFrame(
                {
                    "date": pd.to_datetime(dates),
                    "temp_mean_c": pd.to_numeric(temps, errors="coerce"),
                    "precip_mm": pd.to_numeric(precip, errors="coerce"),
                }
            ).dropna(subset=["date"])

            climate_df["month"] = climate_df["date"].dt.month

            monthly_df = (
                climate_df.groupby("month", as_index=True)
                .agg(
                    temp_mean_c=("temp_mean_c", "mean"),
                    precip_mm=("precip_mm", "mean"),
                )
                .reindex(range(1, 13))
            )

            avg_days_per_month = (
                climate_df.assign(days_in_month=climate_df["date"].dt.days_in_month)
                .groupby("month")["days_in_month"]
                .mean()
                .reindex(range(1, 13))
            )

            monthly_temp = monthly_df["temp_mean_c"].to_numpy(dtype=float)
            monthly_precip = (monthly_df["precip_mm"] * avg_days_per_month).to_numpy(dtype=float)

            koppen = classify_koppen(monthly_temp, monthly_precip, lat)
            return koppen, None, False

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            wait_s = CLIMATE_RETRY_BASE_SLEEP * attempt
            print(
                f"Climate API connection error for lat={lat}, lon={lon}: {e}. "
                f"Retry {attempt}/{CLIMATE_MAX_RETRIES} after {wait_s:.1f}s."
            )
            time.sleep(wait_s)

        except requests.exceptions.RequestException as e:
            return None, f"Climate API request failed: {type(e).__name__}: {e}", False

    if saw_429:
        return None, (
            f"Climate API returned 429 after {CLIMATE_MAX_RETRIES} attempts. "
            f"Entering cooldown for next {CLIMATE_COOLDOWN_REQUESTS} stations."
        ), True

    return None, "Climate API failed after maximum retries.", False
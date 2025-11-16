# ingestion/env_api.py

from typing import List, Tuple
import pandas as pd

from config import WEATHER_API_KEY, WEATHER_API_BASE_URL
from ingestion.utils import safe_request, save_raw_csv


def fetch_weather_monthly(lat: float, lon: float, start_year: int, end_year: int) -> pd.DataFrame:
    """
    좌표 기준 월별 기후 데이터(평균기온, 강수량 등)를 가져온다고 가정.
    실제 기상청/기후 API에 맞게 파라미터 수정 필요.
    """
    if not WEATHER_API_KEY:
        raise ValueError("WEATHER_API_KEY 환경변수가 설정되지 않았습니다.")

    rows = []
    for year in range(start_year, end_year + 1):
        params = {
            "apiKey": WEATHER_API_KEY,
            "lat": lat,
            "lon": lon,
            "year": year,
            "resolution": "monthly",
        }
        resp = safe_request("GET", WEATHER_API_BASE_URL, params=params)
        data = resp.json()
        # 실제 구조 확인 필요. 예시:
        monthly = data.get("monthly", [])
        for m in monthly:
            rows.append({
                "lat": lat,
                "lon": lon,
                "year": year,
                "month": m["month"],
                "temperature_avg": m.get("temp_avg"),
                "temperature_max": m.get("temp_max"),
                "rainfall_avg": m.get("rainfall"),
                "humidity_avg": m.get("humidity"),
            })

    df = pd.DataFrame(rows)
    return df


def run_env_ingestion(sites: List[Tuple[str, float, float]]) -> pd.DataFrame:
    """
    sites: (site_id, lat, lon)
    """
    all_frames = []
    for site_id, lat, lon in sites:
        df = fetch_weather_monthly(lat, lon, start_year=2013, end_year=2024)
        df["site_id"] = site_id
        all_frames.append(df)
    full = pd.concat(all_frames, ignore_index=True)
    save_raw_csv(full, "env/weather_monthly_raw.csv")
    return full

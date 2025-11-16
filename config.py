# config.py

from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# .env에서 불러오도록 설계 (API 키는 코드에 하드코딩 X)
LAND_API_KEY = os.getenv("LAND_API_KEY")          # 국토부 토지 API 키
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")    # 기상 API 키
ENV_API_KEY = os.getenv("ENV_API_KEY")            # 환경 API 키 등

# 엔드포인트는 실제 문서 보고 채우기
LAND_API_BASE_URL = "https://example.land.api/endpoint"
WEATHER_API_BASE_URL = "https://example.weather.api/endpoint"
ENV_API_BASE_URL = "https://example.env.api/endpoint"

HEADERS_DEFAULT = {
    "User-Agent": "datacenter-invest-bot/0.1 (+contact: you@example.com)"
}

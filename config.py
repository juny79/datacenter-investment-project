# config.py

from pathlib import Path
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

# .env 파일 명시적 경로 지정하여 로드
env_file = BASE_DIR / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file, override=True)
else:
    print(f"Warning: .env file not found at {env_file}")

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# .env에서 불러오도록 설계 (API 키는 코드에 하드코딩 X)
LAND_API_KEY = os.getenv("LAND_API_KEY")          # 국토부 토지 API 키
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")    # 기상 API 키
ENV_API_KEY = os.getenv("ENV_API_KEY")            # 환경 API 키 등

# 엔드포인트 (국토교통부 실거래가 토지 API) 문서 기준으로 수정
# 참고: 서비스 URL은 공공데이터포털 활용 가이드에서 확인 필요
LAND_API_BASE_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
WEATHER_API_BASE_URL = "https://example.weather.api/endpoint"
ENV_API_BASE_URL = "https://example.env.api/endpoint"

HEADERS_DEFAULT = {
    "User-Agent": "datacenter-invest-bot/0.1 (+contact: you@example.com)"
}

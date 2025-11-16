# main_run_all.py

from ingestion.land_api import run_land_ingestion
from ingestion.env_api import run_env_ingestion
from ingestion.market_scraper import run_market_ingestion
from ingestion.telecom_scraper import run_telecom_ingestion

# 전력 API, 금융 API 등은 유사 패턴으로 추가한다고 가정
# from ingestion.power_api import run_power_ingestion
# from ingestion.finance_api import run_finance_ingestion


def main():
    # 예시: 수집할 지역/월/사이트 정의
    regions = ["11110", "41135"]  # 예: 서울 중구, 경기도 성남시 등
    ym_list = ["202301", "202302", "202303"]

    # 입지 후보 사이트 (site_id, lat, lon)
    sites = [
        ("SEOUL-01", 37.5665, 126.9780),
        ("GYEONGGI-01", 37.4000, 127.1000),
    ]

    print("[1] Land API 수집")
    land_df = run_land_ingestion(regions, ym_list)
    print("  land rows:", len(land_df))

    print("[2] 환경/기상 API 수집")
    env_df = run_env_ingestion(sites)
    print("  env rows:", len(env_df))

    print("[3] 시장 리포트 크롤링")
    market_df = run_market_ingestion()
    print("  market rows:", len(market_df))

    print("[4] 통신 인프라 수집")
    telecom_df = run_telecom_ingestion()
    print("  telecom rows:", len(telecom_df))

    print("모든 ingestion 단계 완료. 이제 ETL/Feature 생성 단계로 넘기면 됨.")


if __name__ == "__main__":
    main()

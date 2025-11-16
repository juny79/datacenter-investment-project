# ingestion/land_api.py

from typing import List
import pandas as pd

from config import LAND_API_KEY, LAND_API_BASE_URL
from ingestion.utils import safe_request, save_raw_csv


def fetch_land_prices(
    region_code: str,
    start_ym: str,
    end_ym: str,
) -> pd.DataFrame:
    """
    토지가격(또는 실거래가)을 기간별로 조회하여 DataFrame으로 반환.
    실제 API는 page 단위로 나뉘는 경우가 많으므로 page loop 포함.
    """
    if not LAND_API_KEY:
        raise ValueError("LAND_API_KEY 환경변수가 설정되지 않았습니다.")

    all_rows: List[dict] = []
    page = 1
    while True:
        params = {
            "serviceKey": LAND_API_KEY,
            "LAWD_CD": region_code,
            "DEAL_YMD": start_ym,  # 실제로는 월단위 루프 돌릴 수 있음
            "pageNo": page,
            "numOfRows": 1000,
            # 기타 필요한 파라미터...
        }
        resp = safe_request("GET", LAND_API_BASE_URL, params=params)
        data = resp.json()

        # 실제 응답 구조에 맞게 파싱해야 함
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        if not items:
            break

        for it in items:
            row = {
                "region_code": region_code,
                "deal_ym": start_ym,
                "land_price": it.get("거래금액"),  # 실제 키는 문서 확인 필요
                "area": it.get("대지면적"),
                "jibun": it.get("지번"),
                # 필요시 추가 필드...
            }
            all_rows.append(row)
        page += 1

    df = pd.DataFrame(all_rows)
    return df


def run_land_ingestion(regions: List[str], ym_list: List[str]) -> pd.DataFrame:
    """여러 지역·월에 대해 토지 데이터 수집"""
    frames = []
    for r in regions:
        for ym in ym_list:
            df = fetch_land_prices(region_code=r, start_ym=ym, end_ym=ym)
            frames.append(df)
    full = pd.concat(frames, ignore_index=True)
    save_raw_csv(full, "land/land_prices_raw.csv")
    return full

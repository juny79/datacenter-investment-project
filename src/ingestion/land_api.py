# ingestion/land_api.py

from typing import List, Dict, Any
import pandas as pd
import xmltodict

from config import LAND_API_KEY, LAND_API_BASE_URL
from src.ingestion.utils import safe_request, save_raw_csv


def _parse_item(item: Dict[str, Any], region_code: str, ym: str) -> Dict[str, Any]:
    """기술문서의 응답 스키마를 기반으로 단일 거래 item 파싱"""
    return {
        "region_code": region_code,
        "deal_ym": ym,

        # 문서 기반 필드 매핑
        "sggCd": item.get("sggCd"),
        "sggNm": item.get("sggNm"),
        "umdNm": item.get("umdNm"),
        "jibun": item.get("jibun"),
        "jimok": item.get("jimok"),
        "landUse": item.get("landUse"),

        "dealYear": item.get("dealYear"),
        "dealMonth": item.get("dealMonth"),
        "dealDay": item.get("dealDay"),
        "dealArea": item.get("dealArea"),
        "dealAmount": item.get("dealAmount"),

        "shareDealingType": item.get("shareDealingType"),
        "cdealType": item.get("cdealType"),
        "cdealDay": item.get("cdealDay"),
        "dealingGbn": item.get("dealingGbn"),
        "estateAgentSggNm": item.get("estateAgentSggNm"),
    }


def fetch_land_prices(region_code: str, ym: str) -> pd.DataFrame:
    """
    기술문서 기반으로 토지 매매 실거래가 조회 API 호출.
    XML 응답 처리 + totalCount 기반 페이징.
    """
    if not LAND_API_KEY:
        raise ValueError("LAND_API_KEY 환경변수가 설정되지 않았습니다.")

    all_rows = []
    page = 1
    num_rows = 100  # 한 페이지 조회 수

    while True:
        params = {
            "serviceKey": LAND_API_KEY,
            "LAWD_CD": region_code,
            "DEAL_YMD": ym,
            "pageNo": page,
            "numOfRows": num_rows,
        }

        resp = safe_request("GET", LAND_API_BASE_URL, params=params)
        data = xmltodict.parse(resp.text)

        header = data.get("response", {}).get("header", {})
        result_code = header.get("resultCode")

        # ① 에러 처리
        if result_code != "000":
            # No Data 이면 종료
            if result_code == "03":
               break
            raise RuntimeError(f"[API ERROR] code={result_code}, msg={header.get('resultMsg')}")

        body = data.get("response", {}).get("body", {})
        total_count = int(body.get("totalCount", 0))
        num_rows = int(body.get("numOfRows", num_rows))

        items = body.get("items", {}).get("item", [])
        if not items:
            break

        # items가 dict 또는 list 인 경우 모두 처리
        if isinstance(items, dict):
            items = [items]

        for it in items:
            parsed = _parse_item(it, region_code, ym)
            all_rows.append(parsed)

        # totalCount 기반 페이지 증가
        if page * num_rows >= total_count:
            break

        page += 1

    return pd.DataFrame(all_rows)


def run_land_ingestion(regions: List[str], ym_list: List[str]) -> pd.DataFrame:
    """여러 지역 × 월 토지 실거래 데이터 수집"""
    frames = []

    for r in regions:
        for ym in ym_list:
            df = fetch_land_prices(r, ym)
            frames.append(df)

    full = pd.concat(frames, ignore_index=True)

    save_raw_csv(full, "land/land_prices_raw.csv")
    return full

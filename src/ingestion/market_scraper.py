# ingestion/market_scraper.py

from typing import List
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup

from ingestion.utils import save_raw_csv
from config import HEADERS_DEFAULT


def scrape_market_table(url: str) -> pd.DataFrame:
    """
    특정 리포트 페이지(URL)에서 HTML 테이블을 추출해 DataFrame으로 변환하는 예시.
    실제 사이트 구조에 맞춰 table 선택 로직 수정 필요.
    """
    resp = requests.get(url, headers=HEADERS_DEFAULT, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        raise ValueError(f"No table found at {url}")

    # 가장 첫 번째 테이블을 예시로 파싱
    df = pd.read_html(str(tables[0]))[0]

    # 컬럼명 및 값 정제 예시
    df.columns = [re.sub(r"\s+", "_", c.strip().lower()) for c in df.columns]
    return df


def run_market_ingestion() -> pd.DataFrame:
    urls = [
        # 여기에 직접 추출 대상 리포트 URL들 리스트업
        "https://example.com/datacenter-market-report-2024-q1",
        # ...
    ]
    frames = []
    for url in urls:
        df = scrape_market_table(url)
        df["source_url"] = url
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)
    save_raw_csv(full, "market/datacenter_market_raw.csv")
    return full

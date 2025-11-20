# ingestion/utils.py
"""
데이터 수집을 위한 공통 유틸리티 함수들
"""

import time
import json
from pathlib import Path
from typing import Dict, Any, Optional

import requests
import pandas as pd
from tqdm import tqdm

from config import RAW_DIR, HEADERS_DEFAULT


class HttpError(Exception):
    """HTTP 요청 중 발생하는 에러"""
    pass


def safe_request(method: str, url: str, params=None, retries=3, timeout=10):
    """
    API 요청을 안전하게 처리하는 공통 유틸.
    - timeout
    - retry
    - status_code 체크
    
    Args:
        method: HTTP 메서드 (GET, POST 등)
        url: 요청 URL
        params: 쿼리 파라미터
        retries: 재시도 횟수
        timeout: 타임아웃 (초)
        
    Returns:
        requests.Response 객체
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            print(f"[safe_request] Attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(2)  # 재시도 전 2초 대기


def save_raw_json(data: Any, filename: str) -> Path:
    """
    JSON 데이터를 data/raw/ 디렉토리에 저장
    
    Args:
        data: 저장할 데이터
        filename: 저장할 파일명 (상대 경로)
        
    Returns:
        저장된 파일의 Path 객체
    """
    path = RAW_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def save_raw_csv(df: pd.DataFrame, relative_path: str) -> None:
    """
    DataFrame을 CSV로 data/raw/ 디렉토리에 저장
    
    Args:
        df: 저장할 DataFrame
        relative_path: 저장 경로 (예: "land/land_prices_raw.csv")
    """
    out_path = Path("data/raw") / relative_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[saved] {out_path} ({len(df)} rows)")

# ingestion/telecom_scraper.py

from typing import List, Dict, Any
import pandas as pd
import requests

from ingestion.utils import save_raw_csv
from config import HEADERS_DEFAULT


def fetch_fiber_pop_from_api(bbox: List[float]) -> pd.DataFrame:
    """
    예시: bbox = [min_lon, min_lat, max_lon, max_lat]
    실제 OpenInfraMap/유사 서비스의 API 스펙에 맞게 수정 필요.
    """
    url = "https://example-openinframap/api/fiber_pops"
    params = {
        "bbox": ",".join(map(str, bbox)),
        "format": "json",
    }
    resp = requests.get(url, headers=HEADERS_DEFAULT, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    rows: List[Dict[str, Any]] = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates")
        if not coords:
            continue

        rows.append({
            "name": props.get("name"),
            "operator": props.get("operator"),
            "type": props.get("type"),
            "lon": coords[0],
            "lat": coords[1],
        })

    return pd.DataFrame(rows)


def run_telecom_ingestion() -> pd.DataFrame:
    # 예시 bbox 여러 개 정의 (수도권, 영남권 등)
    bboxes = [
        [126.7, 37.3, 127.4, 37.8],  # 서울 근처 가상값
        # ...
    ]

    frames = []
    for bbox in bboxes:
        df = fetch_fiber_pop_from_api(bbox)
        df["bbox"] = str(bbox)
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)
    save_raw_csv(full, "telecom/fiber_pops_raw.csv")
    return full

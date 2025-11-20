# run_land_ingest.py
"""
토지 실거래가 데이터 수집 스크립트
- 여러 지역코드와 년월 조합으로 데이터 수집
- 에러 처리 및 진행 상황 로깅
- 수집 결과 통계 출력
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from src.ingestion.land_api import run_land_ingestion
from config import RAW_DIR

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('land_ingestion.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def generate_ym_list(start_ym: str, end_ym: str) -> list:
    """
    시작년월부터 종료년월까지 YYYYMM 리스트 생성
    예: generate_ym_list("202401", "202403") -> ["202401", "202402", "202403"]
    """
    from dateutil.rrule import rrule, MONTHLY
    from dateutil.parser import parse
    
    start = parse(start_ym + "01")
    end = parse(end_ym + "01")
    
    return [dt.strftime("%Y%m") for dt in rrule(MONTHLY, dtstart=start, until=end)]


def main():
    """메인 실행 함수"""
    logger.info("=" * 60)
    logger.info("토지 실거래가 데이터 수집 시작")
    logger.info("=" * 60)
    
    # ========================================
    # 수집 설정 (필요에 따라 수정)
    # ========================================
    
    # 지역코드 리스트 (법정동코드 5자리)
    # 11110: 서울 종로구
    # 11140: 서울 중구
    # 11680: 서울 강남구
    regions = [
        "11110",  # 서울 종로구
    ]
    
    # 수집 기간 설정
    # 옵션 1: 직접 리스트 지정
    ym_list = ["202407"]
    
    # 옵션 2: 기간으로 자동 생성 (주석 해제하여 사용)
    # ym_list = generate_ym_list("202401", "202407")
    
    logger.info(f"수집 대상 지역: {len(regions)}개 - {regions}")
    logger.info(f"수집 대상 기간: {len(ym_list)}개월 - {ym_list[0]} ~ {ym_list[-1]}")
    logger.info(f"총 API 호출 예상: {len(regions) * len(ym_list)}회")
    
    # ========================================
    # 데이터 수집 실행
    # ========================================
    try:
        start_time = datetime.now()
        
        df = run_land_ingestion(regions, ym_list)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # ========================================
        # 결과 출력
        # ========================================
        logger.info("=" * 60)
        logger.info("수집 완료!")
        logger.info(f"소요 시간: {elapsed:.2f}초")
        logger.info(f"수집된 레코드 수: {len(df):,}건")
        
        if len(df) > 0:
            logger.info(f"데이터 shape: {df.shape}")
            logger.info(f"컬럼 목록: {list(df.columns)}")
            logger.info(f"\n데이터 샘플 (처음 5건):\n{df.head()}")
            
            # 저장 경로 확인
            output_path = RAW_DIR / "land" / "land_prices_raw.csv"
            if output_path.exists():
                file_size = output_path.stat().st_size / 1024  # KB
                logger.info(f"저장 파일: {output_path}")
                logger.info(f"파일 크기: {file_size:.2f} KB")
            
            # 기본 통계
            logger.info(f"\n지역별 건수:\n{df['region_code'].value_counts()}")
            if 'dealAmount' in df.columns:
                logger.info(f"\n거래금액 통계:\n{df['dealAmount'].describe()}")
        else:
            logger.warning("⚠️  수집된 데이터가 없습니다. API 응답 또는 필터 조건을 확인하세요.")
        
        logger.info("=" * 60)
        
        return df
        
    except ValueError as e:
        logger.error(f"❌ 설정 오류: {e}")
        logger.error("LAND_API_KEY 환경변수가 .env 파일에 설정되어 있는지 확인하세요.")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"❌ 데이터 수집 중 오류 발생: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

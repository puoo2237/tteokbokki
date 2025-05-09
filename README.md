# 입맛 취향저격 서울시 떡볶이 맛집 추천

## 1. 수집 범위
- 기간: ~ 2025.04.07
- 대상: 동대문구 (추후 확대할 예정)
  
## 2. 수집 데이터
- 네이버 가게 정보 및 리뷰

## 3. 파일 설명
- .py
  - connect_db.py: DB의 자료 조회 및 적재
  - crawling_store_info.py: 가게 정보 및 메뉴 정보 크롤링 코드
  - crawling_store_keyword_score_review.py: 네이버 리뷰 중 키워드, 별점 리뷰에 대해서 수집
  - crawling_store_review.py: 네이버 리뷰 중 리뷰에 대해서 수집
  - run_review_driver.py: 구글 셀레니움 드라이버가 가게 정보, 메뉴 정보, 네이버 리뷰를 수집하기 위해 동적으로 수행되는 작업
  - save_file: 리뷰 수집 중 이미지 파일 저장
- .json
  - regions.json: 행정구역 정보
  - db_info.json: DB 정보 (보안상의 문제로 제외)
- .ipynb
  - data_preprocessing.ipynb: 데이터 전처리

## 4. 전처리 과정
- `회원`이라는 닉네임을 가진 사람이 여러 명 존재 => 중복 존재
  - 떡볶이와 관련없는 리뷰로 제거
- 평점만 존재하는 리뷰는 방문자 정보만을 사용하기로 함

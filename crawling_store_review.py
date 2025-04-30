import pandas as pd

from tqdm import tqdm 
import json

from connect_db import ConnectPostgres
from run_review_driver import RunDriver

with open('regions.json', 'r', encoding='utf-8') as f:
    regions = json.load(f)

try:
    for region in regions['region']:
        cp = ConnectPostgres()
        rd = RunDriver()
        
        search_keyword = f"{region} 떡볶이"
        rd.go_iframe1(search_keyword)
        
        # 페이지 수만큼 돌리기
        impossible_more_page_ = 'false'
        while impossible_more_page_ == 'false':
            if 'more_page_' in locals():
                impossible_more_page_ = more_page_.get_attribute('aria-disabled')

            stores_ = rd.scoll_store() # 한 페이지에 출력되는 가게 스크롤해서 모두 보기 
        
            for i in tqdm(range(1, stores_+1)):
                rd.click_store(i)

                store_id = rd.get_store_id() # 가게 id 가져오기

                skip_lists = cp.select_store_id(1)['store_id'].tolist()
                if store_id in skip_lists:                      
                    print(f'skip {store_id}')
                    rd.move_to_iframe1()
                    continue 
                
                rd.go_iframe2() # 두 번째 iframe으로 이동
                rd.click_review_tab() # 리뷰 탭으로 이동

                review_df = [] # 수집할 테이블 초기화
                tot_review_cnt = rd.count_reviews()
                print(f"{region}, {store_id}의 총 리뷰 수: {tot_review_cnt}")

                # 리뷰
                for j in tqdm(range(1, tot_review_cnt+1)):
                    users_ = rd.scroll_review(j)
                    user_review = rd.get_review(store_id)
                    
                    review_df.append(user_review) # 데이터 저장
                    rd.click_review_more(j, tot_review_cnt) # 더보기 클릭

                
                # 식당하나 완료할 때마다 DB에 저장
                cp.save_store_review(pd.DataFrame(review_df, columns = [
                                                                'store_id',
                                                                'user_nickname',
                                                                'user_review_cnt',
                                                                'user_photo_cnt',
                                                                'user_follower_cnt',
                                                                'user_photo_path',
                                                                'user_reservation_location',
                                                                # 'user_score',
                                                                'user_feature',
                                                                'user_review',
                                                                'user_hashtag',
                                                                'user_visit_date',
                                                                'user_visit_cnt',
                                                                'user_verification']))
                rd.move_to_iframe1()
            more_page_ = rd.click_page_more()
        rd.quit_driver() # region마다 종료하기
except Exception as e:
    print(f"오류 발생: {e}")
import pandas as pd

from tqdm import tqdm 
import time
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
        
        time.sleep(2)
        # 페이지 수만큼 돌리기
        impossible_more_page_ = 'false'
        while impossible_more_page_ == 'false':
            if 'more_page_' in locals():
                impossible_more_page_ = more_page_.get_attribute('aria-disabled')

            stores_ = rd.scoll_store() # 한 페이지에 출력되는 가게 스크롤해서 모두 보기 
            
            time.sleep(2)

            for i in tqdm(range(1, stores_+1)):
                rd.click_store(i)
                
                store_id = rd.get_store_id()
                
                skip_lists = cp.select_store_id(1)['store_id'].tolist()
                if store_id in skip_lists:                      
                    print(f'skip {store_id}')
                    rd.move_to_iframe1()
                    continue

                rd.go_iframe2() # 두 번째 iframe으로 이동

                store_result = pd.DataFrame([rd.get_store_info(store_id, region)], columns=[
                                                                        'store_id',
                                                                        'store_name',
                                                                        'gu',
                                                                        'store_url',
                                                                        'store_location',
                                                                        'store_business_hour',
                                                                        'store_visit_review_cnt',
                                                                        'store_blog_review_cnt',
                                                                        # 'crawling_datetime'
                                                                        ]
                            )
                
                # DB 가게정보 추가
                cp.save_store_info(store_result)

                # 메뉴 수집
                rd.click_menu_tab()
                rd.click_sold_out()
                sub_tab = rd.check_menu_sub_tab()
                if sub_tab:
                    menu_lists = []

                    # 서브탭 구조 (포장/매장/배달 각각 존재 여부 확인)
                    for nth, tab in enumerate(sub_tab):
                        rd.click_menu_sub_tab(tab, nth)
                        menu_lists.append(rd.get_menu_info(store_id, tab))
                    menu_df = pd.concat([pd.DataFrame(menu for menu in menu_lists)])
                else:
                    menus = rd.get_menu_info(store_id, tab = 'all')
                    menu_df = pd.DataFrame(menus)
                
                cp.save_store_menu(menu_df.drop_duplicates(['store_id', 'is_tab', 'take_out', 'menu_name'], keep='last'))
                rd.move_to_iframe1()
                
            more_page_ = rd.click_page_more()
        rd.quit_driver() # region마다 종료하기

except Exception as e:
    print(f"오류 발생: {e}")
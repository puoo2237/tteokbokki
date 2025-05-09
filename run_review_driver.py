from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import numpy as np
import pandas as pd
import re
import time

from datetime import datetime

from save_file import mk_folder, download_img


set_time = 20

class RunDriver:

    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    actions = ActionChains(driver)
    wait = WebDriverWait(driver, set_time)  

    def go_iframe1(self, search_keyword:str):
        """
        첫 번째 프레임으로 이동
            search_keyword: 행정구역 떡볶이
        """
        self.driver.get(f"https://map.naver.com/p/search/{search_keyword}")
        self.iframe1 = self.wait.until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="searchIframe"]'))
        )
        self.driver.switch_to.frame(self.iframe1) 
     
    def scoll_store(self)->int:
        """
        한 페이지에 있는 가게 수 측정
        """
        stores_ = -1 # 가게 수 초기화
        scolling_ = []
        # 스크롤링 끝까지하기
        while stores_ < len(scolling_):
            stores_ = len(scolling_)
            scolling_ = self.wait.until(
                            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="_pcmap_list_scroll_container"]/ul/li'))
                        )
            self.driver.execute_script("arguments[0].scrollIntoView(true);", scolling_[-1])
        return stores_
    
    def click_page_more(self):
        """
        페이지 넘기기
        """
        more_page_ = self.driver.find_element(By.XPATH, '//*[@id="app-root"]/div/div[2]/div[2]/a[7]')
        more_page_.click() # 페이지 이동  
        return more_page_

    def click_store(self, i:int):
        """
        가게 상세보기로 이동
        """
        store_detail_ = self.wait.until(
                            EC.presence_of_element_located((By.XPATH, f'//*[@id="_pcmap_list_scroll_container"]/ul/li[{i}]/div[1]/a[1]'))
                            )
        store_detail_.click() 

    def get_store_id(self)->str:
        """
        가게의 store_id 가져오기
        """
        current_url = self.driver.current_url
        store_id = current_url.split('?')[0].split('/')[-1] 

        return store_id
    
    def move_to_iframe1(self):
        """
        iframe1으로 재이동
        """
        self.driver.switch_to.parent_frame()
        self.driver.switch_to.frame(self.iframe1)

    def go_iframe2(self):
        """
        두 번째 iframe으로 이동
        """
        self.driver.switch_to.parent_frame()
        self.iframe2 = self.wait.until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="entryIframe"]'))
                            )
        self.driver.switch_to.frame(self.iframe2) 

    def click_review_tab(self):
        """
        리뷰 탭으로 이동
        """
        tab_ = self.wait.until(
                            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="app-root"]/div/div/div/div[4]/div/div/div/div/a'))
                            )
        n = np.where([t.get_attribute('href').split('?')[0].split('/')[-1] == 'review' for t in tab_])[0][0]
        move_review_ = self.wait.until(
                                EC.presence_of_element_located((By.XPATH, f'//*[@id="app-root"]/div/div/div/div[4]/div/div/div/div/a[{n+1}]'))
                                )
        move_review_.click()

    def count_reviews(self):
        """
        리뷰 수 가져오기
        """
        return int(self.wait.until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section.k1QQ5 > h2 > div.place_section_header_title > em'))\
        ).text.replace(',', ''))
                    
    def count_score_reviews(self):
        """
        키워드, 별점 리뷰 수 가져오기
        """
        return int(self.wait.until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section.buywf > h2 > div.place_section_header_title > em'))\
        ).text.replace(',', ''))
    
    def open_review_division(self):
        try:
            keyword_review_more_ = self.wait.until(
                                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.place_section.buywf > h2 > div.place_section_header_extra > a'))
                                                )
            if keyword_review_more_.text == '펼쳐보기':
                keyword_review_more_.click()
        except Exception as e:
            pass        

    def scroll_review(self, j:int)->list:
        """
        리뷰 스크롤하기
        """
        find_user_ = self.wait.until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, f'div.place_section.k1QQ5'))
                            )
        self.users_ = find_user_.find_elements(By.CSS_SELECTOR, 'div > ul > li') # 사이트에서 노출된 유저 수 파악
        self.user_ = self.users_[j-1]
        self.actions.move_to_element(self.user_).perform() # 스크롤 이동
        return self.users_

    def scroll_score_review(self, k:int)->list:
        """
        키워드, 별점 리뷰 스크롤하기
        """
        find_user_ = self.wait.until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, f'div.place_section.buywf'))
                            )
        self.users_ = find_user_.find_elements(By.CSS_SELECTOR, 'div > ul > li') # 사이트에서 노출된 유저 수 파악
        self.user_ = self.users_[k-1]
        self.actions.move_to_element(self.user_).perform() # 스크롤 이동
        return self.users_        

    def get_user_nickname(self):
        """
        이용자 닉네임 가져오기
        """
        return self.user_.find_element(By.CLASS_NAME, 'pui__NMi-Dp').text # 닉네임(text)

    def get_user_info(self):
        """
        이용자의 기록 가져오기
        """
        user_review_cnt, user_photo_cnt, user_follower_cnt = 0, 0, 0
        for user_info in self.user_.find_elements(By.CLASS_NAME, 'pui__WN-kAf'): # 리뷰, 사진, 팔로우
            user_info_text = user_info.text
            if user_info_text.split()[0] == '리뷰':
                user_review_cnt = int(user_info_text.split()[-1].replace(',', '')) # 남긴 리뷰 수(int)
            elif user_info_text.split()[0] == '사진':
                user_photo_cnt = int(user_info_text.split()[-1].replace(',', '')) # 첨부한 사진 수(int)
            elif user_info_text.split()[0] == '팔로워':
                user_follower_cnt = int(user_info_text.split()[-1].replace(',', '')) # 팔로워 수(int)
        return user_review_cnt, user_photo_cnt, user_follower_cnt

    def get_user_photo_path(self, store_id:str, user_nickname:str)->list:
        """
        이용자가 올린 이미지 경로 가져오기
        """
        user_photo_path = []
        for idx, href in enumerate(self.user_.find_elements(By.CLASS_NAME, 'place_thumb')):
            img_link_ = href.find_element(By.TAG_NAME, 'img').get_attribute('src').split('?')[0]
            folder_path_ = f'./img/{store_id}'
            rename_nickname_ = user_nickname.replace('*', '$') # 파일 저장 목적으로 변경
            save_path_ = f'{folder_path_}/{rename_nickname_}_{idx}.jpg'
            mk_folder(folder_path_)
            download_img(img_link_, save_path_)
            user_photo_path.append(save_path_) # 사진 경로(list)
        return user_photo_path
    
    def get_user_reservation_location(self)->str:
        """
        이용자가 예약한 방 정보 가져오기
        """
        try:
            return self.user_.find_element(By.CLASS_NAME, 'pui__ETqMYH').text # 예약 장소(text)
        except Exception as e:
            return None
        
    def get_user_score(self)->float:
        """
        이용자가 부여한 점수 가져오기
        """
        try:
           return float(self.user_.find_elements(By.CLASS_NAME, 'pui__6abRMf')[1].text) # 평점(float)
        except Exception as e:
            return np.nan
        
    def get_user_feature(self)->list:
        """
        이용자의 방문 특성 가져오기
        """

        try:
            return [ft.text for ft in self.user_.find_elements(By.CLASS_NAME, 'pui__V8F9nN')] # 방문자 특성(list)
        except Exception as e:
            return []
        
    def get_user_review(self):
        """
        이용자가 남긴 리뷰 가져오기
        """
        try:
            return self.user_.find_element(By.CLASS_NAME, 'pui__vn15t2')\
                            .text.replace('\n더보기', '')\
                            .replace('\x00', '') # 리뷰(text)
        except Exception as e:
            return None
    def get_user_hashtag(self):
        """
        이용자가 남긴 해시태그 가져오기
        """
        try:
            WebDriverWait(self.user_, set_time).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.pui__HLNvmI > a'))
            ).click() # 해시태그 더보기
        except Exception as e:
            pass
        return [tag.text for tag in self.user_.find_elements(By.CLASS_NAME, 'pui__jhpEyP')] # 해시태그(list)

    def get_visit_info(self):
        """
        이용자의 방문 정보 가져오기
        """
        visit_info_ = [visit.text for visit in self.user_.find_elements(By.CLASS_NAME, 'pui__gfuUIT')]
        user_visit_date = pd.Timestamp(re.sub(r'[^\d\s]', '', visit_info_[0].split('\n')[-1]).strip().replace(' ', '/')).date() # 방문 일시(timestamp)
        user_visit_cnt = int(visit_info_[1].replace('번째 방문', '').replace(',', '')) # 방문 횟수(int)
        user_verification = visit_info_[2].split('\n')[-1] # 인증 수단(text)

        return user_visit_date, user_visit_cnt, user_verification

    def get_review(self, store_id)->list:
        user_nickname = self.get_user_nickname()
        user_review_cnt, user_photo_cnt, user_follower_cnt = self.get_user_info()
        user_photo_path = self.get_user_photo_path(store_id, user_nickname)
        if len(user_photo_path) == 0:
            user_photo_path = None
        elif len(user_photo_path) == 1 and user_photo_path == '':
            user_photo_path = None
        user_reservation_location = self.get_user_reservation_location()
        # user_score = self.get_user_score()
        user_feature = self.get_user_feature()
        user_review = self.get_user_review()
        user_hashtag = self.get_user_hashtag()
        user_visit_date, user_visit_cnt, user_verification = self.get_visit_info()
        crawling_datetime = datetime.now()
        
        return [
                store_id,
                user_nickname,
                user_review_cnt,
                user_photo_cnt,
                user_follower_cnt,
                user_photo_path,
                user_reservation_location,
                # user_score,
                user_feature,
                user_review,
                user_hashtag,
                user_visit_date,
                user_visit_cnt,
                user_verification,
                crawling_datetime]
    
    def click_review_more(self, j:int, tot_review_cnt: int):
        """
        더보기 클릭하기
        """
        if len(self.users_) == j and j < tot_review_cnt:
            more_click_ = self.wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "fvwqf"))
            )
            self.driver.execute_script("arguments[0].scrollIntoView(true);", more_click_)
            self.driver.execute_script("arguments[0].click();", more_click_)
            time.sleep(2)

    def click_score_review_more(self, j:int, tot_review_cnt: int):
        """
        키워드, 별점의 더보기 클릭하기
        """
        if len(self.users_) == j and j < tot_review_cnt:
            more_click_ = self.wait.until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "fvwqf"))
            )[-1]
            self.driver.execute_script("arguments[0].scrollIntoView(true);", more_click_)
            self.driver.execute_script("arguments[0].click();", more_click_)
            time.sleep(2)

    def quit_driver(self):
        """
        driver 종료하기
        """
        self.driver.quit()

    def get_store_url(self)->str:
        return self.driver.current_url
    
    def get_store_name(self)->str:
        return self.wait.until(    
                EC.presence_of_element_located((By.CSS_SELECTOR, 'span.GHAhO')),
            ).text
    
    def get_store_location(self)->str:
        return self.driver.find_element(By.CSS_SELECTOR, 'span.LDgIH').text
    
    def get_store_category(self)->str:
        return self.driver.find_element(By.CSS_SELECTOR, 'span.lnJFt').text
    
    def get_store_business_hour(self)->str:
        try:
            self.driver.execute_script("arguments[0].click();", self.driver.find_element(By.CSS_SELECTOR, 'div.y6tNq'))
            business_hour = [h.text for h in self.driver.find_elements(By.CSS_SELECTOR, 'div.y6tNq')]
            return business_hour
        except NoSuchElementException:
            print('영업시간 데이터 없음')
    
    def _count_review_by_keyword(self, keyword: str)->int:
        """
        방문자 or 블로그 리뷰 수 추출
        """
        for rc in self.driver.find_elements(By.CSS_SELECTOR, 'span.PXMot'):
            text = rc.text.strip()
            if keyword in text:
                try:
                    return int(text.split()[-1].replace(',', ''))
                except ValueError:
                    pass
        return 0
    
    def count_visit_review(self)->int:
        return self._count_review_by_keyword('방문자')
    
    def count_blog_review(self)->int:
        return self._count_review_by_keyword('블로그')
    
    def get_store_info(self, store_id, region)->list:
        
        store_name = self.get_store_name()
        store_location = self.get_store_location()
        store_business_hour = self.get_store_business_hour()
        store_visit_review_cnt = self.count_visit_review()
        store_blog_review_cnt = self.count_blog_review()
        store_url = self.get_store_url()
        crawling_datetime = datetime.now()

        return [
            store_id,
            store_name,
            region,
            store_url,
            store_location,
            store_business_hour,
            store_visit_review_cnt,
            store_blog_review_cnt,
            # crawling_datetime
        ]
    
    def click_menu_tab(self):
        """
        메뉴 탭 클릭
        """
        # 메뉴 탭 찾기
        menu_tab = self.driver.find_element(By.XPATH, '//a[contains(@href, "menu") or contains(text(), "메뉴")]')
        # 메뉴 탭 클릭
        self.driver.execute_script("arguments[0].click();", menu_tab)
        
    def click_sold_out(self):
        # 품절상품 제외 버튼 클릭
        try:
            sold_out_checkbox = self.driver.find_element(By.ID, 'sold_out')
            if sold_out_checkbox.is_selected():
                label = self.driver.find_element(By.CSS_SELECTOR, 'label[for="sold_out"]')
                self.driver.execute_script("arguments[0].click();", label)
                time.sleep(0.5)
        except NoSuchElementException:
            print('품절 상품 버튼 없음')
    
    def check_menu_sub_tab(self)->int:
        """
        포장/매장/배달 탭 존재하는지 확인
        """
        sub_tab_text = [sub_tab.text for sub_tab in self.driver.find_elements(By.CSS_SELECTOR, 'a.YsfhA')]
        return sub_tab_text
 
    def click_menu_sub_tab(self, sub_tab: str, nth: int = 0):
        """
        포장/매장/배달 탭 클릭
        """
        self.click_sold_out()
        if nth == 0:
            tab_element = self.driver.find_element(By.XPATH, f'//a[text()="{sub_tab}"]')
        else:
            tab_element = self.driver.find_element(By.XPATH, f'//a[contains(@class, "tab_switch") and contains(., "{sub_tab}")]')
        self.driver.execute_script("arguments[0].click();", tab_element)
        time.sleep(1)

    def _get_menu_selectors(self, tab: str) -> dict:
        """
        기본/포장or매장or배달 별 css selector 정의
        """
        if tab in ['포장', '매장']:
            return {
                'sub_tab' : 'take_out' if tab == "포장" else 'store' if tab == '매장' else 'delivery',
                'info_detail': 'a.info_link',
                'title': 'div.tit',
                'desc': 'div.detail',
                'price': 'div.price',
                'img': 'img.img'
            } 
        # elif tab == '배달' :
        #     return {
        #         'sub_tab' : 'delivery',
        #         'info_detail': 'a.info_link',
        #         'title': 'div.tit',
        #         'desc': 'div.detail',
        #         'price': 'div.price',
        #         'img': 'img.img'
        #     } 
        else:
            return {
                'sub_tab' : 'delivery' if tab == '배달' else 'all',
                'info_detail': 'li.E2jtL',
                'title': 'span.lPzHi',
                'desc': 'div.kPogF',
                'price': 'div.GXS1X',
                'img': 'img.K0PDV'
            }
        
    def get_menu_info(self, store_id: str, tab: str)->list:
        """
        메뉴정보 추출
        """
        check_sub_tab_ = 1 if tab in ['포장', '매장', '배달'] else 0
        selectors = self._get_menu_selectors(tab)
        menus_ = self.driver.find_elements(By.CSS_SELECTOR, selectors['info_detail'])
        menu_results = []
        for menu_idx, menu in enumerate(menus_):
            try:
                menu_name = menu.find_element(By.CSS_SELECTOR, selectors['title']).text.strip()
            except:
                menu_name = None

            try:
                desc = menu.find_element(By.CSS_SELECTOR, selectors['desc']).text.strip()
            except:
                desc = None

            try:
                price = menu.find_element(By.CSS_SELECTOR, selectors['price']).text.strip()
            except:
                price = None

            try:
                img_url_ = menu.find_element(By.CSS_SELECTOR, selectors['img']).get_attribute('src')
                folder_path_ = f'./menu_img/{store_id}_{check_sub_tab_}_{selectors['sub_tab']}'
                save_path_ = f'{folder_path_}/{menu_idx}.png'
                mk_folder(folder_path_)
                download_img(img_url_, save_path_)
            except:
                img_url_ = None
                save_path_ = None

            menu_results.append({
                        'store_id': store_id,
                        'is_tab' : check_sub_tab_,
                        'take_out' : tab,
                        # 'menu_idx' : menu_idx,
                        'menu_name' : menu_name,
                        'menu_description': desc,
                        'menu_price' : price,
                        'img_path' : save_path_
                    })
            
        return menu_results



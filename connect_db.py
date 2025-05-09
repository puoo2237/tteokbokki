from sqlalchemy import create_engine, TEXT, ARRAY, text
import pandas as pd
import json
from typing import Literal

class ConnectPostgres:
    # 데이터베이스 연결 정보 설정

    with open('db_info.json', 'r', encoding='utf-8') as f:
        db_ = json.load(f)
    review_table_name = 'store_review'
    info_table_name = 'store'
    menu_table_name = 'store_menu'

    def load_engine(self, DB_USER:str, 
                  DB_PASSWORD:str, 
                  DB_HOST:str, 
                  DB_PORT:int, 
                  DB_NAME:str, 
                  DB_SCHEMA:str):
        engine_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-c%20search_path%3D{DB_SCHEMA}'
        engine = create_engine(engine_url)
        return engine

    def create_review_table(self):
        create_sql = f"""
                        CREATE TABLE {self.db_['DB_SCHEMA']}.{self.review_table_name} (
                            store_id int4 NOT NULL,
                            user_nickname varchar(50) NOT NULL,
                            user_review_cnt int4 NOT NULL,
                            user_photo_cnt int4 NOT NULL,
                            user_follower_cnt int4 NOT NULL,
                            user_photo_path _text NULL,
                            user_reservation_location varchar(50) NULL,
                            user_feature _text NULL,
                            user_review text NULL,
                            user_hashtag _text NULL,
                            user_visit_date timestamp NOT NULL,
                            user_visit_cnt int4 NOT NULL,
                            user_verification varchar(10) NULL,
                            crawling_datetime timestamp NOT NULL,
                            CONSTRAINT store_review_pk PRIMARY KEY (store_id, user_nickname, user_visit_date, user_visit_cnt),
                            CONSTRAINT store_review_id_fk FOREIGN KEY (store_id) REFERENCES {self.db_['DB_SCHEMA']}.store(store_id) ON DELETE CASCADE
                        );
                    """

        try:
            engine = self.load_engine(**self.db_)
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    connection.execute(text(create_sql))
                    connection.commit()
                    print(f"{self.review_table_name}, 테이블 생성")

        except Exception as e:
            print(f"오류 발생: {e}")


    def select_store_id(self, option: Literal[1, 2]=1)->pd.DataFrame:
        """
        수집한 store list 가져오기
            options=1: review
            options=2: score review
        """
        sql = f"""
                    select distinct store_id
                    from {self.review_table_name}
                    """
        if option == 1:
            sql += "where user_review is not null;"
        else:
            sql += "where user_review is null;"

        engine = self.load_engine(**self.db_)
        try:
            with engine.connect() as connection:
                return pd.read_sql(text(sql), connection)
        except Exception as e:
            print(f"Error: {e}")
            self.create_review_table()
        finally:
            engine.dispose()  

    def save_store_review(self, df:pd.DataFrame):
        """
        postgres에 store_review 저장하기
        """
        engine = self.load_engine(**self.db_)
        try:
            df.to_sql(self.review_table_name, 
                    engine, 
                    if_exists='append', 
                    index=False,
                    dtype={
                        "user_photo_path": ARRAY(TEXT()),
                        "user_feature": ARRAY(TEXT()),
                        "user_hashtag": ARRAY(TEXT()),
                        })
            print(f"Pandas DataFrame이 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"데이터베이스 저장 오류: {e}")
        finally:
            engine.dispose()
    
    def create_store_table(self):
        create_sql = f"""
                        CREATE TABLE {self.db_['DB_SCHEMA']}.{self.info_table_name} (
                            store_id int4 NOT NULL,
                            store_name varchar(50) NOT NULL,
                            gu varchar(10) NOT NULL,
                            store_category varchar(10) NULL,
                            store_score float4 NULL,
                            store_business_hour _text NULL,
                            store_location varchar(150) NOT NULL,
                            store_visit_review_cnt float4 NOT NULL,
                            store_blog_review_cnt float4 NOT NULL,
                            store_url text NOT NULL,
                            CONSTRAINT store_id_pk PRIMARY KEY (store_id)
                        );
                    """

        try:
            engine = self.load_engine(**self.db_)
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    connection.execute(text(create_sql))
                    connection.commit()
                    print(f"{self.info_table_name}, 테이블 생성")

        except Exception as e:
            print(f"오류 발생: {e}")
    
    def create_store_menu_table(self):
        create_sql = f"""
                        CREATE TABLE {self.db_['DB_SCHEMA']}.{self.info_table_name} (
                            store_id int4 NOT NULL,
                            is_tab int2 NOT NULL,
                            take_out varchar(10) NOT NULL,
                            menu_name varchar(50) NOT NULL,
                            menu_description text NULL,
                            menu_price varchar(20) NULL,
                            img_path text NULL,
                            CONSTRAINT store_menu_pk PRIMARY KEY (store_id, is_tab, take_out, menu_name)
                            CONSTRAINT store_menu_fk FOREIGN KEY (store_id) REFERENCES {self.db_['DB_SCHEMA']}.store(store_id) ON DELETE CASCADE
                            );
                    """

        try:
            engine = self.load_engine(**self.db_)
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    connection.execute(text(create_sql))
                    connection.commit()
                    print(f"{self.menu_table_name}, 테이블 생성")

        except Exception as e:
            print(f"오류 발생: {e}")

    def save_store_info(self, df:pd.DataFrame):
        """
        postgres에 store_info 저장하기
        """
        engine = self.load_engine(**self.db_)
        try:
            df.to_sql(self.info_table_name, 
                    engine, 
                    if_exists='append', 
                    index=False,
                    dtype={
                        "store_business_hour": ARRAY(TEXT()),
                        })
            print(f"Pandas DataFrame이 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"데이터베이스 저장 오류: {e}")
        finally:
            engine.dispose()
    
    def save_store_menu(self, df:pd.DataFrame):
        """
        postgres에 store_menu 저장하기
        """
        engine = self.load_engine(**self.db_)
        try:
            df.to_sql(self.menu_table_name, 
                    engine, 
                    if_exists='append', 
                    index=False,
                    )
            print(f"Pandas DataFrame이 성공적으로 저장되었습니다.")
        except Exception as e:
            print(f"데이터베이스 저장 오류: {e}")
        finally:
            engine.dispose()

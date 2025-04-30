from sqlalchemy import create_engine, TEXT, ARRAY, text
import pandas as pd
import json
from typing import Literal

class ConnectPostgres:
    # 데이터베이스 연결 정보 설정

    with open('db_info.json', 'r', encoding='utf-8') as f:
        db_ = json.load(f)
    review_table_name = 'store_review'

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
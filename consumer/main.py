import sqlalchemy as sa
from HelperUtils.SQLUtils import SQLUtils
# from user.config.config import SQL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Column, Integer, Table, String, MetaData


def tupple_to_dict(sql_response_list: list, keys: list):
    # keys = ["session_id","cart_id", "item_id", "count","is_active"]
    json_list = []
    print(sql_response_list)
    for tpl in sql_response_list:
        json_dict = {}
        for i in range(len(tpl)):
            json_dict[keys[i]] = tpl[i]
        json_list.append(json_dict)
    return json_list

class SQLService:
    def __init__(self) -> None:
        self.sql_conn = "mysql+pymysql://WTrw:WoofandTrill%4012@localhost:3306/external"
        self.engine = create_engine(self.sql_conn)
        self.order = "tbl_order"
        self.checkout = "tbl_checkout"
        self.table="items"
        pass

    def update_sql_tables(self):
        curr_session = sessionmaker(bind=self.engine)
        session = curr_session()
        query=f"select count from item where items_id ='201816_03_01' and is_active= 1;"
        print(query)
        count =[]
        response=session.execute(text(query))
        for r in response:
            count.append(r)
        k=(tupple_to_dict(count,["count"]))
        l=k[0]['count'] -2
        print(l)


SQLService().update_sql_tables()
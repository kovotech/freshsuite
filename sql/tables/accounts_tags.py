import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import ast

class Accounts_Tags:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "accounts_tags",MetaData(),
            Column('accountId',BigInteger),
            Column('tag',Text)
        )
        return TABLE
    
    def create_table(self):
        table = Accounts_Tags.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def str_to_dict(self,sql_query) -> list:
        tag_list = []
        df = pd.read_sql(sql_query,con=self.engine)
        for index,row in df.iterrows():
            tags = ast.literal_eval(row['tags'])
            dict_ = {"accountId":row['id'],"tags":tags}
            tag_list.append(dict_)
        return tag_list
    
    @staticmethod
    def get_delete_stmt(record):
        table = Accounts_Tags.getTable()
        stmt = table.delete().where(table.c.accountId==record['accountId'])
        return stmt

    @staticmethod
    def get_insert_stmt(tag_record:dict,dealId:int) -> dict:
        table = Accounts_Tags.getTable()
        tag_dict = {"accountId":dealId,"tag":tag_record}
        stmt = table.insert().values(tag_dict)
        return stmt

    def import_to_sql(self,record):
        deletestmt = Accounts_Tags.get_delete_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(deletestmt)
        for tag_record in record['tags']:
            insertstmt = Accounts_Tags.get_insert_stmt(tag_record,record['accountId'])
            with self.engine.begin() as connx:
                connx.execute(insertstmt)
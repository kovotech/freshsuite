import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import json
import ast

class Contacts_Sales_Accounts:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "contacts_sales_accounts",MetaData(),
            Column('id',BigInteger),
            Column('partial',Boolean),
            Column('name',Text),
            Column('avatar',Text),
            Column('website',Text),
            Column('open_deals_amount',DECIMAL(20,0)),
            Column('open_deals_count',DECIMAL(20,0)),
            Column('won_deals_amount',DECIMAL(20,0)),
            Column('won_deals_count',Integer),
            Column('last_contacted',DateTime),
            Column('is_primary',Boolean),
            Column('contactId',BigInteger)
                    )
        return TABLE
    
    def create_table(self):
        table = Contacts_Sales_Accounts.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def str_to_dict(self,sql_query) -> list:
        dict_list = []
        df = pd.read_sql(sql_query,con=self.engine)
        for index,row in df.iterrows():
            emails = ast.literal_eval(row['sales_accounts'])
            dict_ = {"contactId":row['id'],"sales_accounts": emails}
            dict_list.append(dict_)
        
        return dict_list
    
    @staticmethod
    def get_delete_stmt(record):
        table = Contacts_Sales_Accounts.getTable()
        stmt = table.delete().where(table.c.contactId==record['contactId'])
        return stmt

    @staticmethod
    def get_insert_stmt(sales_account_record:dict,contactId:int) -> dict:
        table = Contacts_Sales_Accounts.getTable()
        sales_account_record.update({"contactId":contactId})
        stmt = table.insert().values(sales_account_record)
        return stmt

    def import_to_sql(self,record):
        deletestmt = Contacts_Sales_Accounts.get_delete_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(deletestmt)
        for sales_account_record in record['sales_accounts']:
            insertstmt = Contacts_Sales_Accounts.get_insert_stmt(sales_account_record,record['contactId'])
            with self.engine.begin() as connx:
                connx.execute(insertstmt)
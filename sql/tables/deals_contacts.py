import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import ast

class Deals_Contacts:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "deals_contacts",MetaData(),
            Column('dealId',BigInteger),
            Column('contactId',Text)
        )
        return TABLE
    
    def create_table(self):
        table = Deals_Contacts.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def str_to_dict(self,sql_query) -> list:
        contact_list = []
        df = pd.read_sql(sql_query,con=self.engine)
        for index,row in df.iterrows():
            contacts = ast.literal_eval(row['contact_ids'])
            dict_ = {"dealId":row['id'],"contact_ids":contacts}
            contact_list.append(dict_)
        return contact_list
    
    @staticmethod
    def get_delete_stmt(record):
        table = Deals_Contacts.getTable()
        stmt = table.delete().where(table.c.dealId==record['dealId'])
        return stmt

    @staticmethod
    def get_insert_stmt(contact_record:dict,dealId:int) -> dict:
        table = Deals_Contacts.getTable()
        contact_dict = {"dealId":dealId,"contactId":contact_record}
        stmt = table.insert().values(contact_dict)
        return stmt

    def import_to_sql(self,record):
        deletestmt = Deals_Contacts.get_delete_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(deletestmt)
        for contact_record in record['contact_ids']:
            insertstmt = Deals_Contacts.get_insert_stmt(contact_record,record['dealId'])
            with self.engine.begin() as connx:
                connx.execute(insertstmt)
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import ast

class Contacts_Emails:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "contacts_emails",MetaData(),
            Column('id',BigInteger),
            Column('value',Text),
            Column('is_primary',Boolean),
            Column('label',Text),
            Column('_destroy',Boolean),
            Column('contactId',BigInteger)
        )
        return TABLE
    
    def create_table(self):
        table = Contacts_Emails.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def str_to_dict(self,sql_query) -> list:
        dict_list = []
        df = pd.read_sql(sql_query,con=self.engine)
        for index,row in df.iterrows():
            emails = ast.literal_eval(row['emails'])
            dict_ = {"contactId":row['id'],"emails": emails}
            dict_list.append(dict_)
        
        return dict_list
    
    @staticmethod
    def get_delete_stmt(record):
        table = Contacts_Emails.getTable()
        stmt = table.delete().where(table.c.contactId==record['contactId'])
        return stmt

    @staticmethod
    def get_insert_stmt(email_record:str,contactId:int) -> dict:
        table = Contacts_Emails.getTable()
        email_record.update({"contactId":contactId})
        stmt = table.insert().values(email_record)
        return stmt

    def import_to_sql(self,record):
        deletestmt = Contacts_Emails.get_delete_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(deletestmt)
        for email_record in record['emails']:
            insertstmt = Contacts_Emails.get_insert_stmt(email_record,record['contactId'])
            with self.engine.begin() as connx:
                connx.execute(insertstmt)
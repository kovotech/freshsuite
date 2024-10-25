import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import ast

class Calls_Integrated_Resources:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "calls_integrated_resources",MetaData(),
            Column('id',BigInteger),
            Column('integration_name',Text),
            Column('type',Text),
            Column('callId',BigInteger)
        )
        return TABLE
    
    def create_table(self):
        table = Calls_Integrated_Resources.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def str_to_dict(self,sql_query) -> list:
        dict_list = []
        df = pd.read_sql(sql_query,con=self.engine)
        for index,row in df.iterrows():
            integrated_resources = ast.literal_eval(row['integrated_resources'])
            dict_ = {"callId":row['id'],"integrated_resources": integrated_resources}
            dict_list.append(dict_)
        
        return dict_list
    
    @staticmethod
    def get_delete_stmt(record):
        table = Calls_Integrated_Resources.getTable()
        stmt = table.delete().where(table.c.callId==record['callId'])
        return stmt

    @staticmethod
    def get_insert_stmt(resource_record:dict,callId:int) -> dict:
        table = Calls_Integrated_Resources.getTable()
        resource_record.update({"callId":callId})
        stmt = table.insert().values(resource_record)
        return stmt

    def import_to_sql(self,record):
        deletestmt = Calls_Integrated_Resources.get_delete_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(deletestmt)
        for resource_record in record['integrated_resources']:
            insertstmt = Calls_Integrated_Resources.get_insert_stmt(resource_record,record['callId'])
            with self.engine.begin() as connx:
                connx.execute(insertstmt)
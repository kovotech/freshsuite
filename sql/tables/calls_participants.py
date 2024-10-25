import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable
import ast

class Calls_Participants:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "calls_participants",MetaData(),
            Column('id',BigInteger),
            Column('call_id',BigInteger),
            Column('caller_id',BigInteger),
            Column('caller_number',Text),
            Column('caller_name',Text),
            Column('participant_id',Text),
            Column('participant_type',Text),
            Column('connection_type',Integer),
            Column('call_status',Integer),
            Column('duration',Integer),
            Column('duration_unit',Text),
            Column('cost',DECIMAL(20,3)),
            Column('cost_unit',Text),
            Column('enqueued_time',Text),
            Column('created_time',DateTime),
            Column('updated_time',DateTime),
            Column('key',String(1000))
        )
        return TABLE
    
    def create_table(self):
        table = Calls_Participants.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    def str_to_dict(self,sql_query) -> list:
        dict_list = []
        df = pd.read_sql(sql_query,con=self.engine)
        for index,row in df.iterrows():
            calls_participants = ast.literal_eval(row['participants'])
            for record in calls_participants:
                record.update({"key":f"{record['call_id']}_{record['id']}"})
                dict_list.append(record)
        
        return dict_list
    
    @staticmethod
    def get_delete_stmt(record):
        table = Calls_Participants.getTable()
        stmt = table.delete().where(table.c.key==record['key'])
        return stmt

    @staticmethod
    def get_insert_stmt(calls_participants_record:str) -> dict:
        table = Calls_Participants.getTable()
        stmt = table.insert().values(calls_participants_record)
        return stmt

    def import_to_sql(self,record):
        deletestmt = Calls_Participants.get_delete_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(deletestmt)
        insertstmt = Calls_Participants.get_insert_stmt(record)
        with self.engine.begin() as connx:
            connx.execute(insertstmt)
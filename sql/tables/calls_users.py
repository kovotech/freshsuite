from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable

class Calls_Users:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        table = Table(
            "calls_users",MetaData(),
            Column("id",BigInteger,primary_key=True),
            Column("name",Text),
            Column('email',Text),
            Column('phone',Text),
            Column('status',Integer),
            Column('preference',BigInteger),
            Column('mobile_app_preference',BigInteger),
            Column('last_call_time',DateTime),
            Column('last_seen_time',DateTime),
            Column('confirmed',Boolean),
            Column('language',Text),
            Column('time_zone',Text),
            Column('deleted',Boolean),
            Column('role',Text),
            Column('teams',Text)
        )
        return table
    
    def create_table(self):
        table = Calls_Users.getTable()
        stmt = CreateTable(table,if_not_exists=True)
        with self.engine.begin() as connx:
            connx.execute(stmt)

    @staticmethod
    def map(src:dict):
        output_dict:dict = {}
        try:
            output_dict['id'] = src['id']
        except:
            output_dict['id'] = None
        try:
            output_dict['name'] = src['name']
        except:
            output_dict['name'] = None
        try:
            output_dict['email'] = src['email']
        except:
            output_dict['email'] = None
        try:
            output_dict['phone'] = src['phone']
        except:
            output_dict['phone'] = None
        try:
            output_dict['status'] = src['status']
        except:
            output_dict['status'] = None
        try:
            output_dict['preference'] = src['preference']
        except:
            output_dict['preference'] = None
        try:
            output_dict['mobile_app_preference'] = src['mobile_app_preference']
        except:
            output_dict['mobile_app_preference'] = None
        try:
            output_dict['last_call_time'] = src['last_call_time']
        except:
            output_dict['last_call_time'] = None
        try:
            output_dict['last_seen_time'] = src['last_seen_time']
        except:
            output_dict['last_seen_time'] = None
        try:
            output_dict['confirmed'] = src['confirmed']
        except:
            output_dict['confirmed'] = None
        try:
            output_dict['language'] = src['language']
        except:
            output_dict['language'] = None
        try:
            output_dict['time_zone'] = src['time_zone']
        except:
            output_dict['time_zone'] = None
        try:
            output_dict['deleted'] = src['deleted']
        except:
            output_dict['deleted'] = None
        try:
            output_dict['role'] = src['role']
        except:
            output_dict['role'] = None
        try:
            output_dict['teams'] = str(src['teams'])
        except:
            output_dict['teams'] = None
        return output_dict
        
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Calls_Users.map(record)
        table = Calls_Users.getTable()
        stmt = table.insert().values(payload)
        return stmt
    
    @staticmethod
    def get_delete_stmt(record:dict):
        table = Calls_Users.getTable()
        stmt = table.delete().where(table.c.id==record['id'])
        return stmt
    
    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Calls_Users.get_delete_stmt(record)
            insertStmt = Calls_Users.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)
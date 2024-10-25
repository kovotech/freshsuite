from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable

class Calls:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        table = Table(
            "calls",MetaData(),
            Column('id',BigInteger,primary_key=True),
            Column('direction',Text),
            Column('parent_call_id',Text),
            Column('root_call_id',Text),
            Column('phone_number_id',BigInteger),
            Column('phone_number',Text),
            Column('assigned_agent_id',BigInteger),
            Column('assigned_agent_name',Text),
            Column('assigned_team_id',BigInteger),
            Column('assigned_team_name',Text),
            Column('assigned_call_queue_id',Text),
            Column('assigned_call_queue_name',Text),
            Column('assigned_ivr_id',Text),
            Column('assigned_ivr_name',Text),
            Column('bill_duration',DECIMAL(20,2)),
            Column('bill_duration_unit',Text),
            Column('created_time',DateTime),
            Column('updated_time',DateTime),
            Column('call_notes',Text),
            Column('recording',Text),
            Column('recording_to_redact',Text),
            Column('integrated_resources',Text),
            Column('participants',Text),
            Column('parallel_call_groups',Text)
            )
        
        return table
    
    def create_table(self):
        table = Calls.getTable()
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
            output_dict['direction'] = src['direction']
        except:
            output_dict['direction'] = None
        try:
            output_dict['parent_call_id'] = src['parent_call_id']
        except:
            output_dict['parent_call_id'] = None
        try:
            output_dict['root_call_id'] = src['root_call_id']
        except:
            output_dict['root_call_id'] = None
        try:
            output_dict['phone_number_id'] = src['phone_number_id']
        except:
            output_dict['phone_number_id'] = None
        try:
            output_dict['phone_number'] = src['phone_number']
        except:
            output_dict['phone_number'] = None
        try:
            output_dict['assigned_agent_id'] = src['assigned_agent_id']
        except:
            output_dict['assigned_agent_id'] = None
        try:
            output_dict['assigned_agent_name'] = src['assigned_agent_name']
        except:
            output_dict['assigned_agent_name'] = None
        try:
            output_dict['assigned_team_id'] = src['assigned_team_id']
        except:
            output_dict['assigned_team_id'] = None
        try:
            output_dict['assigned_team_name'] = src['assigned_team_name']
        except:
            output_dict['assigned_team_name'] = None
        try:
            output_dict['assigned_call_queue_id'] = src['assigned_call_queue_id']
        except:
            output_dict['assigned_call_queue_id'] = None
        try:
            output_dict['assigned_call_queue_name'] = src['assigned_call_queue_name']
        except:
            output_dict['assigned_call_queue_name'] = None
        try:
            output_dict['assigned_ivr_id'] = src['assigned_ivr_id']
        except:
            output_dict['assigned_ivr_id'] = None
        try:
            output_dict['assigned_ivr_name'] = src['assigned_ivr_name']
        except:
            output_dict['assigned_ivr_name'] = None
        try:
            output_dict['bill_duration'] = src['bill_duration']
        except:
            output_dict['bill_duration'] = None
        try:
            output_dict['bill_duration_unit'] = src['bill_duration_unit']
        except:
            output_dict['bill_duration_unit'] = None
        try:
            output_dict['created_time'] = src['created_time']
        except:
            output_dict['created_time'] = None
        try:
            output_dict['updated_time'] = src['updated_time']
        except:
            output_dict['updated_time'] = None
        try:
            output_dict['call_notes'] = src['call_notes']
        except:
            output_dict['call_notes'] = None
        try:
            output_dict['recording'] = str(src['recording'])
        except:
            output_dict['recording'] = None
        try:
            output_dict['recording_to_redact'] = src['recording_to_redact']
        except:
            output_dict['recording_to_redact'] = None
        try:
            output_dict['integrated_resources'] = str(src['integrated_resources'])
        except:
            output_dict['integrated_resources'] = None
        try:
            output_dict['participants'] = str(src['participants'])
        except:
            output_dict['participants'] = None
        try:
            output_dict['parallel_call_groups'] = str(src['parallel_call_groups'])
        except:
            output_dict['parallel_call_groups'] = None

        return output_dict
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Calls.map(record)
        table = Calls.getTable()
        stmt = table.insert().values(payload)
        return stmt
    
    @staticmethod
    def get_delete_stmt(record:dict):
        table = Calls.getTable()
        stmt = table.delete().where(table.c.id==record['id'])
        return stmt
    
    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Calls.get_delete_stmt(record)
            insertStmt = Calls.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)
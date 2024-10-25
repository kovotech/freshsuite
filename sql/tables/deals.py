from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable

class Deals:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
                    "deals",MetaData(),
                    Column('id',BigInteger,primary_key=True),
                    Column('name',Text),
                    Column('amount',DECIMAL(20,0)),
                    Column('base_currency_amount',DECIMAL(20,0)),
                    Column('expected_close',Text),
                    Column('closed_date',Date),
                    Column('stage_updated_time',DateTime),
                    Column('probability',Integer),
                    Column('updated_at',DateTime),
                    Column('created_at',DateTime),
                    Column('deal_pipeline_id',BigInteger),
                    Column('deal_stage_id',BigInteger),
                    Column('age',Integer),
                    Column('links_conversations',Text),
                    Column('links_document_associations',Text),
                    Column('links_notes',Text),
                    Column('links_tasks',Text),
                    Column('links_appointments',Text),
                    Column('recent_note',Text),
                    Column('completed_sales_sequences',Text),
                    Column('active_sales_sequences',Text),
                    Column('web_form_id',Text),
                    Column('upcoming_activities_time',Text),
                    Column('collaboration',Text),
                    Column('last_assigned_at',DateTime),
                    Column('last_contacted_sales_activity_mode',Text),
                    Column('last_contacted_via_sales_activity',Text),
                    Column('expected_deal_value',DECIMAL(20,0)),
                    Column('is_deleted',Boolean),
                    Column('team_user_ids',Text),
                    Column('avatar',Text),
                    Column('fc_widget_collaboration_convo_token',Text),
                    Column('fc_widget_collaboration_auth_token',Text),
                    Column('fc_widget_collaboration_encoded_jwt_token',Text),
                    Column('forecast_category',Text),
                    Column('deal_prediction',DECIMAL(20,0)),
                    Column('deal_prediction_last_updated_at',Text),
                    Column('freddy_forecast_metrics',Text),
                    Column('last_deal_prediction',Text),
                    Column('rotten_days',Integer),
                    Column('tags',Text),
                    Column('sales_activity_ids',Text),
                    Column('owner_id',BigInteger),
                    Column('creater_id',BigInteger),
                    Column('updater_id',BigInteger),
                    Column('lead_source_id',Text),
                    Column('contact_ids',Text),
                    Column('sales_account_id',BigInteger),
                    Column('deal_type_id',BigInteger),
                    Column('deal_reason_id',BigInteger),
                    Column('campaign_id',BigInteger),
                    Column('deal_payment_status_id',BigInteger),
                    Column('deal_product_id',BigInteger),
                    Column('currency_id',BigInteger),
                    Column('cf_deal_closed',Boolean),
                    Column('cf_deal_close_date',Date),
                    Column('cf_vertical',Text)
                )
        return TABLE
    
    def create_table(self):
        table = Deals.getTable()
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
            output_dict['amount'] = src['amount']
        except:
            output_dict['amount'] = 0
        try:
            output_dict['base_currency_amount'] = src['base_currency_amount']
        except:
            output_dict['base_currency_amount'] = 0
        try:
            output_dict['expected_close'] = src['expected_close']
        except:
            output_dict['expected_close'] = None
        try:
            output_dict['closed_date'] = output_dict['closed_date']
        except:
            output_dict['closed_date'] = None
        try:
            output_dict['stage_updated_time'] = src['stage_updated_time']
        except:
            output_dict['stage_updated_time'] = None
        try:
            output_dict['probability'] = src['probability']
        except:
            output_dict['probability'] = None
        try:
            output_dict['updated_at'] = src['updated_at']
        except:
            output_dict['updated_at'] = None
        try:
            output_dict['created_at'] = src['created_at']
        except:
            output_dict['created_at'] = None
        try:
            output_dict['deal_pipeline_id'] = src['deal_pipeline_id']
        except:
            output_dict['deal_pipeline_id'] = None
        try:
            output_dict['deal_stage_id'] = src['deal_stage_id']
        except:
            output_dict['deal_stage_id'] = None
        try:
            output_dict['age'] = src['age']
        except:
            output_dict['age'] = None
        try:
            output_dict['links_conversations'] = src['links']['conversations']
        except:
            output_dict['links_conversations'] = None
        try:
            output_dict['links_document_associations'] = src['links']['document_associations']
        except:
            output_dict['links_document_associations'] = None
        try:
            output_dict['links_notes'] = src['links']['notes']
        except:
            output_dict['links_notes'] = None
        try:
            output_dict['links_tasks'] = src['links']['tasks']
        except:
            output_dict['links_tasks'] = None
        try:
            output_dict['links_appointments'] = src['links']['appointments']
        except:
            output_dict['links_appointments'] = None
        try:
            output_dict['recent_note'] = src['recent_note']
        except:
            output_dict['recent_note'] = None
        try:
            output_dict['completed_sales_sequences'] = str(src['completed_sales_sequences'])
        except:
            output_dict['completed_sales_sequences'] = None
        try:
            output_dict['active_sales_sequences'] = str(src['active_sales_sequences'])
        except:
            output_dict['active_sales_sequences'] = None
        try:
            output_dict['web_form_id'] = src['web_form_id']
        except:
            output_dict['web_form_id'] = None
        try:
            output_dict['upcoming_activities_time'] = src['upcoming_activities_time']
        except:
            output_dict['upcoming_activities_time'] = None
        try:
            output_dict['collaboration'] = str(src['collaboration'])
        except:
            output_dict['collaboration'] = None
        try:
            output_dict['last_assigned_at'] = src['last_assigned_at']
        except:
            output_dict['last_assigned_at'] = None
        try:
            output_dict['last_contacted_sales_activity_mode'] = src['last_contacted_sales_activity_mode']
        except:
            output_dict['last_contacted_sales_activity_mode'] = None
        try:
            output_dict['last_contacted_via_sales_activity'] = src['last_contacted_via_sales_activity']
        except:
            output_dict['last_contacted_via_sales_activity'] = None
        try:
            output_dict['expected_deal_value'] = src['expected_deal_value']
        except:
            output_dict['expected_deal_value'] = None
        try:
            output_dict['is_deleted'] = src['is_deleted']
        except:
            output_dict['is_deleted'] = None
        try:
            output_dict['team_user_ids'] = str(src['team_user_ids'])
        except:
            output_dict['team_user_ids'] = None
        try:
            output_dict['avatar'] = src['avatar']
        except:
            output_dict['avatar'] = None
        try:
            output_dict['fc_widget_collaboration_convo_token'] = src['fc_widget_collaboration']['convo_token']
        except:
            output_dict['fc_widget_collaboration_convo_token'] = None
        try:
            output_dict['fc_widget_collaboration_auth_token'] = src['fc_widget_collaboration']['auth_token']
        except:
            output_dict['fc_widget_collaboration_auth_token'] = None
        try:
            output_dict['fc_widget_collaboration_encoded_jwt_token'] = src['fc_widget_collaboration']['encoded_jwt_token']
        except:
            output_dict['fc_widget_collaboration_encoded_jwt_token'] = None
        try:
            output_dict['forecast_category'] = src['forecast_category']
        except:
            output_dict['forecast_category'] = None
        try:
            output_dict['deal_prediction'] = src['deal_prediction']
        except:
            output_dict['deal_prediction'] = None
        try:
            output_dict['deal_prediction_last_updated_at'] = src['deal_prediction_last_updated_at']
        except:
            output_dict['deal_prediction_last_updated_at'] = None
        try:
            output_dict['freddy_forecast_metrics'] = src['freddy_forecast_metrics']
        except:
            output_dict['freddy_forecast_metrics'] = None
        try:
            output_dict['last_deal_prediction'] = src['last_deal_prediction']
        except:
            output_dict['last_deal_prediction'] = None
        try:
            output_dict['rotten_days'] = src['rotten_days']
        except:
            output_dict['rotten_days'] = None
        try:
            output_dict['tags'] = str(src['tags'])
        except:
            output_dict['tags'] = None
        try:
            output_dict['sales_activity_ids'] = str(src['sales_activity_ids'])
        except:
            output_dict['sales_activity_ids'] = None
        try:
            output_dict['owner_id'] = src['owner_id']
        except:
            output_dict['owner_id'] = None
        try:
            output_dict['creater_id'] = src['creater_id']
        except:
            output_dict['creater_id'] = None
        try:
            output_dict['updater_id'] = src['updater_id']
        except:
            output_dict['updater_id'] = None
        try:
            output_dict['lead_source_id'] = src['lead_source_id']
        except:
            output_dict['lead_source_id'] = None
        try:
            output_dict['contact_ids'] = str(src['contact_ids'])
        except:
            output_dict['contact_ids'] = None
        try:
            output_dict['sales_account_id'] = src['sales_account_id']
        except:
            output_dict['sales_account_id'] = None
        try:
            output_dict['deal_type_id'] = src['deal_type_id']
        except:
            output_dict['deal_type_id'] = None
        try:
            output_dict['deal_reason_id'] = src['deal_reason_id']
        except:
            output_dict['deal_reason_id'] = None
        try:
            output_dict['campaign_id'] = src['campaign_id']
        except:
            output_dict['campaign_id'] = None
        try:
            output_dict['deal_payment_status_id'] = src['deal_payment_status_id']
        except:
            output_dict['deal_payment_status_id'] = None
        try:
            output_dict['deal_product_id'] = src['deal_product_id']
        except:
            output_dict['deal_product_id'] = None
        try:
            output_dict['currency_id'] = src['currency_id']
        except:
            output_dict['currency_id'] = None
        try:
            output_dict['cf_deal_closed'] = src['custom_field']['cf_deal_closed']
        except:
            output_dict['cf_deal_closed'] = None
        try:
            output_dict['cf_deal_close_date'] = src['custom_field']['cf_deal_close_date']
        except:
            output_dict['cf_deal_close_date'] = None
        try:
            output_dict['cf_vertical'] = src['custom_field']['cf_vertical']
        except:
            output_dict['cf_vertical'] = None
    
        return output_dict
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Deals.map(record)
        table = Deals.getTable()
        stmt = table.insert().values(payload)
        return stmt
    
    @staticmethod
    def get_delete_stmt(record:dict):
        table = Deals.getTable()
        stmt = table.delete().where(table.c.id==record['id'])
        return stmt
    
    def update_recyclebin(self,record:dict):
        with self.engine.begin() as connx:
            table = Deals.getTable()
            stmt = table.update().where(table.c.id==record['id']).values(is_deleted=True)
            connx.execute(stmt)

    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Deals.get_delete_stmt(record)
            insertStmt = Deals.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)
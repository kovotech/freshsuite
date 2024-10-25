MODULE_PATH = "sql.accounts"

from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable

class Accounts:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "accounts",MetaData(),
            Column('id',BigInteger,primary_key=True),
            Column('name',Text),
            Column('address',Text),
            Column('city',Text),
            Column('state',Text),
            Column('zipcode',Text),
            Column('country',Text),
            Column('number_of_employees',Integer),
            Column('annual_revenue',DECIMAL(20,0)),
            Column('website',Text),
            Column('owner_id',BigInteger),
            Column('phone',Text),
            Column('open_deals_amount',Text),
            Column('open_deals_count',Integer),
            Column('won_deals_amount',Text),
            Column('won_deals_count',Integer),
            Column('last_contacted',DateTime),
            Column('last_contacted_mode',Text),
            Column('facebook',Text),
            Column('twitter',Text),
            Column('linkedin',Text),
            Column('links_conversations',Text),
            Column('links_document_associations',Text),
            Column('links_notes',Text),
            Column('links_tasks',Text),
            Column('links_appointments',Text),
            Column('cf_cabinet_brands',Text),
            Column('cf_products_interested',Text),
            Column('cf_dealer_type',Text),
            Column('created_at',DateTime),
            Column('updated_at',DateTime),
            Column('avatar',Text),
            Column('parent_sales_account_id',Text),
            Column('recent_note',Text),
            Column('last_contacted_via_sales_activity',DateTime),
            Column('last_contacted_sales_activity_mode',Text),
            Column('completed_sales_sequences',Text),
            Column('active_sales_sequences',Text),
            Column('last_assigned_at',DateTime),
            Column('is_deleted',Boolean),
            Column('team_user_ids',Text),
            Column('web_form_ids',Text),
            Column('tags',Text),
            Column('creater_id',BigInteger),
            Column('updater_id',BigInteger),
            Column('business_type_id',BigInteger),
            Column('industry_type_id',BigInteger)
        )
        return TABLE
    
    def create_table(self):
        table = Accounts.getTable()
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
            output_dict['address'] = src['address']
        except:
            output_dict['address'] = None
        try:
            output_dict['city'] = src['city']
        except:
            output_dict['city'] = None
        try:
            output_dict['state'] = src['state']
        except:
            output_dict['state'] = None
        try:
            output_dict['zipcode'] = src['zipcode']
        except:
            output_dict['zipcode'] = None
        try:
            output_dict['country'] = src['country']
        except:
            output_dict['country'] = None
        try:
            output_dict['number_of_employees'] = src['number_of_employees']
        except:
            output_dict['number_of_employees'] = None
        try:
            output_dict['annual_revenue'] = src['annual_revenue']
        except:
            output_dict['annual_revenue'] = None
        try:
            output_dict['website'] = src['website']
        except:
            output_dict['website'] = None
        try:
            output_dict['owner_id'] = src['owner_id']
        except:
            output_dict['owner_id'] = None
        try:
            output_dict['phone'] = src['phone']
        except:
            output_dict['phone'] = None
        try:
            output_dict['open_deals_amount'] = src['open_deals_amount']
        except:
            output_dict['open_deals_amount'] = None
        try:
            output_dict['open_deals_count'] = src['open_deals_count']
        except:
            output_dict['open_deals_count'] = None
        try:
            output_dict['won_deals_amount'] = src['won_deals_amount']
        except:
            output_dict['won_deals_amount'] = None
        try:
            output_dict['won_deals_count'] = src['won_deals_count']
        except:
            output_dict['won_deals_count'] = None
        try:
            output_dict['last_contacted'] = src['last_contacted']
        except:
            output_dict['last_contacted'] = None
        try:
            output_dict['last_contacted_mode'] = src['last_contacted_mode']
        except:
            output_dict['last_contacted_mode'] = None
        try:
            output_dict['facebook'] = src['facebook']
        except:
            output_dict['facebook'] = None
        try:
            output_dict['twitter'] = src['twitter']
        except:
            output_dict['twitter'] = None
        try:
            output_dict['linkedin'] = src['linkedin']
        except:
            output_dict['linkedin'] = None
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
            output_dict['cf_cabinet_brands'] = src['custom_field']['cf_cabinet_brands']
        except:
            output_dict['cf_cabinet_brands'] = None
        try:
            output_dict['cf_products_interested'] = src['custom_field']['cf_products_interested']
        except:
            output_dict['cf_products_interested'] = None
        try:
            output_dict['cf_dealer_type'] = src['custom_field']['cf_dealer_type']
        except:
            output_dict['cf_dealer_type'] = None
        try:
            output_dict['created_at'] = src['created_at']
        except:
            output_dict['created_at'] = None
        try:
            output_dict['updated_at'] = src['updated_at']
        except:
            output_dict['updated_at'] = None
        try:
            output_dict['avatar'] = src['avatar']
        except:
            output_dict['avatar'] = None
        try:
            output_dict['parent_sales_account_id'] = src['parent_sales_account_id']
        except:
            output_dict['parent_sales_account_id'] = None
        try:
            output_dict['recent_note'] = src['recent_note']
        except:
            output_dict['recent_note'] = None
        try:
            output_dict['last_contacted_via_sales_activity'] = src['last_contacted_via_sales_activity']
        except:
            output_dict['last_contacted_via_sales_activity'] = None
        try:
            output_dict['last_contacted_sales_activity_mode'] = src['last_contacted_sales_activity_mode']
        except:
            output_dict['last_contacted_sales_activity_mode'] = None
        try:
            output_dict['completed_sales_sequences'] = str(src['completed_sales_sequences'])
        except:
            output_dict['completed_sales_sequences'] = None
        try:
            output_dict['active_sales_sequences'] = str(src['active_sales_sequences'])
        except:
            output_dict['active_sales_sequences'] = None
        try:
            output_dict['last_assigned_at'] = src['last_assigned_at']
        except:
            output_dict['last_assigned_at'] = None
        try:
            output_dict['is_deleted'] = src['is_deleted']
        except:
            output_dict['is_deleted'] = None
        try:
            output_dict['team_user_ids'] = str(src['team_user_ids'])
        except:
            output_dict['team_user_ids'] = None
        try:
            output_dict['web_form_ids'] = str(src['web_form_ids'])
        except:
            output_dict['web_form_ids'] = None
        try:
            output_dict['tags'] = str(src['tags'])
        except:
            output_dict['tags'] = None
        try:
            output_dict['creater_id'] = src['creater_id']
        except:
            output_dict['creater_id'] = None
        try:
            output_dict['updater_id'] = src['updater_id']
        except:
            output_dict['updater_id'] = None
        try:
            output_dict['business_type_id'] = src['business_type_id']
        except:
            output_dict['business_type_id'] = None
        try:
            output_dict['industry_type_id'] = src['industry_type_id']
        except:
            output_dict['industry_type_id'] = None
        return output_dict
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Accounts.map(record)
        table = Accounts.getTable()
        stmt = table.insert().values(payload)
        return stmt
        
    @staticmethod
    def get_delete_stmt(record:dict):
        table = Accounts.getTable()
        stmt = table.delete().where(table.c.id==record['id'])
        return stmt
        
    def update_recyclebin(self,record:dict):
        with self.engine.begin() as connx:
            table = Accounts.getTable()
            stmt = table.update().where(table.c.id==record['id']).values(is_deleted=True)
            connx.execute(stmt)
        
    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Accounts.get_delete_stmt(record)
            insertStmt = Accounts.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)
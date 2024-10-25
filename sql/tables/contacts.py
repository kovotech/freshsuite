MODULE_PATH = "sql.contacts"

from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, DateTime, BigInteger, DECIMAL, Integer, text, Boolean, Time, Text
import sqlalchemy
from sqlalchemy.schema import CreateTable

class Contacts:
    def __init__(self,engine:sqlalchemy.engine.Engine) -> None:
        self.engine=engine

    @staticmethod
    def getTable() -> Table:
        TABLE = Table(
            "contacts",MetaData(),
            Column('id',BigInteger,primary_key=True),
            Column('first_name',Text),
            Column('last_name',Text),
            Column('display_name',Text),
            Column('avatar',Text),
            Column('job_title',Text),
            Column('city',Text),
            Column('state',Text),
            Column('zipcode',Text),
            Column('country',Text),
            Column('email',Text),
            Column('emails',Text),
            Column('time_zone',Text),
            Column('work_number',Text),
            Column('mobile_number',Text),
            Column('address',Text),
            Column('last_seen',Text),
            Column('lead_score',Integer),
            Column('last_contacted',DateTime),
            Column('open_deals_amount',Text),
            Column('won_deals_amount',Text),
            Column('links_conversations',Text),
            Column('links_timeline_feeds',Text),
            Column('links_document_associations',Text),
            Column('links_notes',Text),
            Column('links_tasks',Text),
            Column('links_appointments',Text),
            Column('links_reminders',Text),
            Column('links_duplicates',Text),
            Column('links_connections',Text),
            Column('last_contacted_sales_activity_mode',Text),
            Column('cf_captured_by_lead_field',Text),
            Column('cf_demo_booked_by',Text),
            Column('cf_lead_lot',Text),
            Column('cf_lead_source',Text),
            Column('cf_lifecycle_stages',Text),
            Column('cf_business_opportunity',Text),
            Column('cf_website',Text),
            Column('cf_duplicatey',Text),
            Column('cf_demo_done_by',Text),
            Column('cf_department_migrated',Text),
            Column('cf_has_authority_migrated',Boolean),
            Column('cf_what_city_is_your_business_in',Text),
            Column('cf_location_city_lead_field',Text),
            Column('cf_tell_us_a_bit_about_what_you_re_looking_for_lead_field',Text),
            Column('cf_services_lead_field',Text),
            Column('cf_work_number_backup',Text),
            Column('cf_mobile_number_backup',Text),
            Column('cf_your_message_lead_field',Text),
            Column('cf_business_opportunities',Boolean),
            Column('created_at',DateTime),
            Column('updated_at',DateTime),
            Column('keyword',Text),
            Column('medium',Text),
            Column('last_contacted_mode',Text),
            Column('recent_note',Text),
            Column('won_deals_count',Integer),
            Column('last_contacted_via_sales_activity',DateTime),
            Column('completed_sales_sequences',Text),
            Column('active_sales_sequences',Text),
            Column('web_form_ids',Text),
            Column('open_deals_count',Integer),
            Column('last_assigned_at',DateTime),
            Column('facebook',Text),
            Column('twitter',Text),
            Column('linkedin',Text),
            Column('is_deleted',Boolean),
            Column('team_user_ids',Text),
            Column('external_id',Text),
            Column('work_email',Text),
            Column('subscription_status',Integer),
            Column('subscription_types',Text),
            Column('unsubscription_reason',Text),
            Column('other_unsubscription_reason',Text),
            Column('customer_fit',Integer),
            Column('whatsapp_subscription_status',Integer),
            Column('sms_subscription_status',Integer),
            Column('last_seen_chat',Text),
            Column('first_seen_chat',Text),
            Column('locale',Text),
            Column('total_sessions',Text),
            Column('system_tags',Text),
            Column('first_campaign',Text),
            Column('first_medium',Text),
            Column('first_source',Text),
            Column('last_campaign',Text),
            Column('last_medium',Text),
            Column('last_source',Text),
            Column('latest_campaign',Text),
            Column('latest_medium',Text),
            Column('latest_source',Text),
            Column('mcr_id',Text),
            Column('phone_numbers',Text),
            Column('sales_accounts',Text),
            Column('tags',Text),
            Column('owner_id',BigInteger),
            Column('creater_id',BigInteger),
            Column('updater_id',BigInteger),
            Column('lead_source_id',BigInteger),
            Column('contact_status_id',BigInteger),
            Column('lifecycle_stage_id',BigInteger),
            Column('cf_demo_completed',Boolean),
            Column('cf_demo_completion_date',DateTime),
            Column('cf_vertical',Text),
            Column('cf_utm_source',Text),
            Column('cf_utm_medium',Text),
            Column('cf_utm_campaign',Text),
            Column('cf_business_identified',Boolean),
            Column('cf_business_identified_date',DateTime),
            Column('cf_business_type',Text)
        )
        return TABLE
    
    def create_table(self):
        table = Contacts.getTable()
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
            output_dict['first_name'] = src['first_name']
        except:
            output_dict['first_name'] = None
        try:
            output_dict['last_name'] = src['last_name']
        except:
            output_dict['last_name'] = None
        try:
            output_dict['display_name'] = src['display_name']
        except:
            output_dict['display_name'] = None
        try:
            output_dict['avatar'] = src['avatar']
        except:
            output_dict['avatar'] = None
        try:
            output_dict['job_title'] = src['job_title']
        except:
            output_dict['job_title'] = None
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
            output_dict['email'] = src['email']
        except:
            output_dict['email'] = None
        try:
            output_dict['emails'] = str(src['emails'])
        except:
            output_dict['emails'] = None
        try:
            output_dict['time_zone'] = src['time_zone']
        except:
            output_dict['time_zone'] = None
        try:
            output_dict['work_number'] = src['work_number']
        except:
            output_dict['work_number'] = None
        try:
            output_dict['mobile_number'] = src['mobile_number']
        except:
            output_dict['mobile_number'] = None
        try:
            output_dict['address'] = src['address']
        except:
            output_dict['address'] = None
        try:
            output_dict['last_seen'] = src['last_seen']
        except:
            output_dict['last_seen'] = None
        try:
            output_dict['lead_score'] = src['lead_score']
        except:
            output_dict['lead_score'] = None
        try:
            output_dict['last_contacted'] = src['last_contacted']
        except:
            output_dict['last_contacted'] = None
        try:
            output_dict['open_deals_amount'] = src['open_deals_amount']
        except:
            output_dict['open_deals_amount'] = None
        try:
            output_dict['won_deals_amount'] = src['won_deals_amount']
        except:
            output_dict['won_deals_amount'] = None
        try:
            output_dict['links_conversations'] = src['links']['conversations']
        except:
            output_dict['links_conversations'] = None
        try:
            output_dict['links_timeline_feeds'] = src['links']['timeline_feeds']
        except:
            output_dict['links_timeline_feeds'] = None
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
            output_dict['links_reminders'] = src['links']['reminders']
        except:
            output_dict['links_reminders'] = None
        try:
            output_dict['links_duplicates'] = src['links']['duplicates']
        except:
            output_dict['links_duplicates'] = None
        try:
            output_dict['links_connections'] = src['links']['connections']
        except:
            output_dict['links_connections'] = None
        try:
            output_dict['last_contacted_sales_activity_mode'] = src['last_contacted_sales_activity_mode']
        except:
            output_dict['last_contacted_sales_activity_mode'] = None
        try:
            output_dict['cf_captured_by_lead_field'] = src['custom_field']['cf_captured_by_lead_field']
        except:
            output_dict['cf_captured_by_lead_field'] = None
        try:
            output_dict['cf_demo_booked_by'] = src['custom_field']['cf_demo_booked_by']
        except:
            output_dict['cf_demo_booked_by'] = None
        try:
            output_dict['cf_lead_lot'] = src['custom_field']['cf_lead_lot']
        except:
            output_dict['cf_lead_lot'] = None
        try:
            output_dict['cf_lead_source'] = src['custom_field']['cf_lead_source']
        except:
            output_dict['cf_lead_source'] = None
        try:
            output_dict['cf_lifecycle_stages'] = src['custom_field']['cf_lifecycle_stages']
        except:
            output_dict['cf_lifecycle_stages'] = None
        try:
            output_dict['cf_business_opportunity'] = src['custom_field']['cf_business_opportunity']
        except:
            output_dict['cf_business_opportunity'] = None
        try:
            output_dict['cf_website'] = src['custom_field']['cf_website']
        except:
            output_dict['cf_website'] = None
        try:
            output_dict['cf_duplicatey'] = src['custom_field']['cf_duplicatey']
        except:
            output_dict['cf_duplicatey'] = None
        try:
            output_dict['cf_demo_done_by'] = src['custom_field']['cf_demo_done_by']
        except:
            output_dict['cf_demo_done_by'] = None
        try:
            output_dict['cf_department_migrated'] = src['custom_field']['cf_department_migrated']
        except:
            output_dict['cf_department_migrated'] = None
        try:
            output_dict['cf_has_authority_migrated'] = src['custom_field']['cf_has_authority_migrated']
        except:
            output_dict['cf_has_authority_migrated'] = None
        try:
            output_dict['cf_what_city_is_your_business_in'] = src['custom_field']['cf_what_city_is_your_business_in']
        except:
            output_dict['cf_what_city_is_your_business_in'] = None
        try:
            output_dict['cf_location_city_lead_field'] = src['custom_field']['cf_location_city_lead_field']
        except:
            output_dict['cf_location_city_lead_field'] = None
        try:
            output_dict['cf_tell_us_a_bit_about_what_you_re_looking_for_lead_field'] = src['custom_field']['cf_tell_us_a_bit_about_what_you_re_looking_for_lead_field']
        except:
            output_dict['cf_tell_us_a_bit_about_what_you_re_looking_for_lead_field'] = None
        try:
            output_dict['cf_services_lead_field'] = src['custom_field']['cf_services_lead_field']
        except:
            output_dict['cf_services_lead_field'] = None
        try:
            output_dict['cf_work_number_backup'] = src['custom_field']['cf_work_number_backup']
        except:
            output_dict['cf_work_number_backup'] = None
        try:
            output_dict['cf_mobile_number_backup'] = src['custom_field']['cf_mobile_number_backup']
        except:
            output_dict['cf_mobile_number_backup'] = None
        try:
            output_dict['cf_your_message_lead_field'] = src['custom_field']['cf_your_message_lead_field']
        except:
            output_dict['cf_your_message_lead_field'] = None
        try:
            output_dict['cf_business_opportunities'] = src['custom_field']['cf_business_opportunities']
        except:
            output_dict['cf_business_opportunities'] = None
        try:
            output_dict['cf_business_identified'] = src['custom_field']['cf_business_identified']
        except:
            output_dict['cf_business_identified'] = None
        try:
            output_dict['cf_business_identified_date'] = src['custom_field']['cf_business_identified_date']
        except:
            output_dict['cf_business_identified_date'] = None
        try:
            output_dict['created_at'] = src['created_at']
        except:
            output_dict['created_at'] = None
        try:
            output_dict['updated_at'] = src['updated_at']
        except:
            output_dict['updated_at'] = None
        try:
            output_dict['keyword'] = src['keyword']
        except:
            output_dict['keyword'] = None
        try:
            output_dict['medium'] = src['medium']
        except:
            output_dict['medium'] = None
        try:
            output_dict['last_contacted_mode'] = src['last_contacted_mode']
        except:
            output_dict['last_contacted_mode'] = None
        try:
            output_dict['recent_note'] = src['recent_note']
        except:
            output_dict['recent_note'] = None
        try:
            output_dict['won_deals_count'] = src['won_deals_count']
        except:
            output_dict['won_deals_count'] = None
        try:
            output_dict['last_contacted_via_sales_activity'] = src['last_contacted_via_sales_activity']
        except:
            output_dict['last_contacted_via_sales_activity'] = None
        try:
            output_dict['completed_sales_sequences'] = str(src['completed_sales_sequences'])
        except:
            output_dict['completed_sales_sequences'] = None
        try:
            output_dict['active_sales_sequences'] = str(src['active_sales_sequences'])
        except:
            output_dict['active_sales_sequences'] = None
        try:
            output_dict['web_form_ids'] = str(src['web_form_ids'])
        except:
            output_dict['web_form_ids'] = None
        try:
            output_dict['open_deals_count'] = src['open_deals_count']
        except:
            output_dict['open_deals_count'] = None
        try:
            output_dict['last_assigned_at'] = src['last_assigned_at']
        except:
            output_dict['last_assigned_at'] = None
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
            output_dict['is_deleted'] = src['is_deleted']
        except:
            output_dict['is_deleted'] = None
        try:
            output_dict['team_user_ids'] = str(src['team_user_ids'])
        except:
            output_dict['team_user_ids'] = None
        try:
            output_dict['external_id'] = src['external_id']
        except:
            output_dict['external_id'] = None
        try:
            output_dict['work_email'] = src['work_email']
        except:
            output_dict['work_email'] = None
        try:
            output_dict['subscription_status'] = src['subscription_status']
        except:
            output_dict['subscription_status'] = None
        try:
            output_dict['subscription_types'] = src['subscription_types']
        except:
            output_dict['subscription_types'] = None
        try:
            output_dict['unsubscription_reason'] = src['unsubscription_reason']
        except:
            output_dict['unsubscription_reason'] = None
        try:
            output_dict['other_unsubscription_reason'] = src['other_unsubscription_reason']
        except:
            output_dict['other_unsubscription_reason'] = None
        try:
            output_dict['customer_fit'] = src['customer_fit']
        except:
            output_dict['customer_fit'] = None
        try:
            output_dict['whatsapp_subscription_status'] = src['whatsapp_subscription_status']
        except:
            output_dict['whatsapp_subscription_status'] = None
        try:
            output_dict['sms_subscription_status'] = src['sms_subscription_status']
        except:
            output_dict['sms_subscription_status'] = None
        try:
            output_dict['last_seen_chat'] = src['last_seen_chat']
        except:
            output_dict['last_seen_chat'] = None
        try:
            output_dict['first_seen_chat'] = src['first_seen_chat']
        except:
            output_dict['first_seen_chat'] = None
        try:
            output_dict['locale'] = src['locale']
        except:
            output_dict['locale'] = None
        try:
            output_dict['total_sessions'] = src['total_sessions']
        except:
            output_dict['total_sessions'] = None
        try:
            output_dict['system_tags'] = str(src['total_sessions'])
        except:
            output_dict['system_tags'] = None
        try:
            output_dict['first_campaign'] = src['first_campaign']
        except:
            output_dict['first_campaign'] = None
        try:
            output_dict['first_medium'] = src['first_medium']
        except:
            output_dict['first_medium'] = None
        try:
            output_dict['first_source'] = src['first_source']
        except:
            output_dict['first_source'] = None
        try:
            output_dict['last_campaign'] = src['last_campaign']
        except:
            output_dict['last_campaign'] = None
        try:
            output_dict['last_medium'] = src['last_medium']
        except:
            output_dict['last_medium'] = None
        try:
            output_dict['last_source'] = src['last_source']
        except:
            output_dict['last_source'] = None
        try:
            output_dict['latest_campaign'] = src['latest_campaign']
        except:
            output_dict['latest_campaign'] = None
        try:
            output_dict['latest_medium'] = src['latest_medium']
        except:
            output_dict['latest_medium'] = None
        try:
            output_dict['latest_source'] = src['latest_source']
        except:
            output_dict['latest_source'] = None
        try:
            output_dict['mcr_id'] = src['mcr_id']
        except:
            output_dict['mcr_id'] = None
        try:
            output_dict['phone_numbers'] = str(src['phone_numbers'])
        except:
            output_dict['phone_numbers'] = None
        try:
            output_dict['sales_accounts'] = str(src['sales_accounts'])
        except:
            output_dict['sales_accounts'] = None
        try:
            output_dict['tags'] = str(src['tags'])
        except:
            output_dict['tags'] = None
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
            output_dict['contact_status_id'] = src['contact_status_id']
        except:
            output_dict['contact_status_id'] = None
        try:
            output_dict['lifecycle_stage_id'] = src['lifecycle_stage_id']
        except:
            output_dict['lifecycle_stage_id'] = None
        try:
            output_dict['cf_demo_completed'] = src['custom_field']['cf_demo_completed']
        except:
            output_dict['cf_demo_completed'] = None
        try:
            output_dict['cf_demo_completion_date'] = src['custom_field']['cf_demo_completion_date']
        except:
            output_dict['cf_demo_completion_date'] = None
        try:
            output_dict['cf_vertical'] = src['custom_field']['cf_vertical']
        except:
            output_dict['cf_vertical'] = None
        try:
            output_dict['cf_utm_source'] = src['custom_field']['cf_utm_source']
        except:
            output_dict['cf_utm_source'] = None
        try:
            output_dict['cf_utm_medium'] = src['custom_field']['cf_utm_medium']
        except:
            output_dict['cf_utm_medium'] = None
        try:
            output_dict['cf_utm_campaign'] = src['custom_field']['cf_utm_campaign']
        except:
            output_dict['cf_utm_campaign'] = None
        try:
            output_dict['cf_business_type'] = src['custom_field']['cf_business_type']
        except:
            output_dict['cf_business_type'] = None

        
        return output_dict
    
    @staticmethod
    def get_insert_stmt(record:dict):
        payload = Contacts.map(record)
        table = Contacts.getTable()
        stmt = table.insert().values(payload)
        return stmt
        
    @staticmethod
    def get_delete_stmt(record:dict):
        table = Contacts.getTable()
        stmt = table.delete().where(table.c.id==record['id'])
        return stmt
        
    def update_recyclebin(self,record:dict):
        with self.engine.begin() as connx:
            table = Contacts.getTable()
            stmt = table.update().where(table.c.id==record['id']).values(is_deleted=True)
            connx.execute(stmt)
        
    def import_to_sql(self,record:dict):
        with self.engine.begin() as connx:
            deleteStmt = Contacts.get_delete_stmt(record)
            insertStmt = Contacts.get_insert_stmt(record)
            connx.execute(deleteStmt)
            connx.execute(insertStmt)
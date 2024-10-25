from sql.tables.deals_contacts import Deals_Contacts
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.datetime_util import timer
from otherOps.joblogger import JobLogger

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger_all = JobLogger("Logs","Deals Contacts","all")
log_path_all = logger_all.getPath()
logger_recent = JobLogger("Logs","Deals Contacts","recent")
log_path_recent = logger_recent.getPath()


def deals_contacts_all():
    sql_query = f"SELECT id,contact_ids FROM `{config['sql_credentials']['db']}`.`deals` WHERE contact_ids != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    deal_contacts_job = Deals_Contacts(sql_engine)

    deal_contacts = deal_contacts_job.str_to_dict(sql_query)

    pos = 0
    for record in deal_contacts:
        deal_contacts_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['dealId']}",flush=True)
        pos += 1
    logger_all.writeLog(log_path_all,'info',f"{pos} records imported")
    


def deals_contacts_days(days,date_col):
    date_dict = timer(days)
    sql_query = f"SELECT id,contact_ids FROM `{config['sql_credentials']['db']}`.`deals` WHERE {date_col} BETWEEN '{date_dict['start']}' AND '{date_dict['end']}' AND contact_ids != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    deal_contacts_job = Deals_Contacts(sql_engine)

    deal_contacts = deal_contacts_job.str_to_dict(sql_query)

    pos = 0
    for record in deal_contacts:
        deal_contacts_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['dealId']}",flush=True)
        pos += 1
    logger_recent.writeLog(log_path_recent,'info',f"{pos} records imported")

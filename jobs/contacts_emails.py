from sql.tables.contacts_emails import Contacts_Emails
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.datetime_util import timer
from otherOps.joblogger import JobLogger

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger_all = JobLogger("Logs","Contacts Emails","all")
log_path_all = logger_all.getPath()
logger_recent = JobLogger("Logs","Contacts Emails","recent")
log_path_recent = logger_recent.getPath()


def contacts_emails_all():
    sql_query = f"SELECT id,emails FROM `{config['sql_credentials']['db']}`.`contacts` WHERE emails != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    contacts_emails_job = Contacts_Emails(sql_engine)

    contacts_emails = contacts_emails_job.str_to_dict(sql_query)

    pos = 0
    for record in contacts_emails:
        contacts_emails_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['contactId']}",flush=True)
        pos += 1
    logger_all.writeLog(log_path_all,'info',f"{pos} records imported")
    


def contacts_emails_days(days,date_col):
    date_dict = timer(days)
    sql_query = f"SELECT id,emails FROM `{config['sql_credentials']['db']}`.`contacts` WHERE {date_col} BETWEEN '{date_dict['start']}' AND '{date_dict['end']}' AND emails != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    contacts_emails_job = Contacts_Emails(sql_engine)

    contacts_emails = contacts_emails_job.str_to_dict(sql_query)

    pos = 0
    for record in contacts_emails:
        contacts_emails_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['contactId']}",flush=True)
        pos += 1
    logger_recent.writeLog(log_path_recent,'info',f"{pos} records imported")

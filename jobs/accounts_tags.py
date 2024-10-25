from sql.tables.accounts_tags import Accounts_Tags
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.datetime_util import timer
from otherOps.joblogger import JobLogger

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger_all = JobLogger("Logs","Accounts Tags","all")
log_path_all = logger_all.getPath()
logger_recent = JobLogger("Logs","Accounts Tags","recent")
log_path_recent = logger_recent.getPath()


def accounts_tags_all():
    sql_query = f"SELECT id,tags FROM `{config['sql_credentials']['db']}`.`accounts` WHERE tags != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    accounts_tags_job = Accounts_Tags(sql_engine)

    accounts_tags = accounts_tags_job.str_to_dict(sql_query)

    pos = 0
    for record in accounts_tags:
        accounts_tags_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['accountId']}",flush=True)
        pos += 1
    logger_all.writeLog(log_path_all,'info',f"{pos} records imported")
    


def accounts_tags_days(days,date_col):
    date_dict = timer(days)
    sql_query = f"SELECT id,tags FROM `{config['sql_credentials']['db']}`.`accounts` WHERE {date_col} BETWEEN '{date_dict['start']}' AND '{date_dict['end']}' AND tags != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    accounts_tags_job = Accounts_Tags(sql_engine)

    accounts_tags = accounts_tags_job.str_to_dict(sql_query)

    pos = 0
    for record in accounts_tags:
        accounts_tags_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['accountId']}",flush=True)
        pos += 1
    logger_recent.writeLog(log_path_recent,'info',f"{pos} records imported")

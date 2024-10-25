from sql.tables.calls_participants import Calls_Participants
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.datetime_util import timer
from otherOps.joblogger import JobLogger

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger_all = JobLogger("Logs","Calls Participants","all")
log_path_all = logger_all.getPath()
logger_recent = JobLogger("Logs","Calls Participants","recent")
log_path_recent = logger_recent.getPath()


def calls_participants_all():
    sql_query = f"SELECT id,participants FROM `{config['sql_credentials']['db']}`.`calls` WHERE participants != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    calls_participants_job = Calls_Participants(sql_engine)

    calls_participants = calls_participants_job.str_to_dict(sql_query)

    pos = 0
    for record in calls_participants:
        calls_participants_job.import_to_sql(record)
        print(f"{pos+1} Record Imported: {record['id']}",flush=True)
        pos += 1
    logger_all.writeLog(log_path_all,'info',f"{pos} records imported")
    


def calls_participants_days(days,date_col):
    date_dict = timer(days)
    print(date_dict)
    sql_query = f"SELECT id,participants FROM `{config['sql_credentials']['db']}`.`calls` WHERE {date_col} BETWEEN '{date_dict['start']}' AND '{date_dict['end']}' AND participants != '[]'"
    
    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
    
    calls_participants_job = Calls_Participants(sql_engine)

    calls_participants = calls_participants_job.str_to_dict(sql_query)

    pos = 0
    for record in calls_participants:
        calls_participants_job.import_to_sql(record)
        print(f"{pos+1} Record Imported {record['id']}",flush=True)
        pos += 1
    logger_recent.writeLog(log_path_recent,'info',f"{pos} records imported")

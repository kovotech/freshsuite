from apis.freshcaller import FreshCallerApi
from sql.tables.calls import Calls
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.joblogger import JobLogger
from otherOps.datechecker import fw_date_range_checker
import datetime as dt

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
# logger = JobLogger("Logs","Calls",logger_job_folder)
# log_path = logger.getPath()

def calls_cronjob(days:int,logger:JobLogger):
    log_path = logger.getPath()
    logger.writeLog(log_path,'info',"Job Started...")
    print("Job Started...",flush=True)

    api_obj = FreshCallerApi(
        domain=config['freshcaller_credentials']['domain'],
        key=config['freshcaller_credentials']['key'],
        record_type="calls"
    )
    all_calls = []
    loop_status = True
    startDate = dt.datetime.now()
    page = 1
    while loop_status is True:
        response = api_obj.getCalls(
            page=page,
            per_page=1000
        )
        if len(response) > 0:
            for record in response:
                if fw_date_range_checker(record,"updated_time",startDate,days=days) == 'Yes':
                    all_calls.append(record)
                else:
                    break
            loop_status = False
        else:
            loop_status = False

        print(f"Page No {page} Api Called.",flush=True)
        logger.writeLog(log_path,'info',f"Page No {page} Api Called.")
        page += 1

    sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )

    sql_job = Calls(sql_engine)

    import_count = 0
    for record in all_calls:
        sql_job.import_to_sql(record)
        import_count += 1
        print(f"{record['id']} imported to calls table",flush=True)

    print(f"{import_count} records imported to calls table",flush=True)
    logger.writeLog(log_path,'info',f"{import_count} records imported to calls table")

def calls_bulkjob(pageStart,pageEnd,pagechunk):
    pageRange = range(pageStart,pageEnd+1)
    api_obj = FreshCallerApi(
        domain=config['freshcaller_credentials']['domain'],
        key=config['freshcaller_credentials']['key'],
        record_type="calls"
    )
    total_import_count = 0
    for page in pageRange:
        response = api_obj.getCalls(page=page,per_page=pagechunk)
        print(f"Api called for page no {page}")
        sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
        sql_job = Calls(sql_engine)
        for record in response:
            sql_job.import_to_sql(record)
            print(f"{record['id']} imported to calls table")
            total_import_count += 1
        
        print(f"Data Import done for page no {page}",flush=True)

    print(f"{total_import_count} records has been imported to calls table")
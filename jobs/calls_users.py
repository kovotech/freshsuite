from apis.freshcaller import FreshCallerApi
from sql.tables.calls_users import Calls_Users
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.joblogger import JobLogger

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
# logger = JobLogger("Logs","Calls",logger_job_folder)
# log_path = logger.getPath()


def calls_users_bulkjob(pageStart,pageEnd,pagechunk):
    pageRange = range(pageStart,pageEnd+1)
    api_obj = FreshCallerApi(
        domain=config['freshcaller_credentials']['domain'],
        key=config['freshcaller_credentials']['key'],
        record_type="users"
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
        sql_job = Calls_Users(sql_engine)
        for record in response:
            sql_job.import_to_sql(record)
            print(f"{record['id']} imported to calls_users table")
            total_import_count += 1
        
        print(f"Data Import done for page no {page}",flush=True)

    print(f"{total_import_count} records has been imported to calls_users table")


def calls_users_cronjob(logger:JobLogger):
    log_path = logger.getPath()
    logger.writeLog(log_path,'info',"Job Started...")
    print("Job Started...",flush=True)

    api_obj = FreshCallerApi(
        domain=config['freshcaller_credentials']['domain'],
        key=config['freshcaller_credentials']['key'],
        record_type="users"
    )
    all_users = []
    loop_status = True
    page = 1
    while loop_status is True:
        response = api_obj.getCalls(
            page=page,
            per_page=1000
        )
        if len(response) > 0:
            for record in response:
                all_users.append(record)
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

    sql_job = Calls_Users(sql_engine)

    import_count = 0
    for record in all_users:
        sql_job.import_to_sql(record)
        import_count += 1
        print(f"{record['id']} imported to calls_users table",flush=True)

    print(f"{import_count} records imported to calls_users table",flush=True)
    logger.writeLog(log_path,'info',f"{import_count} records imported to calls_users table")
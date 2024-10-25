from jobs.deals import Job
from otherOps.emailTrigger import trigger_email
from otherOps.exceptions import format_exception_logfile
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH, EMAIL_RECIEVER
from otherOps.joblogger import JobLogger

RECORD_TYPE="deals"
JOB_TYPE="new"
SQL_JOB="import_to_sql"
API_EMBEDDINGS="sales_activities,owner,creater,updater,source,contacts,sales_account,deal_stage,deal_type,deal_reason,campaign,deal_payment_status,deal_product,currency,probability"

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger = JobLogger("Logs",RECORD_TYPE,JOB_TYPE)
log_path = logger.getPath()

if __name__ == '__main__':
    try:
        job = Job(RECORD_TYPE,JOB_TYPE,SQL_JOB)
        job.multi_records(import_records=True,api_embeddings=API_EMBEDDINGS,logger=logger)
    except Exception as e:
        trigger_email(
                host=config['smtp']['host'],
                user=config['smtp']['username'],
                pswd=config['smtp']['pswd'],
                port=config['smtp']['port2'],
                subject="Error in middleware job",
                sender=config['smtp']['sender'],
                reciever=EMAIL_RECIEVER,
                client="TestCompany",
                middleware="FreshSuite",
                jobname="New Deals",
                exception=e
                )
        error_message_logfile=format_exception_logfile(exception=e)
        logger.writeLog(log_path,'info',error_message_logfile)
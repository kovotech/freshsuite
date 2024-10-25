MODULE_PATH="pipelines.accounts.recyclebin"

from jobs.accounts import Job
from otherOps.emailTrigger import trigger_email
from otherOps.exceptions import format_exception_logfile
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH, EMAIL_RECIEVER
from otherOps.joblogger import JobLogger

RECORD_TYPE="sales_accounts"
JOB_TYPE="recyclebin"
SQL_JOB="update_recyclebin"

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger = JobLogger("Logs",RECORD_TYPE,JOB_TYPE)
log_path = logger.getPath()

if __name__ == '__main__':
    try:
        job = Job(RECORD_TYPE,JOB_TYPE,SQL_JOB)
        job.multi_records(update_recyclebin=True,api_embeddings="owner,creater,updater")
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
                jobname="Accounts Recyclebin",
                exception=e
                )
        error_message_logfile=format_exception_logfile(exception=e)
        logger.writeLog(log_path,'info',error_message_logfile)
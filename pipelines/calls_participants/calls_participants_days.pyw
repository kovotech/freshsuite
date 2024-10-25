from otherOps.joblogger import JobLogger
from jobs.calls_participants import calls_participants_days
from otherOps.emailTrigger import trigger_email
from otherOps.exceptions import format_exception_logfile
from otherOps.path import PROJECT_PATH, EMAIL_RECIEVER
from otherOps.configOps import ConfigOps

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")
logger = JobLogger("Logs","Calls Participants","recent")
log_path = logger.getPath()
DAYS=1
DATE_COLUMN='created_time'

if __name__ == '__main__':
    try:
        calls_participants_days(days=DAYS,date_col=DATE_COLUMN)
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
                jobname="Calls Recent Participants",
                exception=e
                )
        error_message_logfile=format_exception_logfile(exception=e)
        logger.writeLog(log_path,'info',error_message_logfile)
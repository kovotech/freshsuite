from apis.freshsuite import FreshSuiteApi
from sql.tables.deals import Deals
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH
from otherOps.joblogger import JobLogger

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")

class Job:
    def __init__(self,record_type,jobtype,sql_job) -> None:
        self.record_type=record_type
        self.jobtype=jobtype
        self.sql_job=sql_job
    

    def multi_records(self,logger:JobLogger,import_records=None,update_recyclebin=None,api_embeddings=""):
        # logger = JobLogger("Logs",self.record_type,self.jobtype)
        log_path = logger.getPath()

        logger.writeLog(log_path,'info',"Job Started...")
        print("Job Started...",flush=True)

        api_obj = FreshSuiteApi(
                                domain=config['freshsales_api_credentials']['domain'],
                                key=config['freshsales_api_credentials']['key'],
                                record_type=self.record_type
                            )
        all_contacts=[]
        deal_pipelines = str(config['deal_views'][self.jobtype]).split(",")
        for pipeline_view in deal_pipelines:
            loop=True
            pageNo=1
            print(f"====================={pipeline_view}=====================",flush=True)
            while loop is True:
                response = api_obj.get_multiple_records(
                    viewId=pipeline_view,
                    page=pageNo,
                    per_page=100,
                    include=api_embeddings
                )
                if len(response) > 0:
                    for record in response:
                        all_contacts.append(record)
                    logger.writeLog(log_path,'info',f"{self.jobtype} {self.record_type} API called for page {pageNo}")
                    print(f"{self.jobtype} {self.record_type} API called for page {pageNo}",flush=True)
                    pageNo += 1
                else:
                    loop = False
                    logger.writeLog(log_path,'info',f"{self.jobtype} {self.record_type} API's called...")
                    print(f"{self.jobtype} {self.record_type} API's called...",flush=True)


        logger.writeLog(log_path,'info',f"Imporing data to sql...")
        print("Imporing data to sql...",flush=True)
        sql_engine = Engine.mysql(
                                host=config['sql_credentials']['host'],
                                db=config['sql_credentials']['db'],
                                user=config['sql_credentials']['user'],
                                pswd=config['sql_credentials']['pass']
                                )
        sql_import_job = Deals(sql_engine)

        record_count = 0
        for record in all_contacts:
            if import_records is True:
                sql_import_job.import_to_sql(record)
                print(f"{self.jobtype} record:{record['id']} imported to table {self.record_type}",flush=True)
                record_count += 1
            elif update_recyclebin is True:
                sql_import_job.update_recyclebin(record)
                print(f"{self.jobtype} record:{record['id']} updated in {self.record_type} recyclebin",flush=True)
                record_count += 1
        
        logger.writeLog(log_path,'info',f"{self.jobtype} {record_count} imported to {self.record_type} table")
        print(f"{self.jobtype} {record_count} imported to {self.record_type} table",flush=True)


#         import json
#         with open(f"{PROJECT_PATH}/test.json",'w') as f:
#             json.dump(all_contacts,fp=f,indent=3)

#         return None

# logger = JobLogger("Logs","deals","all")

# job = Job(
#             record_type="deals",
#             jobtype="all",
#             sql_job=None         
#             )

# job.multi_records(logger=logger)
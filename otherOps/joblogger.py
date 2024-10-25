MODULE_PATH="otherOps.joblogger"

import datetime as dt
from otherOps.path import PROJECT_PATH
import os
import logging

class JobLogger:
    def __init__(self,path,folder,job_folder) -> None:
        self.path=path
        self.folder=folder
        self.job_folder=job_folder

    def getPath(self):        
        if self.path not in os.listdir(PROJECT_PATH):
            os.mkdir(f"{PROJECT_PATH}/{self.path}")

        if self.folder not in os.listdir(f"{PROJECT_PATH}/{self.path}/"):
            os.mkdir(f"{PROJECT_PATH}/{self.path}/{self.folder}")
        
        if self.job_folder not in os.listdir(f"{PROJECT_PATH}/{self.path}/{self.folder}/"):
            os.mkdir(f"{PROJECT_PATH}/{self.path}/{self.folder}/{self.job_folder}")

        path = f"{PROJECT_PATH}/{self.path}/{self.folder}/{self.job_folder}"
        return path
    
    def writeLog(self,path,level,messge):
        currentDate = dt.datetime.today()
        currentDate_str = dt.datetime.strftime(currentDate,"%Y%m%d")

        logging.basicConfig(
                            filename=f"{path}/{currentDate_str}.log",
                            format='%(asctime)s %(levelname)s %(message)s'
                        )
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        getattr(logger,level)(messge)
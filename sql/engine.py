MODULE_PATH = "sql.engine"

from sqlalchemy import create_engine
from otherOps.exceptions import SQLEngineError

class Engine:
    @staticmethod
    def mysql(host:str,db:str,user:str,pswd:str):
        try:
            engine = create_engine(f"mysql+pymysql://{user}:{pswd}@{host}/{db}")
            return engine
        except:
            raise SQLEngineError("Error in creating SQLAlchemy mysql engine")
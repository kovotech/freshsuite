from sql.tables.contacts_sales_accounts import Contacts_Sales_Accounts
from sql.engine import Engine
from otherOps.configOps import ConfigOps
from otherOps.path import PROJECT_PATH

config = ConfigOps.load_configFile(f"{PROJECT_PATH}/config.ini")

if __name__ == '__main__':
    create_table_job = Contacts_Sales_Accounts(Engine.mysql(
                                        host=config['sql_credentials']['host'],
                                        db=config['sql_credentials']['db'],
                                        user=config['sql_credentials']['user'],
                                        pswd=config['sql_credentials']['pass']         
                                        ))
    create_table_job.create_table()
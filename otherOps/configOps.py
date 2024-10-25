MODULE_PATH = "otherOps.configparser"

from configparser import ConfigParser
import datetime as dt
from otherOps.path import PROJECT_PATH

class ConfigOps:
    def load_configFile(path):
        config = ConfigParser()
        config.read(path)
        return config
    
class ConvertINI:
    def config_to_dict():
        config = ConfigOps.load_configFile()
        output_dict = {}
        for section in config.sections():
            items = config.items(section)
            output_dict[section] = dict(items)
        print("config file converted to python dictionary")
        return output_dict
    
    def update_dict(dict_,section,key,value):
        dict_[section][key] = value
        return dict_

    def dict_to_config(dict_):
        config = ConfigOps.load_configFile()
        dic = dict(dict_)
        for k,v in dic.items():
            config[k] = v
        with open('config.ini','w') as f:
            config.write(f)
        print("python dictionary loaded to config file")
MODULE_PATH = "sql.schema.accounts"

import sys
import os
import requests
import json
from otherOps.exceptions import ApiException

class FreshSuiteApi:
    def __init__(self,domain:str,key:str,record_type:str) -> None:
        self.domain=domain
        self.key=key
        self.record_type=record_type

    def get_single_record(self,recordId:int):
        url = f"{self.domain}/{self.record_type}/{recordId}"
        headers = {"Authorization":f"Token token={self.key}"}
        response = requests.get(
                                url=url,
                                headers=headers
                            )
        record_key_name = self.record_type[0:len(self.record_type)-1]
        if response.status_code==200:
            record = json.loads(response.content)[record_key_name]
            return record
        else:
            raise ApiException(f"Api call failed with error {response.status_code}")
        
    def get_multiple_records(self,viewId:int,page:int,per_page:int,include:str="",sort:str="",sort_type:str=""):
        url = f"{self.domain}/{self.record_type}/view/{viewId}"
        headers = {"Authorization":f"Token token={self.key}"}
        params = {
            "page":page,
            "per_page":per_page,
            "include":include,
            "sort":sort,
            "sort_type":sort_type
        }
        response = requests.get(
                                url=url,
                                headers=headers,
                                params=params
                            )
        if response.status_code==200:
            record = json.loads(response.content)
            return record[self.record_type]
        else:
            raise ApiException(f"Api call failed with error {response.status_code}")
        
    def filtered_search(self,filter_rule:list[dict],page:int,per_page:int,include:str="",sort:str="",sort_type:str=""):
        url = f"{self.domain}filtered_search/{self.record_type}"
        headers = {"Authorization":f"Token token={self.key}","Content-Type":"application/json"}
        params =  {
            "page":page,
            "per_page":per_page,
            "include":include,
            "sort":sort,
            "sort_type":sort_type
        }
        body = {
            "filter_rule":filter_rule
        }
        response = requests.post(
                                url=url,
                                headers=headers,
                                params=params,
                                json=body
                            )
        if response.status_code==200:
            record = json.loads(response.content)
            return record[f"{self.record_type}s"]
        else:
            raise ApiException(f"Api call failed with error {response.status_code}")
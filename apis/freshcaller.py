import requests
import json
from otherOps.exceptions import ApiException

class FreshCallerApi:
    def __init__(self,domain:str,key:str,record_type:str) -> None:
        self.domain=domain
        self.key=key
        self.record_type=record_type

    def getCalls(self,page:int,per_page:int) -> list[dict]:
        url = f"{self.domain}/{self.record_type}"
        params = {
            "per_page":per_page,
            "page":page
        }
        headers = {
            "Accept":"application/json",
            "X-Api-Auth":self.key
        }
        response = requests.get(
                                url=url,
                                params=params,
                                headers=headers
                            )
        if response.status_code==200:
            data = json.loads(response.content)[self.record_type]
            return data
        else:
            raise ApiException(f"Api call failed with error {response.status_code}")
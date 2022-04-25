"""This module pulls the weather results of each cities in US from 
given api "https://www.metaweather.com/api/" from 2013 and store them in S3 in the partitoned format """
import requests
import json
from time import strftime
import requests
import configparser
from datetime import datetime,timedelta
# import argparse
import pandas as pd
import json
import os
import re
from S3.s3_client import S3Details
from api.pull_data_from_api import PullDataFromApi
# response_API = requests.get('https://www.metaweather.com/api/')
# print(response_API.json())


config =configparser.ConfigParser()
config.read('develop.ini')

class PullDataUploadS3:
    """This class helps to pull data and upload to s3 in the parttioned path"""
    def __init__(self):
        """This is the init method of the class of PullDataFromApi"""
        self.bucket_name = config["s3"]["bucket"]
        # self.s3_path = config["s3"]["bucket_path"]
        self.local_s3_path=config["local"]['local_s3_path']
    def get_weather_information(self):
        '''This method gets weather information from PullDataFromApi class based on the cities'''
        city_woeid=config['woeid']['Albuquerque']
        PullDataFromApi.get_weather_data_cities(self,city_woeid)
                
    def put_partition_path(self,path):
        '''This method partitions the path based on city,year,month,date,hour'''
        # try:
        #         date_object = datetime.strptime(file_name, "%d.%m.%Y")
        #         partition_path = (
                    
        #             "pt_year="
        #             + date_object.strftime("%Y")
        #             + "/pt_month="
        #             + date_object.strftime("%m")
        #             + "/pt_day="
        #             + date_object.strftime("%d")
        #         )
        #        
        #         self.s3_client.upload_file(file_name, partition_path)
        #         rename = self.sftp_conn.rename_file(file_name)
        #         print(
        #             "The file has been uploaded to s3"
        #         )
        #         logger.info("The file has been uploaded to s3 ")
        #         return 'success'
        #     else:
        #         print("The", file_name, "is not in the prescribed format")
        #         logger.error("The file is not in the prescribed format")
        #         return 'failure'
        # except Exception as err:
        #     print("Cannot be uploaded in S3 in the parttioned path", err)
        #     logger.error("The file cannot be uploaded in the given path in s3")
        #     return 'failure'
    def upload_to_s3(self):
        """This method used to upload the file to s3 which data got from api"""
        s3_service = s3_service()
        for file in os.listdir(self.path):
            pass
    
def main():
    """This is the main method for this module"""
    pull_data = PullDataUploadS3()
    pull_data.get_weather_information()
    
if __name__ == '__main__':
    main()
        
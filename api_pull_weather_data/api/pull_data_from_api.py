""" This module is to pull data from api for each cities in US"""
import requests
import configparser
from datetime import datetime,timedelta


config =configparser.ConfigParser()
config.read('develop.ini')

class PullDataFromApi:
    """This class pulls data from an api as per their woeid-where on earth id"""
    def __init__(self):
        """This is the init method of the PullData class"""
        # self.city_woied = config['woeid']['Albuquerque']
        self.date = datetime.now()
        
    def get_weather_data_cities(self):
        """This method used to get the details using the woeid of city from api"""
        search_date = self.date.strftime('%Y-%m-%d')
       
        # for key,value in config['woeid']['cities_woeid'].items():
        for key,value in config.items('woeid'):
            # print(value)
            # print(key)
            response = requests.get('https://www.metaweather.com/api/'+value+'/'+search_date+'/').json()
            print(response)
            
# def main():
#     """This is the main method of the class """
#     pull=PullDataFromApi()
#     pull.get_weather_data_cities()

# if __name__=='__main__':
#     main()
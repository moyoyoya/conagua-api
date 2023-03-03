import configparser
import logging
import etl.extraction as ext
import etl.transform as cleaning
from datetime import datetime 
import os, os.path
import pandas as pd



#setting up global variables
global_database_raw = 'databases/raw'
global_database_municipalities = 'databases/data_municipios'
global_current_dir = os.path.abspath(os.path.dirname(__file__))
global_utc_time = str(datetime.utcnow())[:19]
global_utc_time = pd.to_datetime(global_utc_time, format='%Y-%m-%d %H:%M:%S')
# Set up logging
if not os.path.exists('project/logs'):
    os.makedirs('project/logs')
logging.basicConfig(filename='project/logs/logs_rutina.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logging.getLogger('sqlalchemy').setLevel(logging.WARNING)


#Calling the API to get the weather data
raw_data = ext.extract_weather_data(global_utc_time, global_database_raw, global_current_dir)
cleaned_data = cleaning.clean_data(raw_data)
municipalities_data = cleaning.upload_data_by_municipality(cleaned_data, global_current_dir, global_database_municipalities, global_utc_time)
logging.info('Weather data extraction completed')

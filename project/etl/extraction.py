import requests
import gzip
import json
import io
import pandas as pd
import etl.loading_to_database as dbf
import sys
import logging
import os


def extract_weather_data(global_utc_time, global_database_raw, global_current_dir):
    #This function calls the API and extracts the weather data from the API response
    #Then it cleans the data, adds the UTC time and the execution time and saves it to a database
    #It renames the rows that have the name San Juan Mixtepec but the lat and long of San Pedro Ixtlahuacan since it appeared in several calls to the API
    #It checks for duplicates and saves them to a csv file if there are any for further investigation
    #Finally creates a table with raw data and returns a dataframe 
 
    #Calling the API and getting the data as a dataframe
    api_endpoint_url = "https://smn.conagua.gob.mx/webservices/index.php"
    params = {"method": 3}
    response = requests.get(api_endpoint_url, params=params, stream=True)
    logging.info(f'API request was succesfull')


    if response.status_code != 200:
        logging.exception(f'API request failed with status code: {response.status_code}')
        sys.exit()
    
    #Decompressing the data and converting it to a json
    decompressed_content = gzip.decompress(response.content)
    readable_content = decompressed_content.decode("utf-8")
    cleaned_content = json.load(io.StringIO(readable_content))

    #Setting the data types for the columns
    dtypes_dict = { 'desciel': str, 'dh': int, 'dirvienc': str, 'dirvieng': float,
    'dpt': float, 'dsem': str, 'hloc': str, 'hr': float, 'ides': int, 'idmun': int,
    'lat': float, 'lon': float, 'nes': str, 'nhor': int, 'nmun': str, 'prec': float,
    'probprec': float, 'raf': float, 'temp': float, 'velvien': float}

    weather_table = pd.DataFrame(cleaned_content)
    weather_table = weather_table.astype(dtypes_dict)
    weather_table['hloc'] = pd.to_datetime(weather_table['hloc'], format='%Y%m%dT%H')
    #Adding a UTC time and Execution time to the dataframe
    weather_table['time_utc'] = weather_table['hloc'] + pd.to_timedelta(weather_table['dh'], unit='h') 
    weather_table['execution_time'] = pd.to_datetime(global_utc_time, format='%Y%m%dT%H')

    #Checking for duplicates
    weather_2 = weather_table[['nes','hloc','nmun']] 
    duplicates = weather_2[weather_2.duplicated()]

    #Cleaning wrong naming San Pedro Ixtlahuaca
    if len(duplicates['nmun'].unique()) == 0:
        logging.info('No duplicates found, if this message persists modify the code to check for duplicates')

    elif duplicates['nmun'].unique() == 'San Juan Mixtepec':
        weather_table.loc[(weather_table['nes'] == 'Oaxaca') & (weather_table['lat'] == 16.2775) & (weather_table['lon'] == -96.2995), 'nmun'] = 'San Pedro Ixtlahuaca'

        logging.info('San Pedro Ixtlahuaca has been renamed')    

    else:
        logging.info('Something went wrong, the result has more than one missnamed register,' +
            f"please check the State,latitude and longitude of the following municipalities: {' '.join(duplicates['nmun'].unique())}")
        
        #To avoid lossing the extracted data the dataframe will be saved as a csv
        weather_table.to_csv(f"weather_table{global_utc_time}.csv", index=False)
        sys.exit()


    #Uploading to the database
    utc_string = str(global_utc_time).replace(" ", "_").replace("-", "_")[:13]
    database_name = os.path.join(global_current_dir, global_database_raw, 'weather_data.db')
    folder = os.path.join(global_current_dir, global_database_raw)
    dbf.data_to_db(weather_table, folder ,database_name,f'raw_data_{utc_string}' )

    logging.info('Data extracted and saved to database')

    return weather_table
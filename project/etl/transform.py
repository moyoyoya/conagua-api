import logging
import os
from etl.loading_to_database import data_to_db, read_db
import sys


def cleaning_text(df, column_name):
    # This function takes the dataframe and cleans the text and returns the cleaned dataframe
    df[column_name] = df[column_name].str.replace(' ', '_').str.replace('á', 'a').str.replace('é', 'e'
    ).str.replace('í', 'i').str.replace('ó', 'o').str.replace('ú', 'u').str.replace('ñ', 'n').str.replace('Á', 'A').str.replace('É', 'E'
    ).str.replace('Í', 'I').str.replace('Ó', 'O').str.replace('Ú', 'U').str.replace('Ñ', 'N')
    df[column_name] = df[column_name].str.lower()
    return df[column_name]




def upload_data_by_municipality(df, global_current_dir, global_database_municipalities, global_utc_time):
    # This function takes the cleaned dataframe and creates a table for each munciipality taking the data from the dataframe 
    # and the time of execution

    municipalities =  df[['municipality', 'state']].drop_duplicates()
    municipalities['municipality'] = cleaning_text(municipalities, 'municipality')
    municipalities['state'] = cleaning_text(municipalities, 'state')
    
    database_name = os.path.join(global_current_dir, global_database_municipalities, 'municipalities.db')
    folder = os.path.join(global_current_dir, global_database_municipalities)
    for index, municipality in municipalities.iterrows():
        municipality_df = df[(df['state'] == municipality['state'])& (df['municipality'] == municipality['municipality'])]
        utc_string = str(global_utc_time).replace(" ", "_").replace("-", "_")[:13]
        #get data from current table


        logging.info(f'Getting data from {municipality["state"]}_{municipality["municipality"]}_current table')
        previous_current = read_db(f'{municipality["state"]}_{municipality["municipality"]}_current', database_name)
        #replace current table with new data
        if not previous_current.empty:

            previous_execution_time = previous_current['execution_time'].iloc[0].unique()
            previous_string = str(previous_execution_time[0]).replace(" ", "_").replace("-", "_")[:13]
            data_to_db(previous_execution_time,folder , database_name, f'{municipality["state"]}_{municipality["municipality"]}_{previous_string}')
            logging.info(f'Table {municipality} previous created')
            #create new current table and cross reference with previous current table to see if there are any missing values
            missing_values = previous_current[previous_current.columns.difference(['execution_time'])
                            ].isin(municipality_df[municipality_df.columns.difference(['execution_time'])]).all(axis=1)
            municipality_df = municipality_df.append(previous_current[missing_values])
            municipality_df['execution_time'] = utc_string
            municipality_df = municipality_df[(municipality_df['utc_time'] >= global_utc_time)]
            
        data_to_db(municipality_df,folder , database_name, f'{municipality["state"]}_{municipality["municipality"]}_current')
        logging.info(f'Table {municipality} current created')
    return None



def clean_data(df):
    # This function cleans the dataframe by dropping the columns that are not needed and renaming the columns.
    # It also drops the rows that have missing values.
    # Finally it returns the cleaned dataframe.
    core_data = df[['hloc', 'nes', 'nmun', 'prec', 'temp', 'time_utc', 'execution_time']]
    core_data = core_data.rename(columns={'hloc': 'local_time', 'nes': 'state', 'nmun': 'municipality', 
                            'prec': 'precipitation', 'temp': 'temperature', 'time_utc': 'utc_time', 
                            'execution_time': 'execution_time'})
    core_data['average_temperature'] = core_data['temperature'].rolling(window=3).mean()
    core_data['average_temperature'] = core_data['average_temperature'].fillna(0)
    core_data['average_precipitation'] = core_data['precipitation'].rolling(window=3).mean()
    core_data['average_precipitation'] = core_data['average_precipitation'].fillna(0)
    core_data = core_data.dropna()
    return core_data


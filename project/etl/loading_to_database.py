import sqlite3
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text, exc
import os
import sys
import pandas as pd
import logging


def create_db(folder, database):
    # This function first checks if the folder exists, if not it creates it.
    # Then it checks if the database exists, if not it creates it.
    # If there is an error when creating the database it logs the error and exits the program.
    if not os.path.exists(folder):
        os.makedirs(folder)
        logging.info(f'Folder {folder} created')
    if not os.path.isfile(f'{database}'):
        try:
            connection = sqlite3.connect(f'{database}')
            connection.close()
            logging.info(f'Database {database} created')

        except Exception as e:
            logging.error(f'There was an error when trying to create the database with error: {e}')
            sys.exit()
        finally:
            connection.close()
    return None

def data_to_db(df,folder, database, table_name):
    # This function checks first checks if the database and folders needed exists by calling the create_db function.
    # Then it tries to load the dataframe to the database, if it fails it logs the error and exits the program.


    create_db(folder, database)
    print('function continued')
    try:
        logging.info(f'Loading dataframe to the database sqlite:///{database}')
        engine = create_engine(f'sqlite:///{database}', echo=True)
        df.to_sql(table_name, engine, index=False, if_exists='replace')
        engine.dispose()
        logging.info(f'Dataframe was successfully loaded to the database {database}')
    except Exception as e:
        logging.error(f'There was an error when trying to load the dataframe to the database with error: {e}')
        sys.exit()

    return None


def read_db(table_name, database):
    # This function tries to read the database and return a pandas dataframe, if it fails it logs the error and exits the program.
    try:
        engine = create_engine(f'sqlite:///{database}', echo=True)
        connection = engine.connect()
        df = pd.read_sql(text(f'SELECT * FROM {table_name}'), connection)
        engine.dispose()
        return df
    except exc.OperationalError as e:
        logging.error(f'There was an error when trying to read the database with error: {e}')
        return pd.DataFrame()
    except Exception as e:
        logging.error(f'There was an error when trying to read the database with error: {e}')
        sys.exit()

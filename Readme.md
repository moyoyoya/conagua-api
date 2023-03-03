## Hourly Weather Data by municpality in Mexico

This ETL process connects to https://smn.conagua.gob.mx/webservices/index.php that provides hourly weather data. The raw data is then uploaded into a SQLite database, cleaned and transformed, and then aggregated to create a separate table with the current values.

#Steps

    1. Connect to the API to retrieve hourly weather data.
    2. Upload the raw data into a SQLite database.
    3. Remove unnecesary columns
    4. Aggregate the data to get the average of precipitation and temperature of the last two hours.
    5. Create a table for each municipality
    6. Create a table that uses the last extraction and the current to create a table with the most uptodate data

#Usage

    Install the required Python packages by running pip install -r requirements.txt.
    Create a config.ini file with the database names.
    Run the main.py script to execute the ETL process.

#Files and folders

    config.ini: configuration file with database connection information.
    main.py: Python script that performs the ETL process.
    requirements.txt: list of required Python packages.
    logs/logs.log: file with the execution information and errors

Notes


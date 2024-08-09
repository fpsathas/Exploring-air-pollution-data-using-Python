import requests
import pandas as pd
import numpy as np
import time
import datetime
# from sqlalchemy import create_engine
# from sqlalchemy.sql import text
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, date, timedelta, timezone
import time

def eu_aqi_limits():

    # This function stores the EU pollutant limits in the euAqiLimits table

    # Define the pollutant limits as a dictionary
    pollutant_limits = {
        'Pollutant': ['PM2.5', 'SO2 (1-hour)', 'SO2 (24-hour)', 'NO2 (1-hour)',
                    'NO2 (annual)', 'PM10 (24-hour)', 'PM10 (annual)',
                    'CO (8-hour)', 'CO (concentration)', 'Ozone (8-hour)'],
        'Limit (µg/m³ or mg/m³)': [20, 350, 125, 200, 40, 50, 40, 10, 10, 120]
    }

    # Convert dictionary to DataFrame
    df = pd.DataFrame(pollutant_limits)

    # Display the DataFrame
    print(df)

    # Save DataFrame to PostgreSQL table 'euAqiLimits'
    hook = PostgresHook("project-db")
    engine = hook.get_sqlalchemy_engine()
    con = engine.connect()
    df.to_sql('euAqiLimits', engine, if_exists='replace', index=False)

    return 0

def update_aqi_table(**kwargs):

    # This function checks whether the aqi table is up to date, and calls the insert function if necessary

    ti = kwargs['ti']
    # pull the empty variable pushed by the task 'check_aqi_table'
    empty = ti.xcom_pull(task_ids='check_aqi_table')
    if empty:
        print('The table is empty - Gathering data beginning from 29th November 2020')
        #bulk insert data from November 2020 until yesterday
        status = aqidata_insert()
        return status

    else:
        print('Aqi table has data')
        # connect to db
        hook = PostgresHook("project-db")
        engine = hook.get_sqlalchemy_engine()
        con = engine.connect()

        # get most recent timestamp stored in the table
        max_date = pd.read_sql("select max(date_time) as max_date from aqidata2",con)
        max_date = max_date['max_date'].iloc[0]

        # convert timestamp to date object
        last_update = pd.Timestamp(max_date, tz=None).to_pydatetime().date()

        # yesterday's date
        yesterday = date.today() - timedelta(days=1)

        # database was updated today if yesterday's date matches most recent date from db
        if str(yesterday) == str(last_update):
            print('Database is up to date!')
            return 0

        # database has not been updated
        elif (yesterday - last_update).days >= 1:
            print(f'Database has not been updated for {yesterday - last_update}')
            print('Starting update procedure...')

            # get next day from last update
            last_update = last_update + timedelta(days=1)
            print('data from which to start update', last_update)
            
            # convert date to epoch
            last_update_ts = datetime(last_update.year,last_update.month,last_update.day)
            last_update_ts = int(last_update_ts.replace(tzinfo=timezone.utc).timestamp())
            print('start time epoch will be',last_update_ts)

            #insert data starting from the most recent date in the db + 1 day, until yesterday
            status = aqidata_insert(start_date=last_update_ts)

        return status


def aqidata_insert(start_date = 1606608000):

    # This function calls the api to gather data, starting from 'start_date' and insert data to the db

    aqInfo = [] # List to store air quality info

    sql_query = "SELECT * FROM city_data2"

    # connect to db
    hook = PostgresHook("project-db")
    engine = hook.get_sqlalchemy_engine()
    con = engine.connect()

    #get city information, necessary fro calling the API
    df_cities = pd.read_sql_query(sql_query, engine)

    # Iterate over cities
    for index, row in df_cities.iterrows():
        # Extract relevant geographical data
        city = row['city']
        country = row['country']
        countrycode = row['countrycode']
        lat = row['lat']
        lon = row['lon']

        # Set start date and current date in epoch format 
        # Sunday, 29 November 2020 00:00:00 (1606608000) is the default value
        start = start_date     

        # Yesterday's date
        yesterday = date.today() - timedelta(days = 1)

        # convert to datetime object
        yesterday_dt = datetime(yesterday.year,yesterday.month,yesterday.day,23,0)

        # convert to utc timestamp. No matter the date, the timestamp will always be at 23:00 
        yesterday_dt = yesterday_dt.replace(tzinfo=timezone.utc).timestamp()
        end = int(yesterday_dt)

        # Fetch air quality data using OpenWeatherMap API
        API_KEY = 'ba03c44f2b1365660bf76be37c6e1485'
        api_url = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={API_KEY}'
        response = requests.get(api_url)

        print(f'Iterating over {city}: {lat}, {lon}, {start}, {end}')

        if response.status_code == requests.codes.ok:
            dataAQ = response.json()
            aqInfo = []
            # Extract air quality data and append to aqInfo list
            for i in range(len(dataAQ['list'])):
                #aqInfo = []
                d = {}  # Create a dictionary to store air quality information
                sample = dataAQ['list'][i]

                # Extract relevant air quality parameters
                d['aqi'] = sample['main']['aqi']
                d['co'] = sample['components']['co']
                d['no'] = sample['components']['no']
                d['no2'] = sample['components']['no2']
                d['o3'] = sample['components']['o3']
                d['so2'] = sample['components']['so2']
                d['pm2_5'] = sample['components']['pm2_5']
                d['pm10'] = sample['components']['pm10']
                d['nh3'] = sample['components']['nh3']
                d['epoch'] = sample['dt']
                d['lat'] = dataAQ['coord']['lat']
                d['lon'] = dataAQ['coord']['lon']
                d['city'] = city
                d['countrycode'] = countrycode
                d['country'] = country
                aqInfo.append(d)
        else:
            print("2 - Error:", response.status_code, response.text)

        print('Creating dataframe for air quality data...')
        df = pd.DataFrame(aqInfo)

        print('Converting epoch to datetime...')
        df['date_time'] = df.apply(lambda row: time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(row.epoch)) , axis = 1)

        print('Inserting to aqidata2')
        df.to_sql('aqidata2', con=engine, if_exists='append', index= False)

    print('Exit - Done')

    return 0
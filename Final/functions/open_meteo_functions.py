import requests_cache
from retry_requests import retry
import openmeteo_requests
import pandas as pd
from datetime import date, timedelta
from airflow.hooks.postgres_hook import PostgresHook
import time

def get_open_meteo_data(name,responses,meteo_data):

    # This function creates a dataframe with all the meteo data about a specific city

    # Most of the code in this function is provided directly by OpenMeteo
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(4).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(5).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(6).ValuesAsNumpy()
    hourly_soil_temperature_7_to_28cm = hourly.Variables(7).ValuesAsNumpy()

    hourly_data = {"date_time": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    # add city name to dataframe
    hourly_data["name"] = name
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["rain"] = hourly_rain
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
    hourly_data["soil_temperature_7_to_28cm"] = hourly_soil_temperature_7_to_28cm
    hourly_dataframe = pd.DataFrame(data = hourly_data)

    #concatenate the city-specific dataframe, with the full dataframe
    meteo_data = pd.concat([meteo_data,hourly_dataframe], ignore_index = True)

    return meteo_data


def call_open_meteo_api(start_date = '2020-01-01'):
    
    # This function calls the open meteo api once for each city 

    # open meteo session parameters
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # connect to db
    hook = PostgresHook("project-db")
    engine = hook.get_sqlalchemy_engine()
    con = engine.connect()

    # get city name, lat and long for each city
    df_city = pd.read_sql(sql="select * from city_data2;", con=con)
    lats = df_city['lat']
    longs = df_city['lon']
    city_name = df_city['city']

    # api url
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # empty dataframe to concatenate results
    meteo_data = pd.DataFrame()

    for name, lat,long in zip(city_name,lats,longs):
        print(name)
        params = {
        "latitude": lat,
        "longitude": long,
        "start_date": start_date,
        # the api has a two day delay, so most recent available data is today - 2 days
        "end_date": str(date.today() - timedelta(days=2)),
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "cloud_cover", "wind_speed_10m", "wind_gusts_10m", "soil_temperature_7_to_28cm"]
        }
        responses = openmeteo.weather_api(url, params=params)

        # give the api some breathing space or you will get an error
        time.sleep(2)

        if responses:
            meteo_data = get_open_meteo_data(name,responses,meteo_data)
            time.sleep(2)
        else:
            print('Could not communicate with open meteo api. Returned status, responses.status_code')

    print('Gathered all data, beginning to inser to db')
    meteo_data.to_sql("meteo_data", con=engine, if_exists = 'append',index=False)

    return meteo_data


def update_table(**kwargs):

    # This functions calls the insert function of the meteo_data table if necessary

    ti = kwargs['ti']
    empty = ti.xcom_pull(task_ids='check_table')
    if empty:
        print('The table is empty - Gathering data beginning from 2020-01-01')
        meteo_data_test = call_open_meteo_api()
        print(meteo_data_test.head())

    else:
        # connect to db
        hook = PostgresHook("project-db")
        engine = hook.get_sqlalchemy_engine()
        con = engine.connect()

        # get most recent date
        max_date = pd.read_sql("select max(date_time) as max_date from meteo_data",con)
        max_date = max_date['max_date'].iloc[0]

        # last date (-2) that the database was updated
        last_update_2 = pd.Timestamp(max_date, tz=None).to_pydatetime().date()

        # today's day, -2 days to account for api update delay
        today_2 = date.today() - timedelta(days=2)

        # database was updated today
        if str(today_2) == str(last_update_2):
            print('Database is up to date!')
            return 0

        # database has not been updated
        elif (today_2 - last_update_2).days >= 1:
            print(f'Database has not been updated for {today_2 - last_update_2} day(s)')
            print('Starting update procedure...')
            meteo_update = call_open_meteo_api(start_date=str(last_update_2 + timedelta(days=1)))
            print('Update completed')
            print(meteo_update.head())
    return 0
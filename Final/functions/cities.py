import requests
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json
import urllib
from airflow.hooks.postgres_hook import PostgresHook

def check_insert_cities(**kwargs):

    # This functions calls the insert function of the city table if necessary

    ti = kwargs['ti']

    # pull variable that was pushed by 'check_city_table'
    empty = ti.xcom_pull(task_ids='check_city_table')

    if empty:
        print('The city table is empty - Gathering city information')
        status = insert_cities()

    else:
        print('City table already has data!')

    return 0


def insert_cities():

    # This function inserts to the city table

    url = 'https://parseapi.back4app.com/classes/list_of_eu_union_countries?count=1&limit=30&keys=country,capital'
    headers = {
        'X-Parse-Application-Id': '7KffPh4njNmLBMB30kk9pvPGzBe7hwuK9SSQch6F', # This is the fake app's application id
        'X-Parse-Master-Key': '5dN5MVGWctV1CIDY4CmI6AC6LGQImCj8DlpEI2Ix' # This is the fake app's readonly master key
    }

    # call the api and store the returned data  to get the capital of every EU country
    data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
    df = pd.DataFrame(data['results'])

    # API to take the capitals and produce and add coordinates and add table to the db

    cities = [] # list of dictionaries for cities

    # will be used as the city_id
    id_counter = 1

    for index, row in df.iterrows():
        city=row['capital']
        country = row['country']

        # call the api for each city to get information like coordinates
        print('API call for - ', city)
        api_url = f'https://api.api-ninjas.com/v1/geocoding?city={city}&country={country}'
        response = requests.get(api_url, headers={'X-Api-Key': 'ArnJ6dPGVHDHg2I6YdJdGQ==fWZqAKFwF1HMvv77'})

        if response.status_code == 200:
            try:
                d={}
                datacoords = response.json()
                d['city_id']= id_counter
                id_counter = id_counter + 1
                print(id_counter)
                d['city'] = datacoords[0]['name']
                d['country']=country
                d['countrycode'] = datacoords[0]['country']
                d['lat'] = datacoords[0]['latitude']
                d['lon'] = datacoords[0]['longitude']

                cities.append(d)

            except IndexError:
                print("Error: The expected data structure is not present in the response.")
            except KeyError:
                print("Error: One or more keys are missing in the JSON data.")
            except ValueError:
                print("Error: Unable to decode JSON data.")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        else:
            print(f"Error: Received response with status code {response.status_code}")


    cities_df=pd.DataFrame(cities)

    print('Gathered all city data, beginning to insert to city table')

    # connect to db
    hook = PostgresHook("project-db")
    engine = hook.get_sqlalchemy_engine()
    con = engine.connect()

    # insert to table
    cities_df.to_sql("city_data2", con=engine, if_exists = 'append',index=False)
    print('Insert to city table done!')

    return 0
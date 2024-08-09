import requests
import pandas as pd
import numpy as np
import time
import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from airflow.hooks.postgres_hook import PostgresHook


def check_insert_countries(**kwargs):

    # This functions calls the insert function of the countryInfo table if necessary

    ti = kwargs['ti']

    # pull variable that was pushed by 'check_country_table'
    empty = ti.xcom_pull(task_ids='check_country_table')
    if empty:
        print('The country table is empty - Gathering country information')
        status = insert_countries()

    else:
        print('Country table already has data!')

    return 0

def insert_countries():

    # This function inserts to the countryInfo table

    # conncet to db
    hook = PostgresHook("project-db")
    engine = hook.get_sqlalchemy_engine()
    con = engine.connect()
    sql_query = "SELECT * FROM city_data2"

    # get city information, necessary to call the country API
    df_cities = pd.read_sql_query(sql_query, engine)

    countryInfo = []
    for index, row in df_cities.iterrows():
        country = row['country']

        api_url = f'https://api.api-ninjas.com/v1/country?name={country}'
        response = requests.get(api_url, headers={'X-Api-Key': 'ArnJ6dPGVHDHg2I6YdJdGQ==fWZqAKFwF1HMvv77'})
        if response.status_code == requests.codes.ok:
            data = response.json()
            print(data)
            d = {}
            d['name'] = data[0]['name']
            d['gdp'] = data[0]['gdp']
            d['gdp_per_capita'] = data[0]['gdp_per_capita']
            d['region'] = data[0]['region']
            d['pop_density'] = data[0]['pop_density']
            d['population'] = data[0]['population']
            d['life_expectancy_male'] = data[0]['life_expectancy_male']
            d['life_expectancy_female'] = data[0]['life_expectancy_female']

            countryInfo.append(d)
        else:
            print(f"Error fetching response for {country}:", response.status_code, response.text)

    df = pd.DataFrame(countryInfo)

    # Insert the DataFrame data into the countryInfo table
    print('Starting insert')
    df.to_sql('countryInfo', con=engine, if_exists='append', index= False)

    return 0
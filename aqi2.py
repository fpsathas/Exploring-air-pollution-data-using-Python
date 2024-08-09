import requests
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text

cities = ['Athens', 'Thessaloniki']
country = 'Greece'
aqInfo = [] # air quality info

for city in cities:
    print('')
    print('API call for - ', city)
    api_url = f'https://api.api-ninjas.com/v1/geocoding?city={city}&country={country}'
    response = requests.get(api_url, headers={'X-Api-Key': 'ArnJ6dPGVHDHg2I6YdJdGQ==fWZqAKFwF1HMvv77'})

    print(response.status_code)
    if response.status_code == requests.codes.ok:
        dataCoords = response.json()
        #print(dataCoords) 
        name = dataCoords[0]['name']
        countryCode = dataCoords[0]['country']
        state = dataCoords[0]['state']
        lat = dataCoords[0]['latitude']
        lon = dataCoords[0]['longitude']        
        
        start = '1714521600'
        end = '1717199999'
        API_KEY = 'ba03c44f2b1365660bf76be37c6e1485'
        api_url = f'http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={API_KEY}'
        response = requests.get(api_url)

        print('status code ' + str(response.status_code))
        if response.status_code == requests.codes.ok:
            res = response.text
            #print(res)
            dataAQ = response.json()
            #print('data = ', dataAQ) 
            #print('type = ', type(dataAQ))
            #print ('----------------------')
            #for i in dataAQ:
                #print(i)
        else:
            print("2 - Error:", response.status_code, response.text)

        for i in range(len(dataAQ['list'])):
        # for i in range(1):
            d = {}  # Create a dictionary to store air quality information
            # print(data['list'][i])
            sample = dataAQ['list'][i]
            
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
            d['name'] = name
            d['countrycode'] = countryCode
            d['state'] = state


            aqInfo.append(d)


    else:
        print("1 - Error:", response.status_code, response.text)

df = pd.DataFrame(aqInfo)
df['date_time'] = df.apply(lambda row: time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(row.epoch)) , axis = 1)
print(df.head())
engine = create_engine(
'postgresql+psycopg2:'                       
'//postgres:'            # username for postgres       
'docker'                 # password for postgres  
'@postgres-db:5432/'     # postgres container's name and the exposed port                
'postgres')

con = engine.connect()

# create an empty table tennis 

sql = """
  create table if not exists aqidata (
    aqi	int,
    co	real,
    no	real,
    no2	real,
    o3	real,
    so2	real,
    pm2_5	real,
    pm10	real,
    nh3	real,
    epoch  int,
    lat	real,
    lon	real,
    name	varchar(50),
    countrycode	varchar(3),
    state	varchar(50),
    date_time  date

);
"""
# execute the 'sql' query
with engine.connect().execution_options(autocommit=True) as conn:
    query = conn.execute(text(sql))

# insert the dataframe data to 'tennis' SQL table
with engine.connect().execution_options(autocommit=True) as conn:
    df.to_sql('aqidata', con=conn, if_exists='append', index= False)

#optional
print(pd.read_sql_query("""
select * from aqidata
""", con))

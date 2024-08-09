create table if not exists city_data2 (
    city_id int,
    city        varchar(50),
    country  varchar(50),
    countrycode varchar(3),
    lat real,
    lon real
);

 CREATE TABLE if not exists countryInfo (
    name VARCHAR(50),
    gdp FLOAT,
    gdp_per_capita FLOAT,
    region VARCHAR(50),
    pop_density FLOAT,
    population FLOAT,
    life_expectancy_male FLOAT,
    life_expectancy_female FLOAT
);

CREATE TABLE if not exists aqidata2 (
    id SERIAL ,
    aqi INT,
    co  REAL,
    no  REAL,
    no2 REAL,
    o3  REAL,
    so2 REAL,
    pm2_5       REAL,
    pm10        REAL,
    nh3 REAL,
    epoch  INT,
    lat REAL,
    lon REAL,
    city        VARCHAR(50),
    countrycode VARCHAR(3),
    country     VARCHAR(50),
    date_time  timestamp,
    primary key (date_time,city)
);

create table if not exists meteo_data(
        date_time                    timestamp,
        name                         varchar(50),
        temperature_2m               real,
        relative_humidity_2m         real,
        rain                         real,
        snowfall                     real,
        cloud_cover                  real,
        wind_speed_10m               real,
        wind_gusts_10m               real,
        soil_temperature_7_to_28cm   real,
        primary key (date_time,name)
        );
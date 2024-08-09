# Project Title: Exploring air pollution data using Python 

> **Authors:** 
> Lena Liosi,
> Fragkoulis Psathas,
> Alexandra Arseni

## Scope: Explore and Analyze air pollution and weather data in the EU capitals.

### Technologies: 
docker, postgres, airflow, metabase

#### Project files:

1. final-project.py 
This file is the main dag of the application, and it needs to be placed under airflow's dags/ folder.

2. create_tables.sql
This script contains the DDL statements for the project's tables. It needs to be placed under an sql/ folder inside airflow's dags/ folder.

3. check_table.py
Contains a function that checks whether a table is empty and returns boolean. The same function is used to check all tables.

4. cities.py
This file contains the functions needed to insert data inside the city_data2 table.

5. countries.py
This file contains the functions needed to insert data inside the countryInfo table.

6. aqi.py
This file contains the functions necessary to gather data from OpenWeather API about air quality and insert/update the aqidata2 table.

7. open_meteo_funtions.py
This file containes the functions necessary to gather data from the OpenMeteo API and insert/update the meteo_data table.

> All files from 3 to 7 need to be placed under a functions/ folder inside airflow's dags/ folder.

#### How to run this application:

1. Create a machine that can run docker.

2. Create a folder project/ and place inside it the contents of the database-docker folder.

3. Run docker-compose-up in the project/ folder to get the database and pgadmin running.

4. Create a folder airflow/ and place inside it the content of the airflow-docker folder. 

5. Run the command mkdir -p ./dags ./logs ./plugins ./config

> You can find more information about running airflow on docker here https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

6. Run the command docker compose up to start aiflow.

7. In the dags/ folder under arflow/ place the final-project.py.

8. Create a folder sql/ under dags/ and place inside it the create_tables.sql script.

9. Create a folder functions under dags/ and place inside it files 3 to 7.

10. Run the command docker run -d -p 3000:3000 --name metabase metabase/metabase to start the metabase instance.

> For more information on how to run metabase on docker click here https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker

11. You can now access pgadmin on your_ip:80, aiflow on your_ip:8080 and metabase on your_ip:3000.

#### Logging in and registering the database
> The below information can be found inside the docker files but is also listed here for convenience
1. Login to pdamin:

    username/password: user@domain.com,

    To register the db with pgadmin:

    hostname: postgresdb,
    port: 5432,
    database: postgres,
    username: postgres,
    password: docker

2. Login to airflow:

    username/password: airflow

    To register the database with airflow and/or metabase:

    hostname: your_ip,
    port: 5432,
    database: postgres,
    username: postgres,
    password: docker

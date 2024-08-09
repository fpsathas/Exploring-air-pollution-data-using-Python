import sys
sys.path.insert(0,"/home/azure-user/airflow/dags/functions")
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import functions.database as database
import functions.check_table as check
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import functions.open_meteo_functions as omf
import functions.cities as ct
import functions.aqi as aqi
import functions.countries as countries

with DAG (
    dag_id = "final-project",
    # This dag is meant to be scheduled daily or monthly, not hourly
    schedule = None
) as dag:

    # create tables
    create_task = SQLExecuteQueryOperator(
        task_id = "create_task",
        sql = "./sql/create_tables.sql",
        conn_id = "project-db"
    )

    # save variable is_empty to show whether city table is empty
    check_city_table = PythonOperator(
        task_id = "check_city_table",
        python_callable = check.is_empty,
        op_kwargs = {"table_name":'city_data2'},
        trigger_rule = 'all_success'
    )

    # insert to aqilimits table
    aqi_limits = PythonOperator(
        task_id = "aqi_limits",
        python_callable = aqi.eu_aqi_limits,
        trigger_rule = 'all_success'
    )

    # if the city table is empty, this task will insert data
    update_cities = PythonOperator(
        task_id = "update_cities",
        python_callable = ct.check_insert_cities,
        provide_context = True,
        trigger_rule = 'all_success'
    )

    # save variable is_empty to show whether countryInfo table is empty
    check_country_table = PythonOperator(
        task_id = "check_country_table",
        python_callable = check.is_empty,
        op_kwargs = {"table_name":'countryInfo'},
        trigger_rule = 'all_success'
    )

    # insert to the countryInfo table if necessary
    update_countries = PythonOperator(
        task_id = "update_countries",
        python_callable = countries.check_insert_countries,
        provide_context = True,
        trigger_rule = 'all_success'
    )

    # save variable is_empty to show whether aqidata table is empty
    check_aqi_table = PythonOperator(
        task_id = "check_aqi_table",
        python_callable = check.is_empty,
        op_kwargs = {"table_name":'aqidata2'},
        trigger_rule = 'all_success'
    )

    # bulk insert or update aqidata table if necessary
    update_aqidata = PythonOperator(
        task_id = "update_aqidata",
        python_callable = aqi.update_aqi_table,
        trigger_rule = 'all_success'
     )

    # save variable is_empty to show whether meteo_data table is empty
    check_meteo_table = PythonOperator(
        task_id = "check_meteo_table",
        python_callable = check.is_empty,
        op_kwargs = {"table_name":'meteo_data'},
        trigger_rule = 'all_success'
    )

    # bulk insert or update meteo_data table if necessary
    update_meteo_data = PythonOperator(
        task_id = "update_meteo_data",
        python_callable = omf.update_table,
        provide_context = True
     )

    create_task >> check_city_table >> update_cities >> check_meteo_table >> update_meteo_data
    create_task >> aqi_limits
    create_task >> check_city_table >> update_cities >> check_aqi_table >> update_aqidata
    create_task >> check_city_table >> update_cities >> check_country_table >> update_countries

if __name__ == "__main__":
    dag.test()
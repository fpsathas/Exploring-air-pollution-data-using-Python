import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

def is_empty(table_name):

    # checks whether table 'table_name' is empty and pushes the boolean variable

    # connect to db
    hook = PostgresHook("project-db")

    # Define SQL query to count table records
    sql = f"SELECT COUNT(*) FROM {table_name};"

    # Execute query and get record count
    records = hook.get_first(sql)[0]

    # empty will be True, if the table is empty
    empty = records == 0

    print('Checking Complete')

    return empty
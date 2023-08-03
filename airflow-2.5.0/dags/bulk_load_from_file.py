from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
from airflow.hooks.mysql_hook import MySqlHook
    
def get_file_name():
    """Get the user-supplied filename to load into the database from the context"""
    context = get_current_context()
    return context["params"].get("filename", "test_file")

def get_table_name():
    """Get the user-supplied table name"""
    context = get_current_context()
    return context["params"].get("table_name", "test_table")

def bulk_load_sql(**kwargs):
    """Call the hook's bulk_load method to load the file content into the database"""
    conn = MySqlHook(mysql_conn_id='mysql')
    get_filename = get_file_name()
    get_tablename = get_table_name()
    conn.bulk_load(table=get_tablename, tmp_file=get_filename)

"""Define the DAG"""
dag = DAG(
        "bulk_load_from_file",
        start_date=datetime(2023, 7, 1),
        schedule_interval=None)

"""Define the operator that calls the method to bulk load"""
t1 = PythonOperator(
        task_id='bulk_load',
        provide_context=True,
        python_callable=bulk_load_sql,
        dag=dag)

"""Call the operator"""
t1


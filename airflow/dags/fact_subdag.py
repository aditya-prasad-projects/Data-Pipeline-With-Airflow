import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (LoadFactOperator, DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers.sql_queries import SQLQueries
import sql

def get_fact_table_dag(parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        columns,
        append=True,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        *args, **kwargs)
    
    begin_execution_task = DummyOperator(task_id='Begin_execution',  dag=dag)
    
    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SQLQueries.create_load_test_queries[table]["CREATE"]
    )
    
    load_fact_dimension_task = LoadFactOperator(
        task_id=f"load_{table}_fact_table",
        dag=dag,
        redshift_conn_id = redshift_conn_id,
        table = table,
        columns = columns,
        sql_stmt = SQLQueries.create_load_test_queries[table]["LOAD"],
        append=append
    )
    
    data_quality_check_task = DataQualityOperator(
        task_id=f"data_quality_check_{table}_fact_table",
        dag=dag,
        redshift_conn_id = redshift_conn_id,
        table = table,
    )
    
    end_execution_task = DummyOperator(task_id='End_execution',  dag=dag)
    
    begin_execution_task >> create_task
    create_task >> load_fact_dimension_task
    load_fact_dimension_task >> data_quality_check_task
    data_quality_check_task >> end_execution_task
    
    return dag

        


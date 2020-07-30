import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators.dummy_operator import DummyOperator
from helpers.sql_queries import SQLQueries
import sql

def get_dimension_tables_dag(parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        columns,
        append=False,            
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs)
    
    begin_execution_task = DummyOperator(task_id='Begin_execution',  dag=dag)
    
    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SQLQueries.create_load_test_queries[table]["CREATE"]
    )
    
    load_dimension_table_task = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        redshift_conn_id = redshift_conn_id,
        table = table,
        columns =columns,
        sql_stmt = SQLQueries.create_load_test_queries[table]["LOAD"],
        append=append
    )
    
    end_execution_task = DummyOperator(task_id='End_execution',  dag=dag)
    
    begin_execution_task >> create_task
    create_task >> load_dimension_table_task
    load_dimension_table_task >> end_execution_task
    
    return dag
    

    
    
import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import DataQualityOperator
from helpers.sql_queries import SQLQueries
import sql

def run_data_quality_checks_dag(
    parent_dag_name="",
    task_id="",
    redshift_conn_id="",
    tables=[],
    **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs)
    data_quality_tasks = []
    
    begin_execution_task = DummyOperator(task_id='Begin_execution',  dag=dag)
    
    for table in tables:
        data_quality_tasks.append(DataQualityOperator(
            task_id=f"data_quality_check_{table}_dim_table",
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=table))
    
    end_execution_task = DummyOperator(task_id='End_execution',  dag=dag)
    
    for i in range(len(data_quality_tasks)):
        begin_execution_task >> data_quality_tasks[i]
    
    for i in range(len(data_quality_tasks)):
        data_quality_tasks[i] >> end_execution_task
    
        
    return dag
    
            

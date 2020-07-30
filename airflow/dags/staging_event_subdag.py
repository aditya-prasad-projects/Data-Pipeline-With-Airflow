import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)

from helpers.sql_queries import SQLQueries
import sql


# Returns a DAG which creates a staging table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def get_s3_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        table,
        s3_bucket,
        s3_key,
        region,
        json_format,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
    begin_execution_task = DummyOperator(task_id='Begin_execution',  dag=dag)

    create_task = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=SQLQueries.create_load_test_queries[table]["CREATE"]
    )

    copy_task = StageToRedshiftOperator(
        task_id=f"load_{table}_from_s3_to_redshift",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        load_staging_table_sql=SQLQueries.create_load_test_queries[table]["LOAD"],
        region=region,
        json_format=json_format
    )
    
    end_execution_task = DummyOperator(task_id='End_execution',  dag=dag)
    
    begin_execution_task >> create_task
    create_task >> copy_task
    copy_task >> end_execution_task

    return dag

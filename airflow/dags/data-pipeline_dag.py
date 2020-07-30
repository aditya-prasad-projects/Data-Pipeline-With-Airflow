from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from staging_event_subdag import get_s3_to_redshift_dag
from fact_subdag import get_fact_table_dag
from dimension_subdag import get_dimension_tables_dag
from data_quality_check_subdag import run_data_quality_checks_dag
from airflow.operators import DataQualityOperator
from airflow.operators import AnalysisOperator
from helpers.sql_queries import SQLQueries

start_date = datetime.utcnow()

default_args = {
    'owner': 'aditya',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'provide_context': True,
}

dag_name = "sparkify.star.schema"
dag = DAG(
    dag_id = dag_name,
    start_date=start_date,
    default_args=default_args
)

begin_execution_task = DummyOperator(task_id='Begin_execution',  dag=dag)

staging_events_task_id="log_staging_events"
staging_events_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        parent_dag_name=dag_name,
        task_id = staging_events_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket = "udacity-dend",
        s3_key = "log_data",
        region = "us-west-2",
        json_format = "s3://udacity-dend/log_json_path.json",
        start_date=start_date
    ),
    task_id=staging_events_task_id,
    dag=dag
)
    
staging_songs_task_id="songs_staging_songs"
staging_songs_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        parent_dag_name=dag_name,
        task_id = staging_songs_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket = "udacity-dend",
        s3_key = "song_data",
        region = "us-west-2",
        json_format = "auto",
        start_date=start_date
    ),
    task_id=staging_songs_task_id,
    dag=dag
)
    
load_songplays_fact_table_task_id="load_songplays_fact_table"
load_songplays_fact_table_task = SubDagOperator(
    subdag=get_fact_table_dag(
        parent_dag_name=dag_name,
        task_id=load_songplays_fact_table_task_id,
        redshift_conn_id="redshift",
        table="songplays",
        columns="""
            songplay_id,
            start_time,
            userid,
            level,
            songid,
            artistid,
            sessionid,
            location,
            user_agent
        """,
        start_date=start_date
    ),
    task_id=load_songplays_fact_table_task_id,
    dag=dag
)

load_songs_dim_table_task_id="load_songs_dim_table"
load_songs_dim_table_task = SubDagOperator(
    subdag=get_dimension_tables_dag(
        parent_dag_name = dag_name,
        task_id=load_songs_dim_table_task_id,
        redshift_conn_id = "redshift",
        table="songs",
        columns="""
            songid,
            title,
            artistid,
            year,
            duration
        """,
        start_date=start_date
    ),
    task_id=load_songs_dim_table_task_id,
    dag=dag
)

load_artists_dim_table_task_id="load_artists_dim_table"
load_artists_dim_table_task = SubDagOperator(
    subdag=get_dimension_tables_dag(
        parent_dag_name = dag_name,
        task_id=load_artists_dim_table_task_id,
        redshift_conn_id = "redshift",
        table="artists",
        columns="""
            artistid,
            name,
            location,
            lattitude,
            longitude
        """,
        start_date=start_date
    ),
    task_id=load_artists_dim_table_task_id,
    dag=dag
)

load_users_dim_table_task_id="load_users_dim_table"
load_users_dim_table_task = SubDagOperator(
    subdag=get_dimension_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_users_dim_table_task_id,
        redshift_conn_id="redshift",
        table="users",
        columns="""
            userid,
            first_name,
            last_name,
            gender,
            level
        """,
        start_date=start_date
    ),
    task_id=load_users_dim_table_task_id,
    dag=dag
)

load_time_dim_table_task_id="load_time_dim_table"
load_time_dim_table_task = SubDagOperator(
    subdag=get_dimension_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_time_dim_table_task_id,
        redshift_conn_id="redshift",
        table="time",
        columns="""
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        """,
        start_date=start_date
    ),
    task_id=load_time_dim_table_task_id,
    dag=dag
)

run_data_quality_checks_task_id="run_data_quality_checks"
run_data_quality_checks_task = SubDagOperator(
    subdag=run_data_quality_checks_dag(
        parent_dag_name=dag_name,
        task_id=run_data_quality_checks_task_id,
        redshift_conn_id="redshift",
        tables=["songs","artists","users","time"],
        start_date=start_date
    ),
    task_id=run_data_quality_checks_task_id,
    dag=dag
)

user_song_popularity_summary_task = AnalysisOperator(
    task_id="user_song_popularity_analysis_summary",
    dag=dag,
    redshift_conn_id = "redshift",
    destination_table = "user_song_popularity_summary",
    sql_query=SQLQueries.analysis_sql["USER_SONG_POPULARITY_SUMMARY"]
)

user_song_popularity_indepth = AnalysisOperator(
    task_id="user_song_popularity_analysis_indepth",
    dag=dag,
    redshift_conn_id = "redshift",
    destination_table="user_song_popularity_indepth",
    sql_query=SQLQueries.analysis_sql["USER_SONG_POPULARITY_INDEPTH"]
)


end_execution_task = DummyOperator(task_id='End_execution',  dag=dag)
    
begin_execution_task >> staging_events_task
begin_execution_task >> staging_songs_task
staging_events_task >> load_songplays_fact_table_task
staging_songs_task >> load_songplays_fact_table_task
load_songplays_fact_table_task >> load_songs_dim_table_task 
load_songplays_fact_table_task >> load_artists_dim_table_task
load_songplays_fact_table_task >> load_users_dim_table_task
load_songplays_fact_table_task >> load_time_dim_table_task
load_songs_dim_table_task >> run_data_quality_checks_task
load_artists_dim_table_task >> run_data_quality_checks_task
load_users_dim_table_task >> run_data_quality_checks_task
load_time_dim_table_task >> run_data_quality_checks_task

run_data_quality_checks_task >> user_song_popularity_summary_task
run_data_quality_checks_task >> user_song_popularity_indepth
user_song_popularity_summary_task >> end_execution_task
user_song_popularity_indepth >> end_execution_task

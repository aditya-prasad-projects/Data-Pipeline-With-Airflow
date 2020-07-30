from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SQLQueries
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator Started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        test_queries = SQLQueries.create_load_test_queries[self.table]["TEST"]
        for i in range(len(test_queries)):
            query = test_queries[i][0]
            records = redshift_hook.get_records(query)
            if(records[0][0] > test_queries[i][1]):
                raise ValueError(f"Data quality check failed. {self.table} failed for {query} as it returned {records[0][0]} NULL values")
            logging.info(f"Data quality check on table {self.table} passed for {query}")
        
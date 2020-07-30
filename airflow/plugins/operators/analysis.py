from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class AnalysisOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_query="",
                 *args, **kwargs):

        super(AnalysisOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_query = sql_query
        
    def execute(self, context):
        self.log.info('AnalysisOperator has started')
        # obtain the redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)
        formatted_sql = self.sql_query.format(destination_table=self.destination_table)
        logging.info("Running analysis using: {}".format(formatted_sql))
        redshift_hook.run(formatted_sql)
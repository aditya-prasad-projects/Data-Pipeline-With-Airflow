from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SQLQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_dimension_sql = """
        INSERT INTO {} {} {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns="",
                 sql_stmt="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        self.log.info('LoadDimensionOperator has started')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = "({})".format(self.columns)
        if self.append == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift_hook.run(SQLQueries.truncate_sql.format(table=self.table))
        load_sql = LoadDimensionOperator.load_dimension_sql.format(
            self.table,
            columns,
            self.sql_stmt
            )
        redshift_hook.run(load_sql)

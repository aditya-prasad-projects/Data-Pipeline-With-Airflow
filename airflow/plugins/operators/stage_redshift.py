from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SQLQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_format="",
                 load_staging_table_sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.load_staging_table_sql = load_staging_table_sql
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is now in progress')

        # get the hooks
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift_hook.run(SQLQueries.truncate_sql.format(table=self.table))

        # as we are providing_context = True, we get them in kwargs form
        # use **context to upack the dictionary and format the s3_key
        rendered_key = self.s3_key.format(**context)

        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        sql_stmt = self.load_staging_table_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
        redshift_hook.run(sql_stmt)






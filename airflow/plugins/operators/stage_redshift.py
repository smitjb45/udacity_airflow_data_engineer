from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    tempale_fields=("s3_key",)
    
    copy_sql = """
        COPY = {}
        FROM = '{}'
        ACCESS_KEY_ID = '{}'
        SECRET_ACCESS_KEY = '{}'
        {} '{}'
        REGION '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 sql_create,
                 table="",
                 s3_bucket="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 json_command="",
                 json_argument="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
            self.sql_create = sql_create
            self.table = table
            self.s3_bucket = s3_bucket
            self.redshift_conn_id = redshift_conn_id
            self.aws_credentials_id = aws_credentials_id
            self.json_command = json_command
            self.json_argument = json_argument
            self.region = region

    def execute(self, context):
        self.log.info('StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Recreating table from destination Redshift table")
        redshift.run(self.sql_create)

        self.log.info("Copying data from S3 to Redshift")
#         rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}".format(self.s3_bucket)
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
            self.json_command,
            self.json_argument,
            self.region
        )
        redshift.run(formatted_sql)






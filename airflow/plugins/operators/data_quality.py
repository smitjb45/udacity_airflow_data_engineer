from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 redshift_conn_id=""
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
            self.tables = tables
            self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator Starting')
        
        for table in tables:
            redshift_hook = PostgresHook("redshift")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
            else:
                self.log.info(f"DataQualityOperator Successful Transfer of Records to {table}")
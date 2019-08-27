from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table = "",
                 sql_create = "",
                 sql_query = "",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
            self.table = table
            self.sql_create = sql_create
            self.sql_query = sql_query
            self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator')
   
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Recreating {} fact tables in redshift").format(self.table)
        redshift.run(self.sql_create)

        self.log.info("inserting Redshift table {}").format(self.table)
        redshift.run(self.sql_query)
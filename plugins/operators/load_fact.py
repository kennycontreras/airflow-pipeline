from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(self.postgres_conn_id)
        self.log.info('Loading fact table')

        # Insert statement. Fact table will allow insert mode and not truncate-insert or delete-insert
        insert_sql = """
                INSERT INTO {}
                {}
        """.format(self.table, self.sql_query)

        redshift.run(insert_sql)

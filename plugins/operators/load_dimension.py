from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 table='',
                 sql_query='',
                 load_mode='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sql_query = sql_query
        self.load_mode = load_mode

    def execute(self, context):

        redshift = PostgresHook(self.postgres_conn_id)
        # truncate statement
        truncate_sql = """
                    TRUNCATE {};
                """.format(self.table)

        # insert statement
        insert_sql = """
                    INSERT INTO {}
                    {};
                """.format(self.table, self.sql_query)

        # Dimension tables allow truncate mode.
        if self.load_mode == 'truncate':

            self.log.info('Truncating and loading dimesion table')
            # Run truncate first and then insert statement
            redshift.run(truncate_sql + '\n' + insert_sql)

        else:
            self.log.info('Loading dimesion table')
            # Run insert statement
            redshift.run(insert_sql)

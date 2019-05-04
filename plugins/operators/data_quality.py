from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 sql_queries='',
                 expected_result='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_queries = sql_queries
        self.expected_result = expected_result

    def execute(self, context):
        redshift = PostgresHook(self.postgres_conn_id)

        # For statement to iterate over queries and conditions
        for query, condition in zip(self.sql_queries, self.expected_result):
            # Execute query and save result
            count = redshift.get_records(query)

            # Validate results
            if count[0][0] > condition:
                raise ValueError("Data Quality check failed. Expected condition: (), Result on table was: {}".format(
                    condition, count[0][0]))
            else:
                self.log.info("Data Quality check passed Successfully. Expected condition: {}, Result on table was: {}".format(
                    condition, count[0][0]))

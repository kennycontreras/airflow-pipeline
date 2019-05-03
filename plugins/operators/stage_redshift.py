from datetime import datetime
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 s3_key='',
                 s3_bucket='',
                 table='',
                 copy_sql='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.table = table
        self.copy_sql = copy_sql

    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        execution_date = context['execution_date']

        self.log.info('clearing data from destination Redshift table')
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info('Copying data from S3 to Redshift')
        rendered_key = self.s3_key.format(**context)

        s3_path = 's3://{}/{}/{}-{}-{}'.format(self.s3_bucket,  rendered_key,
                                               execution_date.year, execution_date.month, execution_date.day)

        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
        )

        redshift.run(formatted_sql)

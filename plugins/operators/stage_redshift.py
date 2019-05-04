from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ('s3_key',)

    copy_cmd = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
        """

    json_source = "json 'auto'"

    csv_source = """
            IGNOREHEADER {}
            DELIMITER {}
        """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 s3_key='',
                 s3_bucket='',
                 table='',
                 ignore_header=1,
                 delimiter=',',
                 file_extension='csv',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.table = table
        self.ignore_header = ignore_header
        self.delimiter = delimiter
        self.file_extension = file_extension

    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket,  rendered_key)

        self.log.info('Delete data from Staging Tables')
        redshift.run('DELETE FROM {}'.format(self.table))

        self.log.info('Copying data from S3 to Redshift')

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key) + "\n" + StageToRedshiftOperator.csv_source.format(
            self.ignore_header,
            self.delimiter) + "\n" + self.file_extension

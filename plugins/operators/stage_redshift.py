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

    csv_source = """
            IGNOREHEADER {}
            DELIMITER '{}'
            """

    json_source = "json 'auto'"

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

        # AwsHook for credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Generate s3 path for files
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket,  rendered_key)

        # Truncate stage tables
        self.log.info('Truncate Staging Table')
        redshift.run('TRUNCATE {}'.format(self.table))

        self.log.info('Copying data from S3 to Redshift')

        if self.file_extension == 'csv':

            # Formatted sql for CSV files.
            formatted_sql = StageToRedshiftOperator.copy_cmd.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key) + StageToRedshiftOperator.csv_source.format(
                self.ignore_header,
                self.delimiter) + self.file_extension
        else:
            # Formatted sql for Json files
            formatted_sql = StageToRedshiftOperator.copy_cmd.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key) + StageToRedshiftOperator.json_source

        # Execute formatted sql for csv/json
        redshift.run(formatted_sql)

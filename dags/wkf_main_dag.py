from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.local_executor import LocalExecutor
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from wkf_subdag import load_dimension_table_dag


from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 11, 1),
    'end_date:': datetime(2018, 11, 30),
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}


dag = DAG('workflow_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1,
          )

dimension_task_id = "Dimension_tables_subdag"
dimension_subdag_task = SubDagOperator(
    subdag=load_dimension_table_dag(
        "workflow_pipeline",
        dimension_task_id,
        "redshift",
        ['users', 'songs', 'artists', 'time'],
        [SqlQueries.user_table_insert, SqlQueries.song_table_insert,
            SqlQueries.artist_table_insert, SqlQueries.time_table_insert],
        "truncate",
        dag.schedule_interval,
        start_date=default_args['start_date']
    ),
    task_id=dimension_task_id,
    default_args=default_args,
    executor=LocalExecutor(),
    dag=dag
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_key='log_data/{{ ds }}-events.csv',
    s3_bucket='dend',
    table='staging_events'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_key='song_data/',
    s3_bucket='dend',
    table='staging_songs',
    file_extension='json'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id='redshift',
    sql_queries=['select count(1) from public."artists" where artistid is null ',
                 'select count(1) from public."songplays" where userid is null'],
    expected_result=[0, 0]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> dimension_subdag_task >> run_quality_checks
run_quality_checks >> end_operator

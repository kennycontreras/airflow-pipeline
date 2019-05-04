from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 11, 1),
    'end_date:': datetime(2018, 11, 30),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('workflow_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1,
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    s3_key='log_data/{{ ds }}-event.csv',
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

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    table='users',
    sql_query=SqlQueries.user_table_insert,
    load_mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    table='songs',
    sql_query=SqlQueries.song_table_insert,
    load_mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    table='artists',
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    table='time',
    sql_query=SqlQueries.time_table_insert,
    load_mode='truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Graph View
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator

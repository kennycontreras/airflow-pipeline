from datetime import datetime

from airflow import DAG
from airflow.operators import LoadDimensionOperator
from airflow.operators.dummy_operator import DummyOperator

def load_dimension_table_dag(
        parent_dag_name,
        task_id,
        conn_id,
        table_list,
        sql_query_list,
        load_mode,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}", **kwargs
    )
    
    start_operator = DummyOperator(task_id='Begin_subdag_execution',  dag=dag)
    
    
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_{}_dim_table'.format(table_list[0]),
        dag=dag,
        postgres_conn_id=conn_id,
        table=table_list[0],
        sql_query=sql_query_list[0],
        load_mode=load_mode
    )
    
    
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_{}_dim_table'.format(table_list[1]),
        dag=dag,
        postgres_conn_id=conn_id,
        table=table_list[1],
        sql_query=sql_query_list[1],
        load_mode=load_mode
    )
    
    
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_{}_dim_table'.format(table_list[2]),
        dag=dag,
        postgres_conn_id=conn_id,
        table=table_list[2],
        sql_query=sql_query_list[2],
        load_mode=load_mode
    )
    
    
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_{}_dim_table'.format(table_list[3]),
        dag=dag,
        postgres_conn_id=conn_id,
        table=table_list[3],
        sql_query=sql_query_list[3],
        load_mode=load_mode
    )
    
    end_operator = DummyOperator(task_id='End_subdag_execution',  dag=dag)
    
    start_operator >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> end_operator
    
    return dag
    
    
    
    
    
    
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
json_path = 's3://udacity-dend/log_json_path.json'

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),  # datetime(2019, 1, 12) 
    'depends_on_past': False,
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False   
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    region='us-west-2',
    json_path=json_path,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    region='us-west-2',
    json_path='auto',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert
)

check_stms = [{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
           {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    check_stms=check_stms
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# task dependencies
start_operator >> create_table
create_table >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [
    load_song_dimension_table, 
    load_user_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table
    ] >> run_quality_checks
run_quality_checks >> end_operator

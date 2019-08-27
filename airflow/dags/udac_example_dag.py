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
    'owner': 'udacity',
    'depends_on_past': False,
    'catchup_by_default' = False,
    'start_date': datetime(2019, 1, 12),
    'email': ['jsmithwoodworm@yahoo.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
    sql_create = SqlQueries.staging_events_table_create,
    table="staging_events",
    s3_bucket="s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    json_command="FORMAT AS JSON",
    json_argument="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
    sql_create = SqlQueries.staging_songs_table_create,
    table="staging_songs",
    s3_bucket="s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    json_command="json",
    json_argument="auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
    table = "songplays",
    sql_create = SqlQueries.songplay_table_create,
    sql_query = SqlQueries.songplay_table_insert,
    redshift_conn_id = "redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
    table = "users",
    sql_create = SqlQueries.users_table_create,
    sql_query = SqlQueries.users_table_insert,
    redshift_conn_id = "redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
    table = "songs",
    sql_create = SqlQueries.songs_table_create,
    sql_query = SqlQueries.songs_table_insert,
    redshift_conn_id = "redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
    table = "artists",
    sql_create = SqlQueries.artists_table_create,
    sql_query = SqlQueries.artists_table_insert,
    redshift_conn_id = "redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
    table = "times",
    sql_create = SqlQueries.times_table_create,
    sql_query = SqlQueries.times_table_insert,
    redshift_conn_id = "redshift"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
    table = ["songplays", "users", "songs", "artists", "times"],
    redshift_conn_id = "redshift"
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#################################
## Start Airflow Stages
#################################

# Stage 1
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Stage 2
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Stage 3
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Stage 4
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Stage 5
run_quality_checks >> end_operator




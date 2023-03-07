import os
import pendulum
from pendulum  import datetime, duration

from airflow.decorators import dag,task
from airflow.operators.dummy_operator import DummyOperator 
from datetime import timedelta

from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator

from airflow.operators.postgres_operator import PostgresOperator 

from sql_commands.sql_queries import SqlQueries
from sql_commands import create_tables

default_args = {
    "owner":"udacity",
    "depens_on_past":False,
    "email_on_failure":False,
    "retires":1,
    "retry_delay": duration(minutes=1),
    "catchup":False,
}
@dag(
    start_date = pendulum.now(),
    schedule_interval='@hourly',
    default_args = default_args
)
def sparkify_pipeline():
    
    start_operator = DummyOperator(
        task_id="Begin_execution"
    )
    
    #Load Data from S3 to redshift
    create_events_table = PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id="sparkify_redshift",
        sql=create_tables.CREATE_STAGING_EVENTS_TABLE_SQL
    )
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id="aws_credentials",
        redshift_conn_id="sparkify_redshift",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key="log_data",
        region = "us-west-2",
        json_format='s3://udacity-dend/log_json_path.json'
    )

    create_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="sparkify_redshift",
        sql=create_tables.CREATE_STAGING_SONGS_TABLE_SQL
    )
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        aws_credentials_id = "aws_credentials",
        redshift_conn_id = "sparkify_redshift",
        table = "staging_songs",
        s3_bucket = "sparkify-proj",
        s3_key = "song_data",
        region = "us-east-1",
    )
    # load fact tables
    create_songplays_fact_table = PostgresOperator(
        task_id = "create_songplays_fact_table",
        postgres_conn_id="sparkify_redshift",
        sql= create_tables.CREATE_FACT_SONGPLAYS_TABLE_SQL
    )
    load_songplays_fact_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id = "sparkify_redshift",
        table="fact_songplays",
        sql_query= SqlQueries.songplay_table_insert
    )
    #Load dimension tables
    create_users_dim_table = PostgresOperator(
        task_id="create_users_dim_table",
        postgres_conn_id="sparkify_redshift",
        sql = create_tables.CREATE_DIM_USERS_TABLE_SQL
    )
    load_users_dimension_table = LoadDimensionOperator(
        task_id="Load_users_dim_table",
        redshift_conn_id = "sparkify_redshift",
        table = "dim_users",
        sql_query= SqlQueries.user_table_insert
    )

    create_songs_dim_table = PostgresOperator(
        task_id="create_songs_dim_table",
        postgres_conn_id="sparkify_redshift",
        sql = create_tables.CREATE_DIM_SONGS_TABLE_SQL
    )
    load_songs_dimension_table = LoadDimensionOperator(
        task_id="Load_songs_dim_table",
        redshift_conn_id = "sparkify_redshift",
        table= "dim_songs",
        sql_query = SqlQueries.song_table_insert
    )
    
    create_artists_dim_table = PostgresOperator(
        task_id = "create_artists_dim_table",
        postgres_conn_id = "sparkify_redshift",
        sql = create_tables.CREATE_DIM_ARTISTS_TABLE_SQL
    )
    
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="sparkify_redshift",
        table="dim_artists",
        sql_query= SqlQueries.artist_table_insert
    )
    
    create_time_dim_table = PostgresOperator(
        task_id= "create_time_dim_table",
        postgres_conn_id = "sparkify_redshift",
        sql = create_tables.CREATE_DIM_TIME_TABLE_SQL
    )
    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id = "sparkify_redshift",
        table="dim_time",
        sql_query= SqlQueries.time_table_insert
    )

    #data quality
    data_quality_checks = DataQualityOperator(
        task_id="Data_quality_check",
        redshift_conn_id= "sparkify_redshift",
        tables=["dim_users","dim_songs","dim_artists","dim_time"]
    )
    end_operator = DummyOperator(
        task_id="Stop_execution"
    )
    
    start_operator >> create_events_table >> stage_events_to_redshift
    start_operator >> create_songs_table >> stage_songs_to_redshift 
    
    start_operator >> create_songplays_fact_table >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table 
    
    start_operator >> create_time_dim_table
    load_songplays_fact_table >> load_time_dimension_table >> data_quality_checks >> end_operator
    
    start_operator >> create_users_dim_table
    stage_events_to_redshift >> load_users_dimension_table >> data_quality_checks >> end_operator
    
    start_operator >> create_songs_dim_table
    stage_songs_to_redshift >> load_songs_dimension_table >>  data_quality_checks >> end_operator

    start_operator >> create_artists_dim_table 
    stage_songs_to_redshift >> load_artist_dimension_table >> data_quality_checks >> end_operator
    
    
sparkify_pipeline_dag=sparkify_pipeline()

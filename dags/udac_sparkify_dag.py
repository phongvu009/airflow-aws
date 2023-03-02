import os
import pendulum

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
@dag(
    
    start_date = pendulum.now(),
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)
def sparkify_pipeline():
    
    start_operator = DummyOperator(
        task_id="Begin_execution"
    )
    
    #Load Data from S3 to redshift
    create_events_table = PostgresOperator(
        task_id='create_events_table',
        postgres_conn_id="sparkify_redshift",
        sql=create_tables.CREATE_EVENTS_TABLE_SQL
    )
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        aws_credentials_id="aws_credentials",
        redshift_conn_id="sparkify_redshift",
        table="staging_events",
        s3_bucket="sparkify-proj",
        s3_key="log_data"
    )

    # create_songs_table = PostgresOperator(
    #     task_id="create_songs_table"
    # )
    # stage_songs_to_redshift = StageToRedshiftOperator(
    #     task_id="Stage_songs"
    # )
    #load fact tables
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table"
    )
    #Load dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table"
    )
    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table"
    )
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table"
    )
    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table"
    )

    #data quality
    stage_events_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks_on_stage_events",
        redshift_conn_id = "sparkify_redshift",
        table="stage_events"
    )

    end_operator = DummyOperator(
        task_id="Stop_execution"
    )
    
    start_operator >> create_events_table >> stage_events_to_redshift >> stage_events_quality_checks

sparkify_pipeline_dag=sparkify_pipeline()

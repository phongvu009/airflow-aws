import os
import pendulum

from airflow.decorators import dag,task
from airflow.operators.dummy_operator import DummyOperator 


from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator


from sql_commands.sql_queries import SqlQueries

@dag(
    start_date = pendulum.now()
)
def sparkify_pipeline():
    
    start_operator = DummyOperator(
        task_id="Begin_execution"
    )
    #Load Data to redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs"
    )
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
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
    )

    end_operator = DummyOperator(
        task_id="Stop_execution"
    )

sparkify_pipeline_dag=sparkify_pipeline()

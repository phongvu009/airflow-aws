import os
import pendulum
from datetime import datetime,timedelta 
from airflow import dag,task
from airflow.operators.dummy_operator import DummyOperator 
from aiflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                              LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries 

@dag(
    start_date = pendulum.now()
)
def sparkify_pipeline():
    
    start_operator = DummyOperator(
        task_id="Begin_execution"
    )

    end_operator = DummyOperator(
        task_id="Stop_execution"
    )

sparkify_pipeline_dag=sparkify_pipeline()

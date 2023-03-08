from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    @apply_defaults 
    def __init__(self,
                redshift_conn_id="",
                tables=[],
                *args, **kwargs):
        super(DataQualityOperator,self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self,context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Connected to redshift")
        if not self.tables:
            raise ValueError(f"this can not be empty")
        for self.check_table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {self.check_table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.check_table} return no results")
            num_records = records[0][0]
            if num_records < 1 :
                raise ValueError(f"Data quality check failed. {self.check_table} contains 0 rows")
            logging.info(f"Data Quality on table {self.check_table} check passed with {records[0][0]} records")
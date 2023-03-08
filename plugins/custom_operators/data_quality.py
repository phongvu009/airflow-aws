from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    @apply_defaults 
    def __init__(self,
                redshift_conn_id="",
                dq_checks=[],
                *args, **kwargs):
        super(DataQualityOperator,self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self,context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Connected to redshift")

        self.log.info("Testing begins")
        for i,dq_check in enumerate(self.dq_checks) :
            records = redshift.get_records[dq_check["test_sql"]]
            result = records[0][0]
            if dq_check['op'] == "=":
                if result != dq_check['expected_result'] :
                    raise ValueError(f"Data quality check #{i} failed. {result} not as same as {dq_check['expected_result']} ")
            elif dq_check['op'] == ">":
                if result <= dq_check['expected_result']:
                    raise ValueError(f"Data quality check #{i} failed. {result} is not {dq_check['op']} {dq_check['expected_result']}")
            self.log.info(f"Passed check : {result} {dq_check['op']} {dq_check['expected_result']}")
            
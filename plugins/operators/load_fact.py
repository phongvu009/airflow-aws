
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from aiflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    pass
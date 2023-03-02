from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    insert_sql_template = """
        TRUNCATE TABLE {table};
        INSERT INTO {table} ({sql_query})
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args,**kwargs
                 ):
        super(LoadDimensionOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
    
    def execute(self,context):
        redshift = PostgresHook(self.redshift_conn_id)
        insert_sql = LoadDimensionOperator.insert_sql_template.format(
            table=self.table,
            sql_query=self.sql_query
        )
        #remove all records before doing insert. This help to remove duplicate data
        redshift.run(insert_sql)
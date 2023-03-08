from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    insert_sql_template = """
        INSERT INTO {table} ({sql_query})
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append_only = False,
                 *args,**kwargs
                 ):
        super(LoadDimensionOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_only = append_only
    
    def execute(self,context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Connected to redshift")
        if not self.append_only:
            redshift.run(f"TRUNCATE TABLE {self.table}")
            insert_sql = LoadDimensionOperator.insert_sql_template.format(
                table=self.table,
                sql_query=self.sql_query
            )
            redshift.run(insert_sql)           
        else:
            insert_sql = LoadDimensionOperator.insert_sql_template.format(
                table=self.table,
                sql_query=self.sql_query
            )
            redshift.run(insert_sql)
        self.log.info("Succes on running the insert query")


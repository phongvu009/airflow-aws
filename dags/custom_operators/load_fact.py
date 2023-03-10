
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_sql_template = """
        TRUNCATE TABLE {table};
        INSERT INTO {table} ({sql_query})
    """
    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table = "",
                sql_query = "",
                *args, **kwargs
                 ):
        super(LoadFactOperator,self).__init__(*args,**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        

    def  execute(self,context):
        self.log.info("Connecting to redshift")
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info("Connected to redshift")
        insert_sql = LoadFactOperator.insert_sql_template.format(
            table=self.table,
            sql_query=self.sql_query,
        )
        redshift.run(insert_sql)
        self.log.info("Succes on running the insert query")
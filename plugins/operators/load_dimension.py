from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 dim_table="",
                 sql_statement="",
                 delete_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table=dim_table
        self.sql_statement=sql_statement
        self.delete_load=delete_load

    def execute(self, context):
        self.log.info('LoadDimensionOperator: Started task ')
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if(self.delete_load):
            hook.run("TRUNCATE TABLE {}".format(self.dim_table))
            self.log.info("Truncated dimension table: {}".format(self.dim_table))

        self.log.info(f"Loading data into dimension table {self.dim_table}")
        insert_sql_statement="INSERT INTO {} {};".format(self.dim_table,self.sql_statement)
        hook.run(insert_sql_statement)
        self.log.info("Inserted data into {}".format(self.dim_table))


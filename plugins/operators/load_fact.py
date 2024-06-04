from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 fact_table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table=fact_table
        self.sql_statement=sql_statement

    def execute(self, context):
        self.log.info('LoadFactOperator: Started task ')
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into fact table {self.fact_table}")
        insert_sql_statement="INSERT INTO {} {};".format(self.fact_table,self.sql_statement)
        hook.run(insert_sql_statement)
        self.log.info("Inserted data into {}".format(self.fact_table))

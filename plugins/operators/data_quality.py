from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables=tables
        

    def execute(self, context):
        self.log.info('DataQualityOperator: Started task ')
        hook=PostgresHook(self.redshift_conn_id)

        for table_x in self.tables:
            data= hook.get_records("select count(*) from {}".format(table_x))
            if(len(data)<1 or len(data[0])<1 or data[0][0]==0):
                self.log.error("There are no records present in {}".format(table_x))
                raise ValueError("There are no records present in {}".format(table_x))
            self.log.info("{} has passed data quality checks. It contain {} records.".format(table_x,data[0][0]))


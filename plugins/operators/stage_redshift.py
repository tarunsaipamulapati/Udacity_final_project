from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 des_table_name="",
                 s3_bucket = "",
                 s3_key = "",
                 region="",
                 extra_parameters="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.des_table_name = des_table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.extra_parameters = extra_parameters

    def execute(self, context):
        self.log.info('StageToRedshiftOperator: Started task ')
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Copying data from S3 to Redshift staging {} table".format(self.des_table_name))
        rendered_s3_key  = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket,rendered_s3_key)
        final_stage_sql="COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' REGION AS '{}' {};".format(
            self.des_table_name, 
            s3_path, 
            aws_credentials.access_key, 
            aws_credentials.secret_key,
            self.region,
            self.extra_parameters
            )
        self.log.info("Copying data from '{}' to '{}'".format(s3_path,self.des_table_name))
        redshift_hook.run(final_stage_sql)

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        JSON 'auto'
        COMPUPDATE OFF
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region = '',
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
                
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Copying data from S3 to Redhift')
        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key.format(**context))
        json_path = 's3://udacity-dend/log_json_path.json'
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        
        redshift.run(formatted_sql)
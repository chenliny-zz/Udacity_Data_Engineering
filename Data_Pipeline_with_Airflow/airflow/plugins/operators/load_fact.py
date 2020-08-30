from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clearing data from fact table {}'.format(self.table))
        redshift_hook.run('DELETE FROM {}'.format(self.table))
        
        self.log.info('Loading data into fact table in Redshift')
        
        redshift_hook.run(self.sql)

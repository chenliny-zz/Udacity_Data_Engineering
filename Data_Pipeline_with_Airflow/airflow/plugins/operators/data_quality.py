from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 check_stms = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_stms = check_stms
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for check in self.check_stms:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            
            records = redshift_hook.get_records(sql)[0]
            
            if exp_result != records[0]:
                raise ValueError('Data quality check failed. Expected: {} | Got: {}'.format(exp_result, sql))
            else:
                self.log.info('Data quality on SQL {} check passed with {} records'.format(sql, records[0]))
 
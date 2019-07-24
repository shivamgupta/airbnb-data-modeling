from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extras import execute_values

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql="",
                 *args, **kwargs):
        """
        :param redshift_conn_id: RedShift Connection ID
        :param aws_credentials_id: AWS Credentials ID
        :param sql: SQL Query for loading a dimension table
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql                = sql
        
    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # RedShift Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Populate table
        redshift.run(self.sql)
        self.log.info("Inserted data into the dimension table")

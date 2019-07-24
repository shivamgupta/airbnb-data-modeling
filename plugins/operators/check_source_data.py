import boto3
import json
import pandas as pd
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CheckSourceOperator(BaseOperator):
    
    ui_color = '#FF9E7B'
    
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):
        """
        :param aws_credentials_id: AWS Credentials ID
        :param s3_bucket: Name of the S3 Bucket
        :param s3_key: Key for partitioning
        """

        super(CheckSourceOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket          = s3_bucket
        self.s3_key             = s3_key
            
            
    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info("Checking if {} exists".format(s3_path))
       
        # Get the Data
        client = boto3.client('s3',
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key
                     )
        result = client.get_object(Bucket=self.s3_bucket, Key=rendered_key) 
        text = result["Body"].read().decode()
        data = json.loads(text)
        
        # Make a DataFrame with the received data
        df = pd.DataFrame(data)
        
        # Print number of records in the DataFrame
        self.log.info("Found {} records in {}".format(df.shape[0], s3_path))
        
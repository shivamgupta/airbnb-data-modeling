import boto3
import json
import pandas as pd
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CleanSourceOperator(BaseOperator):
    
    ui_color = '#1CA9FF'
    
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 s3_temp_file_store="",
                 *args, **kwargs):
        """
        :param aws_credentials_id: AWS Credentials ID
        :param s3_bucket: Name of the S3 Bucket
        :param s3_key: Key for partitioning
        :param s3_temp_file_store: Path to temperory data store after cleaning
        """

        super(CleanSourceOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket          = s3_bucket
        self.s3_key             = s3_key
        self.s3_temp_file_store = s3_temp_file_store
            
            
    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info("Cleaning data of {}".format(s3_path))
       
        # Get the Data
        client = boto3.client('s3',
                       aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key
                     )
        result = client.get_object(Bucket=self.s3_bucket, Key=rendered_key) 
        text = result["Body"].read().decode()
        text = text.replace('|', '')
        data = json.loads(text)
        
        raw_data = []
        for row in data:
            raw_data.append(row["fields"])
        
        # Make a DataFrame with the received data
        df = pd.DataFrame(raw_data)
        self.log.info("Found {} records in {}".format(df.shape[0], s3_path))
        
        # Clean the Data
        if 'listings' in rendered_key:
            clean_df = CleanSourceOperator.clean_listings_data(self, df)
        if 'stays' in rendered_key:
            clean_df = CleanSourceOperator.clean_stays_data(self, df)
            
        # Save the cleaned DataFrame to S3
        rendered_s3_temp_file_store = self.s3_temp_file_store.format(**context)
        s3_temp_file_path = "s3://{}/{}".format(self.s3_bucket, rendered_s3_temp_file_store)
        
        self.log.info("Storing cleaned data in {}".format(s3_temp_file_path))
        client.put_object(Body=clean_df.to_csv(index=False, sep='|'), Bucket=self.s3_bucket, Key=rendered_s3_temp_file_store)
        self.log.info("Stored cleaned data in {}".format(s3_temp_file_path))
            
    def clean_listings_data(self, listings_df):
        # Rename desired columns
        df = listings_df.rename(columns= {"id": "listing_id", "name": "listing_title"})
        
        # Drop duplicates
        count_before_drop = df.shape[0]
        df.drop_duplicates(subset="listing_id", keep=False, inplace=True)
        count_after_drop = df.shape[0]
        self.log.info("Dropped {} duplicate records".format(count_before_drop-count_after_drop))
        
        # Drop records where listing_id or host_id is NULL or empty string
        df[df.listing_id != '']
        df[df.host_id != '']
        count_after_cleansing = df.shape[0]
        self.log.info("Dropped {} null records".format(count_after_drop-count_after_cleansing))
        
        # Drop columns not of our interest
        df = df.drop([                                  \
                    'review_scores_accuracy',           \
                    'geolocation',                      \
                    'features',                         \
                    'transit',                          \
                    'calendar_last_scraped',            \
                    'review_scores_communication',      \
                    'longitude',                        \
                    'country_code',                     \
                    'review_scores_cleanliness',        \
                    'neighborhood_overview',            \
                    'market',                           \
                    'space',                            \
                    'picture_url',                      \
                    'review_scores_value',              \
                    'latitude',                         \
                    'review_scores_checkin',            \
                    'review_scores_location',           \
                    'host_picture_url',                 \
                    'description',                      \
                    'experiences_offered',              \
                    'extra_people',                     \
                    'smart_location',                   \
                    'xl_picture_url',                   \
                    'host_thumbnail_url',               \
                    'scrape_id',                        \
                    'review_scores_rating',             \
                    'calculated_host_listings_count',   \
                    'medium_url',                       \
                    'calendar_updated',                 \
                    'summary',                          \
                    'thumbnail_url',                    \
                    'last_scraped',                     \
                    'guests_included',                  \
                    'host_total_listings_count',        \
                    'house_rules',                      \
                    'access',                           \
                    'host_about',                       \
                    'host_neighbourhood',               \
                    'interaction',                      \
                    'monthly_price',                    \
                    'weekly_price',                     \
                    'square_feet',                      \
                    'neighbourhood_cleansed',           \
                    'notes'                             \
                ], axis=1)
       
        # Arrange columns in an intuitive order
        df = df[[                                       \
                    'listing_id',                       \
                    'minimum_nights',                   \
                    'maximum_nights',                   \
                    'availability_30',                  \
                    'availability_60',                  \
                    'availability_90',                  \
                    'availability_365',                 \
                    'number_of_reviews',                \
                    'reviews_per_month',                \
                    'first_review',                     \
                    'last_review',                      \
                    'host_id',                          \
                    'host_url',                         \
                    'host_name',                        \
                    'host_location',                    \
                    'host_since',                       \
                    'host_listings_count',              \
                    'host_response_rate',               \
                    'host_response_time',               \
                    'listing_title',                    \
                    'listing_url',                      \
                    'neighbourhood',                    \
                    'street',                           \
                    'city',                             \
                    'state',                            \
                    'zipcode',                          \
                    'country',                          \
                    'property_type',                    \
                    'room_type',                        \
                    'bed_type',                         \
                    'price',                            \
                    'security_deposit',                 \
                    'cleaning_fee',                     \
                    'amenities',                        \
                    'accommodates',                     \
                    'bedrooms',                         \
                    'bathrooms',                        \
                    'beds',                             \
                    'cancellation_policy'               \
        ]]
        # Remove unnecessary characters
        df = df.applymap(CleanSourceOperator.clean_values)
        df = df.fillna(0)
        return df
            
    def clean_values(value):
        value = str(value)
        value = value.replace('nan', '0')
        value = value.replace('\'', '')
        value = value.replace('\"', '')
        value = value.replace("\n", '')
        value = value[:250]
        return value
    
    def clean_stays_data(self, stays_df):
        # Rename desired columns
        df = stays_df.rename(columns= {"reviewer_id"    : "guest_id",       \
                                       "reviewer_name"  : "guest_name",     \
                                       "date"           : "stay_date",      \
                                       "id"             : "stay_id"        }) 
        # Drop duplicates
        count_before_drop = df.shape[0]
        df.drop_duplicates(subset="stay_id", keep=False, inplace=True)
        count_after_drop = df.shape[0]
        self.log.info("Dropped {} duplicate records".format(count_before_drop-count_after_drop))
        
        # Drop records where stay_id, guest_id, listing_id is NULL or empty string
        df[df.listing_id != '']
        df[df.stay_id != '']
        df[df.guest_id != '']
        count_after_cleansing = df.shape[0]
        self.log.info("Dropped {} null records".format(count_after_drop-count_after_cleansing))
        
        # Drop columns not of our interest
        df = df.drop(['comments'], axis=1)
        
        # Arrange columns in an intuitive order
        df = df[[                                       \
                    'listing_id',                       \
                    'guest_id',                         \
                    'guest_name',                       \
                    'stay_id',                          \
                    'stay_date'                         \
        ]]
        return df
    
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (CheckSourceOperator, CleanSourceOperator, StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator, PythonOperator, BashOperator)
from helpers import SqlQueries

COUNTRY = "United-States"
CLEAN_DATA_STORE = "s3a://airbnb-data-bucket/clean_data/"

default_args = {
    'owner': 'shivam_gupta',
    'depends_on_past': False,
    'email': ['shivamvmc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup' : False,
    'start_date' : datetime(2017, 1, 1, 0, 0, 0, 0),
    'end_date' : datetime(2017, 1, 1, 0, 0, 0, 0),
    'schedule_interval' : "@daily"
}

main_dag = DAG('AirBnB_Stays_17',
                description="Datawarehouse for AirBnB's Data Enginnering Team",
                default_args=default_args
)

start_operator = DummyOperator(
                    task_id='Begin_Execution',
                    dag=main_dag
)

check_listings_data_task = CheckSourceOperator(
    task_id="Check_Listings_Data_Source",
    dag=main_dag,
    aws_credentials_id="aws_credentials",
    s3_bucket="airbnb-data-bucket",
    s3_key="listings/airbnb-listings-" + COUNTRY.lower() + ".json"
)

check_stays_data_task = CheckSourceOperator(
    task_id="Check_Stays_Data_Source",
    dag=main_dag,
    aws_credentials_id="aws_credentials",
    s3_bucket="airbnb-data-bucket",
    s3_key=f'stays/{{execution_date.year}}/{{execution_date.month:02d}}/stays-{{execution_date.year}}-{{execution_date.month:02d}}-{{execution_date.day:02d}}.json'
)

clean_listings_data_task = CleanSourceOperator(
    task_id="Clean_Listings_Data_Source",
    dag=main_dag,
    aws_credentials_id="aws_credentials",
    s3_bucket="airbnb-data-bucket",
    s3_key="listings/airbnb-listings-" + COUNTRY.lower() + ".json",
    s3_temp_file_store="listings/temp_store/clean-listings-for-" + COUNTRY.lower() + ".csv"
)

clean_stays_data_task = CleanSourceOperator(
    task_id="Clean_Stays_Data_Source",
    dag=main_dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="airbnb-data-bucket",
    s3_key=f'stays/{{execution_date.year}}/{{execution_date.month:02d}}/stays-{{execution_date.year}}-{{execution_date.month:02d}}-{{execution_date.day:02d}}.json',
    s3_temp_file_store=f'stays/temp_store/clean-stays-for-{{execution_date.year}}-{{execution_date.month:02d}}-{{execution_date.day:02d}}.csv'
)

create_listings_stage_table = PostgresOperator(
    task_id="Create_Listings_Stage",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_listings_table
)

create_stays_stage_table = PostgresOperator(
    task_id="Create_Stays_Stage",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_stays_table
)

copy_listings_to_redshift_task = StageToRedshiftOperator(
    task_id="Stage_Listings",
    dag=main_dag,
    table_name="staging_listings",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="airbnb-data-bucket",
    s3_key="listings/temp_store/clean-listings-for-" + COUNTRY.lower() + ".csv",
    s3_format="csv"
)

copy_stays_to_redshift_task = StageToRedshiftOperator(
    task_id="Stage_Stays",
    dag=main_dag,
    table_name="staging_stays",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="airbnb-data-bucket",
    s3_key=f'stays/temp_store/clean-stays-for-{{execution_date.year}}-{{execution_date.month:02d}}-{{execution_date.day:02d}}.csv',
    s3_format="csv"
)

create_guest_stays_fact_table = PostgresOperator(
    task_id="Create_Guest_Stays_Fact_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_guest_stays_fact_table
)

load_guest_stays_task = LoadFactOperator(
    task_id="Load_Guest_Stays_Fact_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.guest_stays_fact_insert
)

create_listings_dim_table_task = PostgresOperator(
    task_id="Create_Listings_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_listings_dim_table
)

create_availability_dim_table_task = PostgresOperator(
    task_id="Create_Availability_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_availability_dim_table
)

create_hosts_dim_table_task = PostgresOperator(
    task_id="Create_Hosts_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_hosts_dim_table
)

create_reviews_dim_table_task = PostgresOperator(
    task_id="Create_Reviews_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_reviews_dim_table
)

create_guests_dim_table_task = PostgresOperator(
    task_id="Create_Guests_Dim_Table",
    dag=main_dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_guests_dim_table
)

load_listings_task = LoadDimensionOperator(
    task_id="Load_Listings_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.listings_dim_insert
)

load_guests_task = LoadDimensionOperator(
    task_id="Load_Guests_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.guests_dim_insert
)

load_reviews_task = LoadDimensionOperator(
    task_id="Load_Reviews_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.reviews_dim_insert
)

load_availability_task = LoadDimensionOperator(
    task_id="Load_Availability_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.availability_dim_insert
)

load_hosts_task = LoadDimensionOperator(
    task_id="Load_Hosts_Dim_Table",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql=SqlQueries.hosts_dim_insert
)

dq_check_task = DataQualityOperator(
    task_id="Run_Data_Quality_Checks",
    dag=main_dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_info_dict=[{"table_name": "listings", "not_null": "listing_id"},     \
              {"table_name": "guests", "not_null": "guest_id"},                \
              {"table_name": "availability", "not_null": "listing_id"},        \
              {"table_name": "hosts", "not_null": "host_id"},                  \
              {"table_name": "reviews", "not_null": "listing_id"}              \
             ]
)

end_operator = DummyOperator(
                    task_id='End_Execution',
                    dag=main_dag
)

start_operator >> check_listings_data_task >> clean_listings_data_task >> create_listings_stage_table
create_listings_stage_table >> copy_listings_to_redshift_task >> create_guest_stays_fact_table

start_operator >> check_stays_data_task >> clean_stays_data_task >> create_stays_stage_table
create_stays_stage_table >> copy_stays_to_redshift_task >> create_guest_stays_fact_table

create_guest_stays_fact_table >> load_guest_stays_task 

load_guest_stays_task >> create_listings_dim_table_task >> load_listings_task >> dq_check_task
load_guest_stays_task >> create_availability_dim_table_task >> load_availability_task >> dq_check_task
load_guest_stays_task >> create_hosts_dim_table_task >> load_hosts_task >> dq_check_task
load_guest_stays_task >> create_reviews_dim_table_task >> load_reviews_task >> dq_check_task
load_guest_stays_task >> create_guests_dim_table_task >> load_guests_task >> dq_check_task

dq_check_task >> end_operator

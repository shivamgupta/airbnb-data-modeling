# AirBnB Data Warehouse with Airflow

__1. What's the goal?__

AirBnB has created market for people to offer their vacant houses/apartments/rooms to travelers looking for a more "Homely" experience while away from home. As millennials are using AirBnB more than ever, the data engineering team faces a challenge to efficiently structure the data in a more queriable way for further analysis.

__2. What queries will you want to run?__
The Business Intelligence and Machine Learning team can use the data for:
* Number of listings in a given zipcode between certain price range.
* Most popular listings on New Year's Eve.
* Listings with no availability for next one year.
* Most frequent guests in a given city.

__3. How would Spark or Airflow be incorporated?__
Airflow will be used for data orchestration. The data can be found in S3 and we will create our tables in Amazon RedShift.

__4. Why did you choose the model you chose?__
We chose this model because of its ability to scale horizontally. We can scale this model 100x or even 1000x without any bottlenecks in our ETL process.

__5. Document the steps of the process.__
After creating Amazon RedShift Cluster the ETL process runs as follows:
* STEP 1: Check if the datasets are present in Amazon S3
* STEP 2: Clean the attributes present in both datasets
* STEP 3: Create the staging tables (if they don't already exist)
* STEP 4: Populate the staging tables with data from S3
* STEP 5: Create the fact table (if it does not already exist)
* STEP 6: Populate the fact table
* STEP 7: Create the dimension tables (if they don't already exist)
* STEP 8: Populate the dimension tables
* STEP 9: Run Data Quality Checks on the tables

The DAG looks like - 

![alt text](https://github.com/shivamgupta/airbnb-data-modeling/blob/master/img/DAG.png)

__6. Propose how often the data should be updated and why.__
Both the listings data and stays data should be updated daily. Given that new places are listed on the AirBnb platform everyday, we don't want to end up in a situation where we don't have record of a listings, or loose track of a booking. Streaming ETL pipeline would be ideal, but that's outside the scope of this project. We are using batch-processing.

__7. What would you do differently if the data was increased by 100x.__
Nothing as the current pipeline is built keeping scale in mind.

__8. What would you do differently if the pipelines were run on a daily basis by 7am.__
Since we are using Airflow, we just have to set `start_date` to incorporate the 7am runs in our DAG's `default_args`.

__Data Model__
![alt text](https://github.com/shivamgupta/airbnb-data-modeling/blob/master/img/Data_Model.jpg)


# Command to Run
1) First create RedShift cluster after setting credentials in the cfg file.
2) After the cluster setup is done, add end-point and redshift info to Airflow Admin Connections.
3) Run the DAG using Airflow UI

# Description of files

__1. `aws_iac` directory__
Contains notebook for quickly creating a RedShift Cluster.

__2. `dags`__
Contains DAG.

__3. `plugins`__
Contain operators and SQL queries

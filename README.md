# Pinterest Data Processing Pipeline

> NOTE: This is the capstone project which AiCore students complete at the end of their Data Engineering training course, which I helped design the architecture and code for. 
> The Databricks-based code (all of the Spark cleaning and processing) has not been included in this repo, to avoid plagiarism.

## Overview

Pinterest Data Processing Pipeline is a full end-to-end cloud data engineering project which uses AWS MSK and S3 to perform batch processes and Kinesis to perform streaming processes. In this project I have completed a data processing pipeline using AWS and Databricks which connects to a user_posting_emulation to emulate real users posting to the app.

The user_posting_emulation scripts runs an emulation of a pinterest feed for each the streaming and batch layers, posting a random row of data from predefined tables which includes post data, geolocation data and user data.

## Full pipeline architecture 
![](/images/architecture.png)

## Project Outline

> This is an outline of the steps (milesones) a student would follow in order to complete this project. 

### Milestone 1 & 2
I set up my AWS account and downloaded the pinterest posting structure. This included the The posting emulation, which at that time just took rows from the pinterest_database and posted them on the command line. 

### Milestone 3
This was the start of the batch processing pipeline. I created my key-pair.pem file to connect to the EC2 instance in my AWS account. I then set up Kafka on the EC2 instance and installed the IAM MSK authentication package, and made a client.properties file to configure the Kafka client to use IAM authentication.

During this milestone I also created my Kafka topics 0a4e65e909bd.pin, 0a4e65e909bd.geo and 0a4e65e909bd.user.

### Milestone 4

I connected my MSK cluster to the S3 bucket using my EC2 client. To do this I created a custom plugin and connector using MSK connect.

### Milestone 5

I then configured an API to send data to the MSK Connector through a REST Proxy integration. To do this I installed the Confluent package on the EC2 client and modified the kafka-rest.properties file to allow the REST proxy to perform IAM authentication. Using the API invoke URL I modified the user_posting_emulation to send the data through to the 3 created topics. 

### Milestones 6 & 7

These milestones were to set up my Databricks account, ingest the information stored in the topics, clean the data and perform queries.

In Databricks I created the notebook: batch_transformation_and_queries, which mounts the S3 bucket to the account, cleans the dataframes, ensureing the information is the correct datatype, that any empty entries return Null and some merging and dropping transformations - creating username column for the user_df and the coordinates column for the geo_db.

I then performed some queries on the data, such as wroking out the most popular category each year, and the median follower count for different age groups. 

### Milestone 8

I set up a Directed Acyclic Graph (DAG) to automate the workbook I have created to process the batch information every hour using AWS Managed Apache Airflow (MWAA). This is another Databricks Notebook which is run through AWS MWAA. Through using the Airflow UI I can start the DAG which will connect to the Databricks batch_transformation_and_queries workbook.

### Milestone 9

I set up AWS Kinesis Data Streams to create a continual Streaming process for the data. To do this I first created my streams in AWS Kinesis. 

I then configured the API for Kinesis integration creating methods and resources to enable the API to:
- List the streams
- Create, describe and delete streams 
- Add records to streams

Next I edited the user_post_emulation to include sending the stream information through to their corresponding streams. 

Next, I created the function create_dataframe_from_stream_data() which takes the readStream() method in Spark which connects to the Kinesis stream and creates a dataframe for the information to be displayed.
I performed the same transformations on the data as I did with the batch data and then saves each stream in a Databricks Delta Table.
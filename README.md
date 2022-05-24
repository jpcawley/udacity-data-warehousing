# Sparkify Data Warehouse
### Introduction
A music streaming startup, Sparkify, wants to analyze the [data](http://millionsongdataset.com/) they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their processes and data onto the cloud.Their data - a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app - resides in S3.
### Project Objective
Build an ETL pipeline that extracts their data from S3, stages it in Redshift, and transforms it into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
### Project Files
1. Congfiguration File
* `dwh.cfg`: configuration file with parameters and settings for the Sparkify data warehouse
2. Data in the bucket
* udacity-dend/song_data: contains all json files for song data
* udacity-dend/log_data: contains all json files for log data
* udacity-dend/json_log_path: provides details about json structure for log_data files
3. Python files
* `create_tables.py`: Python file with functions to create and drop the database and its tables
* `sql_queries.py`: Python file with drop, create, and insert SQL queries
* `etl.py`: Python file that builds out the ETL process
* `pipeline.py`: Python file that runs the ETL from start to finish
* `validate.py`: Python file that executes queries for data validation
4. Jupyter Notebook(s)
* `pipeline.ipynb`: Notebook to run the ETL from start to finish cell by cell
* `analysis.ipynb`: Notebook to run selection queries for data validation
5. Sparkify Data Warehouse Schema
6. ReadMe
### Sparkify Data Warehouse Schema
![Schema](Sparkify_DWH_Schema.pdf)
### Configuration and Setup
1. Create a new `IAM user` in your AWS account (Needed for role creation)
2. In terminal run ```aws configure``` and enter IAM user credentials 
* `access key id`
* `secret access key`
* region: `us-west-2`
* format: `json`
3. After creating role at least once, add hard coded `ARN` to `dwh.cfg` under `'IAM_ROLE'.roleARN`
4. After creating cluster at least once, add hard coded cluster `Endpoint` to dwh.cfg under `'CLUSTER'.'HOST'`
Note: `ARN` and `Endpoint` can be found in the AWS console.
Note: Need Python 3, access to AWS (access-key-id, secret-access-key), and/or Jupyter Notebook
1. In the Udacity workspace, open `pipeline.ipynb` and run all cells or
2. In the Udacity workspace, open terminal
* run ```python pipeline.py```
## Validation
* Run queries in `analysis.ipynb` after ETL completes but BEFORE deleting AWS resources.
or 
* add queries 'analysis_queries' in `validate.py` and rerun `pipeline.py`.

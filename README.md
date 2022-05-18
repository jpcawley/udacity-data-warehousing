# Sparkify Data Warehouse
## Introduction
A music streaming startup, Sparkify, wants to analyze the [data](http://millionsongdataset.com/) they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their processes and data onto the cloud.Their data - a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app - resides in S3.
## Project Objective
Build an ETL pipeline that extracts their data from S3, stages it in Redshift, and transforms it into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
## Contents
1. Data in their s3 bucket
    - udacity-dend/lsong_data: contains all json files for song data
    - udacity-dend/log_data: contains all json files for log data
2. Python files
    - create_tables.py: Python file with functions to create and drop the database and its tables
    - sql_queries.py: Python file with drop, create, and insert SQL queries
    - etl.py: Python file that builds out the ETL process
    - pipeline.py: Python file that runs the ETL from start to finish
3. Jupyter Notebooks
    - pipeline.ipynb: Notebook to run the ETL from start to finish cell by cell
4. Sparkify Database ERD
5. ReadMe
## Schema
![ERD](Sparkify_ERD.png)
## Running 
Note: Need Python 3, access to AWS (access-key-id, secret-access-key), and/or Jupyter Notebook
1. In the Udacity workspace, open DMP.ipynb and run ```%run etl.py``` or
2. In the Udacity workspace, open terminal
    1. run ```aws configure``` (Enter key id, secret password, 'us-west-2' for region, and json for format)
    2. run ```python pipeline.py```
    or
    1. run ```aws configure``` (Enter key id, secret password, 'us-west-2' for region, and json for format)
    2. ```pip install runipy```
    3. ```runipy pipeline.ipynb```
## Validation
Run queries in analysis.ipynb.

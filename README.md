## Project: Data Warehousing Amazon Redshift

1. Purpose and summary of the project
Sparkify is startup that runs a new music streaming app. Their data has grown to a point that they want to move their processes and data onto the cloud. The raw data on user activity on the app and metadata on songs resides in S3 buckets. Data warehousing is to be handled by an Amazon Redshift cluster and is to provide data in way that enables data analysts.

2. State and justify your database schema design and ETL pipeline
As described in the Udacity Data Modeling course, the star schema is used to model the data since data analyses on song listening data is the goal. The star schema simplifies queries.
Table songlpays is the fact table while user, songs, artists, time and users are the dimension tables (information to answer business questions). The two staging tables that handle load from S3 buckets to increase efficiency of ETL processes, ensure data integrity and support data quality operations

**Staging table staging_events:**
Staging table to process log data from S3 bucket before inserting data into analytics tables

**Staging table staging_songs:**
Staging table to process song data from S3 bucket before inserting data into analytics tables

**Table songplays:**
Records in log data associated with song plays i.e. records with page NextSong
Fact table with references to artists, songs, time and users

**Table users:**
Users in the app
Dimension table with user_id ad PRIMARY KEY which identifies a user
Information about users can change. E.g. a user could switch from level 'free' to 'paid' or change the last name

**Table songs:**
Songs in music database
Dimension table
Information on songs do not change over time

**Table time:**
Timestamps of records in songplays broken down into specific units
Dimension table
Information on songs do not change over time

**Table artists:**
Artists in music database
Dimension table
Information about artists can change. E.g. name or location can change

3. Files
**Song dataset (as per project specifications)**
Song data is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID

**Log dataset (as per project specifications)**
Log files are in JSON format generated by event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.
The log files in the dataset you'll be working with are partitioned by year and month.

**create_tables.py**
Drops and creates tables. Handels inserts

**etl.py**
Load data in JSON files from S3 to staging tables on Redshift specified in copy_table_queries 
Load data from staging tables to analytics tables specified in insert_table_queries

**IaC_create_cluster_and_roles.ipynb**
Populate KEY, SECRET, DWH_DB_USER, DWH_DB_PASSWORD in dwh.cfg
Create cluster and IAM role and open incoming TCP port
Delete cluster

4. Specify cluster and run python scripts
a. Populate dwh.cfg with host (cluster), IAM role, DB_USER and DB_PASSWORD
b. Make sure you can connect to cluster speficied in dwh.cfg and that incoming TCP port is open
c. Open Jupyter
    File - Console - Python 3 - Select
    !python create_tables.py
    !python etl.py
d. Check if tables where populated correctly via the Redshift query editor. Ensure you are in schema 'public'
    SELECT * FROM songplays LIMIT 10
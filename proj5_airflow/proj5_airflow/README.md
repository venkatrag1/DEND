# Data Pipeline with Airflow

## Intro to the Dataset
The primary datasource for this project comes from two prefixes in the udacity-dend
S3 bucket.

There are several log_data json files that contain several lines each
of songplay events. The ones with page = "NextSong" are the ones we're 
interested in as they represent the actual instance of a song being played.
These are imported from the log_data folder using the log_json_path.json json file. 

There are also json_files under song_data folder that contain several dimensions
to join with the log_data.

## Purpose
The purpose of this exercise is to build an ETL pipeline in Apache airflow, that 
runs data ingestion from S3 into Redshift followed by Quality checks. 


## Schema design

The staging tables are designed to match the columns available in the json data.

![staging_tables](assets/staging_tables.png)

The varchar sizes are finalized after trial load with different sizes for various fields.
Since, the dimensional tables are formed primarily by joining on the artist name, 
along with matching the song title and duration (from project 1), we choose
artist name as the distkey, and song title as the sortkey to speed up the joins
and minimize shuffling.

Then the dimensional tables are build out to a Star Schema,
with the events Facts at the center surrounded by relevant
dimension tables.


![dimensional_tables](assets/ERD.png)


Star schema is apt for this database, since there is one central source
of event which stems from songplay and all metadata information is centered
around this event play.

The user and artist table are relatively small (104 and 10025 respectively),
we use diststyle all to broadcast these to all nodes, to enable quick joins and 
minimize shuffling. 

Even though the time table is also small currently, this could potentially
grow faster than artists and user table, as this relates to songplay events
so we will keep this as diststyle auto.

Wherever start_time is present, we will use that as the sorting key.

Then we will use song_id as the distkey to keep events pertaining
to certain songs on the same nodes. Finally we sort on artist name
since we anticipate this will be a frequently accessed field in our queries.


## Files in the Repository and Running the code
sparkify_dag.py is dag that stages the ETL utilizing operators and helpers in the 
plugins directory.

After installing airflow locally, and creating a Redshift instance, we need to 
create two Connections - one for aws_credentials to copy from S3, and one for
Redshift with details of Redshift host and authentication.

Then launch airflow 
```
> airflow scheduler --daemon
> airflow webserver --daemon -p 3000 
```

Now, go to port localhost:3000 in the browser and switch on the 'sparkify_dag' DAG.

All the stages should succeed. 








# Redshift Data Warehouse

A project for Udacity's Data Engineering Nanodegree.

## Project Purpose
This purpose of this project is to process song and songplay data into a data warehouse in Amazon Redshift.

## Datasets
The song data comes from the One Million Songs dataset, while the songplay data is simulated. Both datasets are collections of JSON files stored on AWS S3.

After creating your Redshift cluster through the AWS Management Console or through IaC, enter the following into `dwh.cfg`:

HOST= <br>
DB_NAME= <br>
DB_USER= <br>
DB_PASSWORD= <br>
DB_PORT= <br>

## Creating the Data Warehouse

This project contains three Python files, `sql_queries.py`, `create_tables.py` and `etl.py`. 

`sql_queries.py` contains the SQL queries used to create, drop and load data into the Redshift tables. 

`create_tables.py` contains the Python script that drops and creates the Redshift tables by running the SQL queries in `sql_queries.py`

Similarly, `etl.py` populates these tables from the S3 data sources.

To create the data warehouse, run `create_tables.py` then `etl.py` in your terminal.

## Data Warehouse Schema

Here are the SQL queries that define the data warehouse schema.
These includes the staging tables, which copy the JSON files into a tabular format without any additional transformation.
Columns are then selected from the staging tables and transformed into the final tables.

Notice that in the final tables, we use diststyle = 'KEY' for the songplays table and diststyle = 'ALL' in the other tables.
The KEY diststyle distributes the table's data across slices by key, while ALL copies the table to each node.

We use the KEY diststyle in songplays since it is a large table. The remaining dimensional tables use ALL diststyle. 

## Staging Tables


        CREATE TABLE staging_events (
            artist varchar,
            auth varchar,
            first_name varchar,
            gender char,
            items_in_session int,
            last_name varchar,
            length decimal,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration decimal,
            session_id int,
            song varchar,
            status int ,
            ts bigint,
            user_agent varchar,
            user_id int);

        CREATE TABLE staging_songs (
            artist_id varchar,
            artist_latitude decimal,
            artist_location varchar,
            artist_longitude decimal,
            artist_name varchar,
            duration decimal,
            num_songs int,
            song_id text,
            title varchar, 
            year int);
            
## Final Tables

        CREATE TABLE songplays (
            songplay_id int identity(0,1) PRIMARY KEY SORTKEY DISTKEY,
            start_time timestamp,
            user_id varchar,
            level varchar,
            song_id varchar,
            artist_id varchar,
            session_id varchar,
            location varchar,
            user_agent varchar) 
            DISTSTYLE KEY;


        CREATE TABLE users (
            user_id varchar PRIMARY KEY SORTKEY,
            first_name varchar,
            last_name varchar,
            gender varchar(2),
            level varchar)
            DISTSTYLE ALL;


        CREATE TABLE songs (
            song_id varchar PRIMARY KEY SORTKEY,
            title varchar,
            artist_id varchar,
            year int,
            duration decimal) 
            DISTSTYLE ALL;


        CREATE TABLE artists (
            artist_id varchar PRIMARY KEY SORTKEY,
            name varchar,
            location varchar,
            latitude decimal,
            longitude decimal)
            DISTSTYLE ALL;


        CREATE TABLE time (
            start_time timestamp PRIMARY KEY SORTKEY,
            hour int,
            day int,
            week int,
            month int,
            year int,
            weekday int)
            DISTSTYLE ALL;

# Description:
This Repository is part of Udacity's Data Engineering Nano Degree Program
It conntains the deliverables for **Project: Data Modeling with Postgres**

The purpose of the project is to provide Analytical Capabilities for a fictional Music Streaming Company called _Sparkify_
regarding User listening habits using a Star Schema Database Model to allow the extraction and access to high volumes of data 
in a fast manner.


## Dataset (Source)
The dataset is comprised of .json files distributed as follows:
* data/log_data - Files containing User listening history logs
* data/song_data - Files that containg Song, and Artists information

Example files are included in _data.zip_ file, which needs to be extracted in along the python scripts


## Database (Destination)
The destination of the ETL is a PostgresSQL Database called _Sparkify_ with the following DB structure:
* Four Dimension tables
    * artists: Artist information - ArtistId, Name, location, latitude, longitude 
    * songs:   Song information - SongId, title, ArtistId, year, duration
    * time:    User Listening Activity - StartTime, hour, day, week, month, year, weekday
    * users:   User information - UserId, First and Last names, gender, UserLevel

 * One Fact table
   * songplays: User Listening History - SongplayId (AutoNumeric), StartTime, UserId, UserLevel, SongId, ArtistId, SessionId, location and UserAgent

Referential Integrity is not being enforced across the tables


## ETL Design and Workflow
Code repository contains only three Python scripts:
1. _create_tables.py_ Python Program that drops and recreates the _Sparkify_ Database along with its tables
2. _sql_queries.py_ Contains the SQL queries used in the Main Program
3. _etl.py_ Main Python program that performs the ETL process

Following is ETL Workflow:
1. _create_tables.py_ script needs to be run first in order to:
    * Drop/Create _sparkifydb_ Database
    * Create Dimension and Fact tables

2. Once the Database Structurehas has been created, run the main program, _etl.py_ script, to perform the following ETL operations:
    * Parse through song_data .json files in order extract and load song and artist Dimension tables
    * Parse through log_data .json files in order extract, transform, and load user, and time Dimension tables and songplays Fact table


## Software Pre-Requisites
The following Tools and Modules need to be installed in order to to run the code in this repository:
* PostgresSQL Database
* psycopg2 Database connector for Python
* Pandas
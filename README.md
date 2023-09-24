## Data Warehouse

## Project Synopsis
MusicBee, a burgeoning music streaming service, is expanding rapidly and aims to shift its data and operations to the cloud. Currently, the data is hosted on S3, encompassing directories filled with JSON logs detailing user interactions with the app and JSON metadata detailing the music content available in the app. The goal of this project is to develop a seamless ETL pipeline to construct a data warehouse. This pipeline will facilitate the extraction of data from S3, staging it in Redshift, and transforming it into a series of dimensional tables, empowering the analytics team to glean deeper insights into user music preferences.

## Data Resources
Two main datasets are stored in the S3 buckets:

## Music Dataset
The initial dataset is a segment of data derived from the Million Song Dataset. Every file is structured in JSON format, encapsulating data about individual songs and their respective artists.

## Sample Record of a Song:

{
  "num_songs": 1, 
  "artist_id": "ARJIE2Y1187B994AB7", 
  "artist_latitude": null, 
  "artist_longitude": null, 
  "artist_location": "", 
  "artist_name": "Line Renaud", 
  "song_id": "SOUPIRU12A6D4FA1E1", 
  "title": "Der Kleine Dompfaff", 
  "duration": 152.92036, 
  "year": 0
}
## Activity Log Dataset
The subsequent dataset comprises log files in JSON format created by this event simulator, utilizing the music dataset to generate simulated user activity logs for a music streaming app based on configured parameters. The dataset segregates the log files by year and month.

## Sample Activity Log Record:
{
  "artist": null, 
  "auth": "Logged In", 
  "firstName": "Walter", 
  "gender": "M", 
  "itemInSession": 0, 
  "lastName": "Frye", 
  "length": null, 
  "level": "free", 
  "location": "San Francisco-Oakland-Hayward, CA", 
  "method": "GET", 
  "page": "Home", 
  "registration": 1540919166796.0, 
  "sessionId": 38, 
  "song": null, 
  "status": 200, 
  "ts": 1541105830796, 
  "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", 
  "userId": "39"
}

## Database Schema
## Core Table
songplay_fact: This table documents the logs associated with songs played by users.

## Dimensional Tables
user_dim: Holds data on MusicBee app users. The fields are user_id, first_name, last_name, gender, and level.
song_dim: Contains information on songs available in the database. The fields are song_id, title, artist_id, year, and duration.
artist_dim: Stores data about the artists featured in the database. The fields include artist_id, name, location, latitude, and longitude.
time_dim: Details the timestamps in songplays, segmented into different units including start_time, hour, day, week, month, year, and weekday.

## Execution Guide

## Establish a Redshift cluster using the script:

$ python create_cluster_iac.py

##Execute create_tables.py to establish the necessary staging tables:
$ python create_tables.py

## Run etl.py to facilitate data transfer from the staging tables to the analytics tables housed on Redshift:
$ python etl.py

## Upon completion, you're all set to run analytical queries on your Redshift database.

## To dismantle the Redshift cluster, use the following script:

$ python delete_cluster_iac.py







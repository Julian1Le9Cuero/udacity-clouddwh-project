## 1. Purpose of this database 
The purpose of this database is to serve as a valuable resource for the analytics team in Sparkify in order for them to find relevant insights about the songs all of their users tend to listen to. This data can provide a lot of help for the company in order to make informed decisions which could potentially generate advantage in the market over its competitors.

## 2. Reasoning behind the database schema design and ETL pipeline.
The idea of this code is to extract the Sparkify user activity data located in JSON logs in AWS S3, then transform it into a usable format, and finally load it in order to make it available for the analytics team.

Here I decided to use a star schema design composed by the following tables:
- A **fact table** called *songplays* which could be used for aggregations and important metrics. It has records associated with song plays. These are records that have NextSong as their page. It will mainly contain foreign keys associated with the primary keys from the foreign tables. *Columns*: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- A series of **dimension tables** that contain relevant characteristics about the users and could generate a deeper analysis on the data. These are:
 - **users**: Personal data about each user in the app. *Columns*: first_name, last_name, gender, level
 - **songs**: Songs in music database. *Columns*: song_id, title, artist_id, year, duration
 - **artists**: Artists in music database related to the songs the users listen to. *Columns*: name, location, latitude, longitude
 - **time**: Timestamps of the time a particular song in songplays is listened. It is broken down into specific units. *Columns*: start_time, hour, day, week, month, year, weekday

The ETL pipeline uses Python on the backend. It extracts the data and then puts it into a staging area. This area will contain the raw data and it can be quiteusful if we need to quickly get the raw data without having to wait for it or in case we neeed to do additional checks about the data quality.
Then the data is transformed and loaded into Redshift. From Redshift we will access the S3 data from sparkify using an AWS role with S3 read access. Redshift is used because it can give us notable performance through a cluster that can contain multiple nodes depending on our demand.
Once the data is loaded, it will be in a correct format and cleaned so that it is read and used for further analysis.

## 3. Example queries and results for song play analysis
Examples:
 - **Number of songs by month**
 SELECT t.month, COUNT(sp.start_time) AS num_songs
 FROM songplays sp INNER JOIN time t ON sp.start_time = t.start_time
 GROUP BY t.month
 ORDER BY num_songs DESC
 - **Total number of unique users**
 SELECT COUNT(user_id) FROM users

### 4. How to run the scripts
- Install the necessary libraries:
 - pip install configparser
 - pip install psycopg2
- About the scripts/files:
    - create_tables.py = This is the file that will correctly create the tables for the database. It makes use of the queries located at sql_queries.py as well as credentials from dwh.cfg
    - sql_queries.py = This has all of the queries needed that will interact with the database and then exports them so they can be used from other files in the ETL. It also requires credentials from dwh.cfg 
    - etl.py = This is the most important file given that it will do the necessary steps to make the data available in Redshift. It loads the initial staging tables, transforms the data in the staging tables and then inserts it into the fact and dimension tables in the star schema mentioned above. It also makes additional checks on the data using some select statements. 
    - dwh.cfg = This is an important credential file that divides the credentials required for the etl in three different groups. You will have to fill it with your own AWS credentials.
        - CLUSTER: It has the database credential from the Redshift cluster: host, database name, user, password, and port.
        - IAM_ROLE: The IAM Role that uses Redshift as a service and has a permission to read from S3.
        - S3: Has the locations of the songs and events data in S3 as well as the metadata JSON file that will be used to properly parse the JSON data from the events dataset.
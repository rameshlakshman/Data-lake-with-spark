DATALAKES : SPARKIFY
==============================
Purpose : 
This DATA LAKES is built for moving the sparkify processes and data into cloud.
Business needs to know the below key information generated from the new music streaming app for Sparkify.
    => Metadata for Songs being played
    => User-activity on the app.

SOURCE 
======
Source of data in these tables are JSON logs stored in S3 bucket.
Two feeds into the process are -
    => SONG_DATA formatted as JSON logs
    => LOG_DATA formatted as JSON logs

TARGET
======
Parquet files in S3 bucket for below data - 
        'SONGS' 
        'USERS'
        'ARTISTS'
        'TIME' 
        'SONGPLAYS'

    PARQUET FILES DETAILS :
    
        SONG_DATA feed contains information about the SONG and ARTISTS (i.e. song metadata). 
                Hence, these two information are loaded into separate 
                parquet files SONGS & ARTISTS tables respectively.
            =>SONGS file holds below attributes :
                    song_id, song_title, artist, song_year, duration
            =>ARTISTS file holds below attributes :
                    artist_id, artist_name, location, latitude, longitude

        LOG_DATA feed contains information about user-activity in the app. 
                Hence, the information in this are loaded into three separate 
                files in TIME & USERS & SONGPLAYS tables respectively.
            => USERS file holds below attributes :
                    user_id, first_name, last_name, gender, level
            => TIME file holds below attributes :
                    start_time, hour, day, week, month, year, weekday
            => SONGPLAYS file
                    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

ETL PROCESSES
=============
The process is accomplished in below major steps.
    => SETUP input and output S3 bucket information
    => SETUP SparkSession
    => PROCESS song dataset
        => Loads SONGS, ARTISTS parquet files
    => PROCESS log dataset
        => LOADS USERS, TIME, SONGPLAYS parquet files
    
              
FILES USED
==========
(1) etl.py      => This contains python code that reads S3 source bucket, processes, does ELT
                   and loads into S3 target bucket.
(2) dl.cfg      => Configuration file that has AWS credentials to access S3 bucket


HOW TO RUN
==========
(1) dl.cfg      => Populate access_key_id & secret_access_key for AWS IAM user
    
(2) etl.py      => Execute this to
    => SETUP input and output S3 bucket information
    => SETUP SparkSession
    => PROCESS song dataset
        => Loads SONGS, ARTISTS parquet files
    => PROCESS log dataset
        => LOADS USERS, TIME, SONGPLAYS parquet files
        
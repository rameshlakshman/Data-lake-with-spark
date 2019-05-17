from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date
from pyspark.sql.types import TimestampType as Tstmp
from pyspark.sql.functions import monotonically_increasing_id
import os
import configparser
from datetime import datetime
import boto3
import  pyspark.sql.functions as F


####   SETTING UP AWS CREDENTIALS     ######
config = configparser.ConfigParser()
config.read('dl.cfg')
access_id = config.get('AWS','AWS_ACCESS_KEY_ID')
access_key = config.get('AWS','AWS_SECRET_ACCESS_KEY')
os.environ['AWS_ACCESS_KEY_ID']     = access_id
os.environ['AWS_SECRET_ACCESS_KEY'] = access_key


####   INITIALIZE S3 CLIENT VIA BOTO3 PKG    ######
s3 = boto3.client('s3',
                 aws_access_key_id     = access_id,
                 aws_secret_access_key = access_key
                 )


def create_spark_session():
    """This function is to initiate/load a SparkSession
    """
    print("Setting up sparksession ", datetime.now())
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def get_file_list(input_data, song_folder):
    """ This function paginates the list of files from input dataset
    As the 'list_objects_v2' resource only loads max of 1000 rows, paginator
    function is used to overcome this limitation.
        This function receives the paramets of S3 bucket & the folder information
    from where the list of files to be extracted is to be obtained.
    """
    pagi = s3.get_paginator('list_objects_v2')
    page_iterator = pagi.paginate( Bucket = 'udacity-dend', Prefix = song_folder )
    
    bucket_object_list = []
    for page in page_iterator:
        if "Contents" in page:
            for key in page[ "Contents" ]:
                keyString = input_data + key[ "Key" ]
                if keyString.endswith('json'):
                    bucket_object_list.append(keyString)
                    
    return(bucket_object_list)   


def process_song_data(spark, input_data, output_data):
    """ This function processes the SONG dataset.
    Loads the tables SONGS, ARTISTS.
    If entries are already exist in S3 parquet, code has logic to ignore those entries.
    Exception handling to NOT fail in case of parquet files not existing (e.g. FIRST RUN)
    """
    print("Getting song dataset json files from s3a :", datetime.now())
    song_folder = 'song_data'    
    song_data = get_file_list(input_data, song_folder)
    print("SONG dataset json files list ready... ", datetime.now())
    print("Num of files in song_data :", len(song_data))

    #Schema for SONG dataset
    SongSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int())	
    ])    
        
    #"""
    song_data_10 = []
    for i in range(210):
        song_data_10.append(song_data[i])
    #"""
    
    print("Reading song json data into dataframe  : " , datetime.now())
    #df = spark.read.json(song_data, schema=SongSchema)
    df = spark.read.json(song_data_10)
    print("DONE - Reading song json data into dataframe", datetime.now())

    ###############           SONGS TABLE PROCESSING           ################
    # Extract columns to create songs table
    print("Prep for songs_table ", datetime.now())
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    print("songs_table df ready...", datetime.now())

    # Retrieve existing songs table entries from S3 so duplicates can be avoided
    print("Read existing SONGS table from S3 : ", datetime.now())
    s3a_songs = output_data + "songs"
    try:
        song_table_exist = spark.read.parquet(s3a_songs)
    except:
        #if file not exists in S3, setup a dummy one
        song_table_exist = songs_table.where(songs_table.song_id == '')

    #song_new = song_new.where(song_table_exist.song_id.isNull()) \
    song_new = songs_table.join(song_table_exist, songs_table.song_id == song_table_exist.song_id, how = "left_outer") \
                       .select(songs_table.song_id, songs_table.title, songs_table.artist_id, \
                               songs_table.year, songs_table.duration)

    print("SONGS to-be-insert count :", song_new.count())    
    
    # write songs table to parquet files partitioned by year and artist
    print("Going to write song_table into s3a as parquet : ", datetime.now())
    #if song_new.count() > 0:        
    try:
        song_new.write.partitionBy("year","artist_id").parquet(s3a_songs)
    except:
        song_new.write.mode(saveMode = 'append').partitionBy("year","artist_id").parquet(s3a_songs)
    print("Finished writing song_table parquet into s3a : ", datetime.now())
        
    ###############           ARTISTS TABLE PROCESSING           ################        
    # Extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()

    # Retrieve existing artists table entries from S3 so duplicates can be avoided    
    print("Read existing ARTISTS table from S3 : ", datetime.now())    
    s3a_artists = output_data + "artists"
    try:
        artists_table_exist = spark.read.parquet(s3a_artist)
    except:
        #if file not exists in S3, setup a dummy one
        artists_table_exist = artists_table.where(artists_table.artist_id == '')
        
    #                          .where(artists_table_exist.artist_id.isNull()) \
    artist_new = artists_table.join(artists_table_exist, artists_table.artist_id == artists_table_exist.artist_id, how = "left_outer") \
                              .select(artists_table.artist_id, artists_table.artist_name, artists_table.artist_location, \
                                      artists_table.artist_latitude,artists_table.artist_longitude)
    
    print("ARTISTS to-be-insert count :", artist_new.count())
    # write artists table to parquet files
    print("Going to write artists_table into s3a as parquet : ", datetime.now())
    #if artist_new.count() > 0:        
    try:
        artist_new.write.parquet(s3a_artists)
    except:
        artist_new.write.mode(saveMode = 'append').parquet(s3a_artists)
    print("Finished writing artists parquet into s3a : ", datetime.now())


def process_log_data(spark, input_data, output_data):
    """ This function processes the LOG dataset.
    Loads the tables USERS, TIME, SONGPLAYS
    If entries are already exist in S3 parquet, code has logic to ignore those entries.
    Exception handling to NOT fail in case of parquet files not existing (e.g. FIRST RUN)
    """    
    # Get the foldername in S3
    log_folder = 'log_data'    
    log_data = get_file_list(input_data, log_folder)

    # Read LOG dataset from S3
    df = spark.read.json(log_data)
    
    # user-defined-function for converting epoch ms to meaningful timestamp.
    get_timest = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %T'))
    
    df = df.select("userId", "firstName", "lastName", "gender", "level", "song", "artist", 
                   "sessionId", "location", "userAgent", "ts").withColumn("start_time", get_timest(df.ts))
    df = df.select(df["start_time"].cast(Tstmp()), "userId", "firstName", "lastName", "gender", 
                   "level", "song", "artist", "sessionId", "location", "userAgent")

    ###############           USERS TABLE PROCESSING           ################            
    # Extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    s3a_users = output_data + "users"

    # Retrieve existing USERS table entries from S3 so duplicates can be checked        
    print("Read existing USERS table from S3 : ", datetime.now())    
    try:
        users_table_exist = spark.read.parquet(s3a_users)
    except:
        #if file not exists in S3, setup a dummy one
        users_table_exist = users_table.where(users_table.userId == '')

    # Filter out already existing users in parquet/target.
    #                      .where(users_table_exist.userId.isNull()) \
    user_new = users_table.join(users_table_exist, users_table.userId == users_table_exist.userId, how = "left_outer") \
                          .select(users_table.userId, users_table.firstName,users_table.lastName, \
                                  users_table.gender,users_table.level)
    print("USERS to-be-insert count :", user_new.count())
    
    print("Writing users_table into parquet in s3a : ", datetime.now())    
    try:
        user_new.write.parquet(s3a_users)
    except:
        user_new.write.mode(saveMode = 'append').parquet(s3a_users)    
    print("Finished writing users_table parquet into s3a : ", datetime.now())

    ###############           TIME TABLE PROCESSING           ################                
    # Extract columns to create time table
    time_table = df.select(df.start_time.alias("start_time"), hour(df.start_time).alias("Hour"), 
                           month(df.start_time).alias("month"), dayofmonth(df.start_time).alias("dayofmonth"),
                           weekofyear(df.start_time).alias("weekofyear"), year(df.start_time).alias("year")).distinct()

    # Retrieve existing TIME table entries from S3 so duplicates can be checked            
    print("Read existing TIME table from S3 : ", datetime.now())    
    s3a_time = output_data + "time"
    try:
        time_table_exist = spark.read.parquet(s3a_time)
    except:
        #if file not exists in S3, setup a dummy one
        time_table_exist = time_table.where(time_table.start_time == '')
        
    #                    .where(time_table_exist.start_time.isNull()) \
    time_new = time_table.join(time_table_exist, time_table.start_time == time_table_exist.start_time, how = "left_outer") \
                         .select(time_table.start_time, time_table.Hour,time_table.month,  \
                                 time_table.dayofmonth,time_table.weekofyear, time_table.year)

    print("TIME to-be-insert count :", time_new.count())
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table parquet into s3a : ", datetime.now())
    s3a_time = output_data + "time"
    try:
        time_new.write.partitionBy("year","month").parquet(s3a_time)
    except:
        time_new.write.mode(saveMode = 'append').partitionBy("year","month").parquet(s3a_time)
    print("Finished writing time_table parquet into s3a : ", datetime.now())        
        

    ###############           SONGPLAYS TABLE PROCESSING           ################                
    # read in song data to use for songplays table  
    song_df = spark.read.parquet("spark-warehouse/songs")    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias('a').join(song_df.alias('b'), col('a.song') == col('b.title'), how = 'left_outer').select(col("a.start_time"), 
                   col("a.userId"), col("a.level"),col("b.song_id"), col("b.artist_id"), col("a.sessionId"), 
                   col("a.location"), col("a.userAgent"), year(col("a.start_time")).alias("year"), 
                   month(col("a.start_time")).alias("month"))
    
    s3a_songplays = output_data + "songplays"

    # Retrieve existing SONGPLAYS table entries from S3 so duplicates can be checked            
    print("Read existing SONGPLAYS table from S3 : ", datetime.now())    
    max_cnt = 0
    try:
        songplays_table_exist = spark.read.parquet(s3a_songplays)    
    except:
        max_cnt = 1
        #if file not exists in S3, setup a dummy one
        songplays_table_exist = songplays_table.withColumn("songplays_id", songplays_table.userId).where(songplays_table.start_time == '') 
        songplays_table_exist = songplays_table_exist.select(songplays_table_exist.songplays_id, 
                                                                                  songplays_table.start_time, 
                                                                                  songplays_table.userId, 
                                                                                  songplays_table.level, 
                                                                                  songplays_table.song_id,
                                                                                  songplays_table.artist_id,
                                                                                  songplays_table.sessionId, 
                                                                                  songplays_table.location, 
                                                                                  songplays_table.userAgent, 
                                                                                  songplays_table.year, 
                                                                                  songplays_table.month)
    if max_cnt == 0:
        max_cnt = songplays_df.max(songplay_id) + 1         

    #                                         .where(songplays_table_exist.start_time == '') \
    songplays_new = songplays_table.join(songplays_table_exist, (songplays_table.start_time == songplays_table_exist.start_time) \
                                         & (songplays_table.song_id == songplays_table_exist.song_id) \
                                         & (songplays_table.sessionId == songplays_table_exist.sessionId), how = "left_outer") \
                                              .withColumn("songplays_id", monotonically_increasing_id() + max_cnt) 
    
    songplays_new = songplays_new.select(songplays_new.songplays_id, songplays_table.start_time, songplays_table.userId,songplays_table.level,\
                                                       songplays_table.song_id, songplays_table.artist_id, \
                                                          songplays_table.sessionId, songplays_table.location, \
                                                          songplays_table.userAgent, songplays_table.year, songplays_table.month)   

    print("SONGPLAYS to-be-insert count :", songplays_new.count())
    
    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_table parquet into s3a : ", datetime.now())
    #if songplays_new.count() > 0:
    try:
        songplays_new.write.partitionBy("year","month").parquet(s3a_songplays)
    except:
        songplays_new.write.mode(saveMode = 'append').partitionBy("year","month").parquet(s3a_songplays)
    print("Finished writing songplays parquet into s3a : ", datetime.now())
        
        
def main():
    """MAIN METHOD : Process starts here.
    Setup sparksession
    Setup input and output S3 buckets information
    Issue calls to processing SONG and LOG datasets
    """
    print("MAIN method start :", datetime.now())
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-rl/"
       
    print("Calling process_song_data... ",  datetime.now())
    process_song_data(spark, input_data, output_data)    
    print("SONG dataset processed successfully ! Good job !! Keep it going !!!",  datetime.now())
    
    print("Calling process_log_data... ", datetime.now())
    process_log_data(spark, input_data, output_data)
    print("LOG dataset processed successfully ! Well done !! Mission accomplished !!! ", datetime.now())


if __name__ == "__main__":
    main()

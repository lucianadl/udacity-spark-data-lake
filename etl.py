import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

def create_spark_session():
    """
    Creates or gets the SparkSession
    """     
    
    spark = SparkSession \
        .builder \
        .appName("Sparkify Data Lake") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes JSON song files and writes songs and artists tables to parquet files
    
    Parameters:
    spark: Current SparkSession
    input_data: Location of the "song_data" directory containing the JSON song files to be read
    output_data: Location where the parquet files will be written 
    """    
    
    # Get filepath to song data files
    song_data = os.path.join(input_data, "song_data")     
    
    # Read song data files
    df = spark.read.json(song_data)
    
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("song_data")
    
    # Extract columns to create songs table
    songs_table = spark.sql(
        """
        SELECT DISTINCT song_id, title, artist_id, year, duration 
        FROM song_data
        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table \
        .write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(os.path.join(output_data, "songs.parquet"))

    # extract columns to create artists table
    artists_table = spark.sql(
        """
        SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM song_data
        """)
    
    # write artists table to parquet files
    artists_table \
        .write \
        .mode("overwrite") \
        .parquet(os.path.join(output_data, "artists.parquet"))    

def process_log_data(spark, input_data, output_data):
    """
    Processes JSON log files and writes users, time and fact tables to parquet files
    
    Parameters:
    spark: Current SparkSession
    input_data: Location of the "log_data" directory containing the JSON log files to be read
    output_data: Location where the parquet files will be written 
    """ 
    
    # get filepath to log data files
    log_data = os.path.join(input_data,"log_data")     

    # read log data files
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
        
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("log_data")
    
    # extract columns for users table    
    users_table = spark.sql(
        """
        WITH latest AS (
            SELECT userId, MAX(ts) as ts 
            FROM log_data 
            GROUP BY userId
        )
        SELECT DISTINCT s.userId AS user_id, s.firstName AS first_name, s.lastName AS last_name, s.gender, s.level
        FROM log_data s
             JOIN latest ON latest.userId = s.userId
        WHERE s.ts = latest.ts
        """)
    
    # write users table to parquet files
    users_table \
        .write \
        .mode("overwrite") \
        .parquet(os.path.join(output_data, "users.parquet"))    

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000) ), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    df.createOrReplaceTempView("log_data")
    
    # extract columns to create time table
    time_table = spark.sql(
        """
        SELECT DISTINCT start_time, 
               hour(start_time) AS hour,
               dayofmonth(start_time) AS day, 
               weekofyear(start_time) AS week, 
               month(start_time) AS month, 
               year(start_time) AS year, 
               dayofweek(start_time) AS weekday
        FROM log_data
        """)
    
    # write time table to parquet files partitioned by year and month
    time_table \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(os.path.join(output_data, "time.parquet"))

    # read in song data to use for songplays table
    song_df = spark.sql(
        """
        SELECT l.start_time,
               month(l.start_time) AS month, 
               year(l.start_time) AS year, 
               l.userId AS user_id,
               l.level,
               s.song_id,
               s.artist_id,
               l.sessionId AS session_id,
               l.location,
               l.userAgent AS user_agent
        FROM log_data l
             JOIN song_data s ON l.song = s.title AND l.artist = s.artist_name
        """)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(os.path.join(output_data, "songplays.parquet"))

def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']    
    input_data = config['S3']['INPUT_DATA']
    output_data = config['S3']['OUTPUT_DATA']
    
    spark = create_spark_session()
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This function creates a spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads song_data from S3 and processes it by extracting the songs and artist tables
    
    Parameters:
        spark: the Spark session
        input_data: the location of song_data
        output_data: the location where post-processed data will be stored
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file and create sql view
    song_df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = song_df.select(
        'song_id', 
        'title', 
        'artist_id', 
        'year', 
        'duration').where('song_id IS NOT NULL').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = song_df.select(
        'artist_id', 
        'artist_name', 
        'artist_location', 
        'artist_latitude', 
        'artist_longitude').where('artist_id IS NOT NULL').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    This function loads log_data from S3 and processes it by extracting the songs and artists tables 
    
    Parameters:
        spark: the Spark session
        input_data: the location of log_data
        output_data: the location where post-processed data will be stored
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json' 

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays and create temp view
    # extract columns for users table    
    users_table = log_df.filter("page = 'NextSong'").selectExpr(
        'userId AS user_id',
        'firstName AS first_name',
        'lastName AS last_name',
        'gender AS gender',
        'level AS level').distinct().where('userId IS NOT NULL')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    def format_datetime(ts):
        return datetime.fromtimestamp(ts/1000.0)
    
    get_timestamp = udf(lambda x: format_datetime(int(x)), TimestampType())
    df = log_df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)), DateType())
    df = log_df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        'ts', 
        'datetime', 
        'timestamp', 
        year(df.datetime).alias('year'), 
        month(df.datetime).alias('month')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = log_df.join(song_df, (song_df.artist_name == log_df.artist) & (song_df.title == log_df.song))
    songplays_table = joined_df.selectExpr(
        'monotonically_increasing_id() AS songplay_id',
        'to_timestamp(ts/1000) AS start_time',
        'month(to_timestamp(ts/1000)) AS month',
        'year(to_timestamp(ts/1000)) AS year',
        'userId AS user_id',
        'level AS level',
        'song_id AS song_id',
        'artist_id AS artist_id',
        'sessionId AS session_id',
        'location AS location',
        'userAgent AS user_agent').distinct()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    """
    This function triggers the etl process
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-bucket-cye/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

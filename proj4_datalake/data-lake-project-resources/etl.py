import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import (
    StructType as R, StructField as Fld, 
    DecimalType as Decimal, DoubleType as Dbl, 
    StringType as Str, 
    ShortType as Short, IntegerType as Int, LongType as Long, 
    DateType as Date ,
    TimestampType as TimeStamp,
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Read the song json files into parquet tables
    :param spark: spark session
    :type spark: SparkSession
    :param input_data: path (local or s3) to prefix to song_data root
    :type input_data: str
    :param output_data: path (local or s3) to write output parquet files to
    :type output_data: str
    :return: None
    :rtype: None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # define schema rather than inferring to minimize transformations
    song_data_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str(), nullable=False),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str(), nullable=False),
        Fld("title", Str()),
        Fld("duration", Decimal()),
        Fld("year", Short())
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_data_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs_table')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as lattitude', 'artist_longitude as longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table')


def process_log_data(spark, input_data, output_data):
    """
    Read the songplay log json files into parquet tables
    :param spark: spark session
    :type spark: SparkSession
    :param input_data: path (local or s3) to prefix to log_data root
    :type input_data: str
    :param output_data: path (local or s3) to write output parquet files to
    :type output_data: str
    :return: None
    :rtype: None
    """
    # get filepath to log data file
    
    log_data = input_data + 'log_data/*.json'
    
    log_data_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Int()),
        Fld("lastName", Str()),
        Fld("length", Decimal()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Int(), nullable=False),
        Fld("song", Str()),
        Fld("status", Int()),
        Fld("ts", Long(), nullable=False),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])
    # read log data file
    df = spark.read.json(log_data, schema=log_data_schema)

    # filter by actions for song plays
    df = df.where(col('page') == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000.0, Dbl())
    df = df.withColumn('epoch_ts', get_timestamp(df.ts))
        
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimeStamp())
    df = df.withColumn('dt', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr('dt as start_time', 
                               'hour(dt) as hour', 
                               'dayofmonth(dt) as day', 
                               'weekofyear(dt) as week',
                               'month(dt) as month',
                               'year(dt) as year',
                               'dayofweek(dt) as weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    cond = [df.artist == song_df.artist_name, df.song == song_df.title]#, df.length == song_df.duration]

    # Join on artist name and song title match
    songplays_table = df.join(song_df, cond, 'inner').selectExpr('dt as start_time',
                                                                 'userId as user_id', 
                                                                 'level',
                                                                 'song_id',
                                                                 'artist_id',
                                                                 'sessionId as session_id',
                                                                 'location',
                                                                 'userAgent as user_agent'
                                                                ).withColumn('songplay_id', monotonically_increasing_id()) # For autoincrement primary key
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_table')

def main():
    spark = create_spark_session()
    #input_data = 'data_backup/'
    #output_data = 'output/'
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://myudacitydend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

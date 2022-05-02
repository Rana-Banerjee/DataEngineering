import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data ,'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','artist_id','artist_name','duration','title','year'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("artist_id", "year").parquet(output_data + "song_table.parquet")

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artist_table.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data ,'log_data/*/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where('page=="NextSong"')

    # extract columns for users table    
    artists_table = df.select(['userId', 'firstName','lastName','gender','level'])
    
    # write users table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "user_table.parquet")

    # create timestamp column from original timestamp column
    from pyspark.sql import types
    @udf (types.DateType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts/1000)
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    @udf (types.DateType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts/1000)
    df = df.withColumn('datetime', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr(['timestamp as start_time','hour(timestamp) as hour', 'dayofmonth(timestamp) as day', 
                            'weekofyear(timestamp) as week', 'month(timestamp) as month', 'year(timestamp) as year', 
                            'dayofweek(timestamp) as weekday'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'song_table.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist==song_df.artist_name),'inner')
    
    from pyspark.sql.functions import monotonically_increasing_id
    songplays_table=songplays_table.withColumn('songplay_id',monotonically_increasing_id())
    
    songplays_table=songplays_table.selectExpr(['songplay_id', 'timestamp as start_time',
                                            'userId as user_id', 'level', 'song_id', 'artist_id', 
                                            'sessionId as session_id', 'location', 'userAgent as user_agent','year(timestamp) as year', 'month(timestamp) as month'])
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://rana-datalake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()


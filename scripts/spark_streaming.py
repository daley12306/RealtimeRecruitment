from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, sum as _sum, count as _count, mean as _mean

if __name__ == '__main__':
    # Create Spark session
    spark = (SparkSession.builder
             .appName('RealtimeRecruitment')
             .master('spark://spark-master:7077')
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') # Spark SQL Kafka connector
             .config('spark.sql.adaptive.enabled', 'false') # Disable adaptive execution for better performance in streaming
             .getOrCreate())

    # Define the schema for the application data
    application_schema = StructType([
        StructField('id', StringType(), True),
        StructField('candidate_id', StringType(), True),
        StructField('position_id', IntegerType(), True),
        StructField('position_name', StringType(), True),
        StructField('status', StringType(), True),
        StructField('score', IntegerType(), True),
        StructField('experience_years', IntegerType(), True),
        StructField('submitted_at', TimestampType(), True),
        StructField('full_name', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('email', StringType(), True),
        StructField('phone', StringType(), True),
        StructField('city', StringType(), True),
        StructField('country', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('picture_url', StringType(), True),
        StructField('registered_at', TimestampType(), True)
    ])

    applications_df = (spark.readStream
                       .format('kafka')
                       .option('kafka.bootstrap.servers', 'broker:29092')
                       .option('subscribe', 'applications_topic')
                       .option('startingOffsets', 'earliest') # Start from the earliest offset, if use 'latest', it will only read new messages
                       .load()
                       .selectExpr('CAST(value AS STRING)') # .select(col('value').cast('string'))
                       .select(from_json(col('value'), application_schema).alias('data')) # Parse the JSON string into a struct and alias it as 'data' in dataframe
                       .select('data.*')) # Select all fields from the 'data' struct
    
    # Data preprocessing
    applications_df = (applications_df
                       .withColumn('submitted_at', col('submitted_at').cast(TimestampType())) 
                       .withColumn('registered_at', col('registered_at').cast(TimestampType())))
    enriched_applications_df = applications_df.withWatermark('submitted_at', '1 minute')

    # Aggregate
    applications_per_position = enriched_applications_df.groupBy('position_id', 'position_name') \
                                    .agg(_mean('score').alias('total_score'),
                                        _mean('experience_years').alias('total_experience_years'),
                                        _count('id').alias('application_count'),
                                        _mean('age').alias('average_age'))
    applications_per_score_range = enriched_applications_df.withColumn('score_range',
                                    (((col('score') - 30) / 10).cast('int') * 10 + 30)
                                ).groupBy('score_range').agg(_count('id').alias('application_count'))
    applications_per_experience_years = enriched_applications_df.withColumn(
                                    'experience_years_range',
                                    ((col('experience_years') / 2).cast('int') * 2)
                                ).groupBy('experience_years_range').agg(_count('id').alias('application_count'))

    applications_per_position_to_kafka = (applications_per_position
                                             .selectExpr('to_json(struct(*)) AS value')
                                             .writeStream
                                             .format('kafka')
                                             .option('kafka.bootstrap.servers', 'broker:29092')
                                             .option('topic', 'applications_per_position')
                                             .option('checkpointLocation', './checkpoint/checkpoint_position')
                                             .outputMode('update') # Use 'update' mode to update the state of the stream
                                             .start())
    
    applications_per_score_range_to_kafka = (applications_per_score_range
                                                .selectExpr('to_json(struct(*)) AS value')
                                                .writeStream
                                                .format('kafka')
                                                .option('kafka.bootstrap.servers', 'broker:29092')
                                                .option('topic', 'applications_per_score_range')
                                                .option('checkpointLocation', './checkpoint/checkpoint_score')
                                                .outputMode('update') # Use 'update' mode to update the state of the stream 
                                                .start())
    
    applications_per_experience_years_to_kafka = (applications_per_experience_years
                                                .selectExpr('to_json(struct(*)) AS value')
                                                .writeStream
                                                .format('kafka')
                                                .option('kafka.bootstrap.servers', 'broker:29092')
                                                .option('topic', 'applications_per_experience_years')
                                                .option('checkpointLocation', './checkpoint/checkpoint_experience')
                                                .outputMode('update') # Use 'update' mode to update the state of the stream
                                                .start())

    # Await termination
    applications_per_position_to_kafka.awaitTermination()
    applications_per_score_range_to_kafka.awaitTermination()
    applications_per_experience_years_to_kafka.awaitTermination()
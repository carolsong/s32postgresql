import os
import sys
import configparser
from pyspark.sql import functions as Func
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
import schema


POSTGRES_HOST = os.getenv('POSTGRES_HOST', '10.0.0.5')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'tonebnb')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'sa')
POSTGRES_PWD = os.getenv('POSTGRES_PWD', 'sa')
POSTGRES_URL = f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

def write_dataframe_to_postgres(df, table, mode):
    #print(f'postgres url: {POSTGRES_URL}')
    df = df.withColumn('incident_datetime', Func.to_timestamp(df.incident_datetime, format='YYYY/MM/DD HH24:MI:SS PM'))
    df = df.withColumn('incident_date', Func.to_timestamp(df.incident_date, format='YYYY/MM/DD'))
    df = df.withColumn('incident_time', Func.to_timestamp(df.incident_time, format='HH24:MI'))
    df = df.withColumn('incident_year', df.incident_year.cast('INT'))
    df = df.withColumn('incident_day_of_week', df.incident_day_of_week.cast('STRING'))
    df = df.withColumn('incident_id', df.incident_id.cast('INT'))
    df = df.withColumn('incident_category', df.incident_category.cast('STRING'))
    df = df.withColumn('incident_subcategory', df.incident_subcategory.cast('STRING'))
    df = df.withColumn('incident_description', df.incident_description.cast('STRING'))
    df = df.withColumn('resolution', df.resolution.cast('STRING'))
    df = df.withColumn('analysis_neighborhood', df.analysis_neighborhood.cast('STRING'))
    df = df.withColumn('latitude', df.latitude.cast('FLOAT'))
    df = df.withColumn('longitude', df.longitude.cast('FLOAT'))

    DataFrameWriter(df).jdbc(POSTGRES_URL, table, mode, {
        'user': POSTGRES_USER,
        'password': POSTGRES_PWD,
        'driver': 'org.postgresql.Driver'
    })

def write_dataframe_to_postgres2(df, table, mode):
    #print(f'postgres url: {POSTGRES_URL}')
    df = df.withColumn('listing_id', df.listing_id.cast('INT'))
    df = df.withColumn('id', df.id.cast('INT'))
    df = df.withColumn('date', Func.to_timestamp(df.date, format='YYYY-MM-DD'))
    df = df.withColumn('reviewer_id', df.reviewer_id.cast('INT'))
    df = df.withColumn('reviewer_name', df.reviewer_name.cast('STRING'))
    df = df.withColumn('comments', df.comments.cast('STRING'))
    #df = df.withColumn('tone', 0.0)

    DataFrameWriter(df).jdbc(POSTGRES_URL, table, mode, {
        'user': POSTGRES_USER,
        'password': POSTGRES_PWD,
        'driver': 'org.postgresql.Driver'
    })

def write_events_to_postgres(file):
    df = spark_sql(app="store-events-to-db").read.parquet(file)
    df = df.withColumn('ActionGeo_FeatureID', df.ActionGeo_FeatureID.cast('STRING'))
    df = df.withColumn('SOURCEURL', df.SOURCEURL.cast('STRING'))

    df = df.drop('FractionDate','DATEADDED')

    write_dataframe_to_postgres(df, 'events', 'append')


def spark_sql(app=None, mem='6gb'):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('default', 'aws_access_key_id')
    access_key = config.get('default', 'aws_secret_access_key')
    # initialize spark session
    spark = SparkSession.builder.appName(app).config('spark.executor.memory', mem).getOrCreate()
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    hadoop_conf.set('fs.s3a.awsAccessKeyId', access_id)
    hadoop_conf.set('fs.s3a.awsSecretAccessKey', access_key)

    for dirpath, dirnames, filenames in os.walk(os.path.dirname(os.path.realpath(__file__))):
        for file in filenames:
            if file.endswith('.py'):
                sc.addPyFile(os.path.join(dirpath, file))

    return SQLContext(sc)


def process_events():
    source = 's3a://data-harbor/safetyinfo/sf/2018_to_Present.csv'
    #target = 's3a://gdelt-dataharbor/eventfiles/event.2/parquet'
    df = spark_sql(app='process-incident-data').read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .option("quote", "\"")  \
        .option("escape", "\"") \
        .load(source, schema=schema.IncidentSchema().getIncidentSchema()) \
        .select('incident_datetime', 'incident_date', 'incident_time', 'incident_year', 'incident_day_of_week', 'incident_id',
                'incident_category', 'incident_subcategory', 'incident_description', 'resolution',
                'analysis_neighborhood', 'latitude', 'longitude')

    write_dataframe_to_postgres(df, 'INCIDENT3', 'append')

def process_airreview():
    source = 's3a://data-harbor/airbnb/sanfrancisco/reviews10.csv'
    #target = 's3a://data-harbor/airbnb/event.2/parquet'
    df = spark_sql(app='process-review-data').read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"')  \
        .options(escape='"')    \
        .load(source, schema=schema.IncidentSchema().getAirReviewSchema()) \
        .select('listing_id', 'id', 'date', 'reviewer_id', 'reviewer_name', 'comments')

    write_dataframe_to_postgres2(df, 'AIRREVIEW', 'append')

# df = spark_sql(app='process-incident-data').read \
    #     .format('com.databricks.spark.csv') \
    #     .options(header='true') \
    #     .options(delimiter=',') \
    #     .load(source, schema=schema.IncidentSchema().getIncidentSchema()) \
    #     .select('incident_datetime', 'incident_date', 'incident_time', 'incident_year', 'incident_day_of_week', 'incident_id',
    #             'incident_category', 'incident_subcategory', 'incident_description', 'resolution',
    #             'analysis_neighborhood', 'latitude', 'longitude', 'point')

# def process_mentions(date):
#     source = 's3a://gdelt-dataharbor/eventfiles/event.2/{}*'.format(date)
#     target = 's3a://gdelt-dataharbor/eventfiles/event.2/parquet-{}/'.format(date)
#     spark_sql(app='clean-mentions-data').read \
#         .format('com.databricks.spark.csv') \
#         .options(header='false') \
#         .options(delimiter='\t') \
#         .load(source, schema=schema.GDELTDataSchema().getMentionSchema()) \
#         .filter('Confidence > 50') \
#         .write.parquet(target)


if __name__ == '__main__':
    #process_events()
    process_airreview()
    #cmd = str(sys.argv[1])
    #date = str(sys.argv[2])
    #print(f'Processing data. Process command: {cmd}, date: {date}')

    #if cmd == 'incident':
    #    process_events()
    #elif cmd == 'review':
     #   process_airreview()
    #else:
     #   raise Exception("invalid command: {}".format(cmd))

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

def write_events_to_postgres(file):
    df = spark_sql(app="store-events-to-db").read.parquet(file)

    df = df.withColumn('GLOBALEVENTID', df.GLOBALEVENTID.cast('INT'))
    df = df.withColumn('SQLDATE', Func.to_timestamp(df.SQLDATE, format='YYYY-MM-DD HH24:MI:SS'))
    df = df.withColumn('MonthYear', df.MonthYear.cast('INT'))
    df = df.withColumn('Month', Func.month(df.SQLDATE))
    df = df.withColumn('Year', df.Year.cast('INT'))
    df = df.withColumn('Actor1Code', df.Actor1Code.cast('STRING'))
    df = df.withColumn('Actor1Name', df.Actor1Name.cast('STRING'))
    df = df.withColumn('Actor1CountryCode', df.Actor1CountryCode.cast('STRING'))
    df = df.withColumn('Actor1KnownGroupCode', df.Actor1KnownGroupCode.cast('STRING'))
    df = df.withColumn('Actor1EthnicCode', df.Actor1EthnicCode.cast('STRING'))
    df = df.withColumn('Actor1Religion1Code', df.Actor1Religion1Code.cast('STRING'))
    df = df.withColumn('Actor1Religion2Code', df.Actor1Religion2Code.cast('STRING'))
    df = df.withColumn('Actor1Type1Code', df.Actor1Type1Code.cast('STRING'))
    df = df.withColumn('Actor1Type2Code', df.Actor1Type2Code.cast('STRING'))
    df = df.withColumn('Actor1Type3Code', df.Actor1Type3Code.cast('STRING'))
    df = df.withColumn('Actor2Code', df.Actor2Code.cast('STRING'))
    df = df.withColumn('Actor2Name', df.Actor2Name.cast('STRING'))
    df = df.withColumn('Actor2CountryCode', df.Actor2CountryCode.cast('STRING'))
    df = df.withColumn('Actor2KnownGroupCode', df.Actor2KnownGroupCode.cast('STRING'))
    df = df.withColumn('Actor2EthnicCode', df.Actor2EthnicCode.cast('STRING'))
    df = df.withColumn('Actor2Religion1Code', df.Actor2Religion1Code.cast('STRING'))
    df = df.withColumn('Actor2Religion2Code', df.Actor2Religion2Code.cast('STRING'))
    df = df.withColumn('Actor2Type1Code', df.Actor2Type1Code.cast('STRING'))
    df = df.withColumn('Actor2Type2Code', df.Actor2Type2Code.cast('STRING'))
    df = df.withColumn('Actor2Type3Code', df.Actor2Type3Code.cast('STRING'))
    df = df.withColumn('IsRootEvent', df.IsRootEvent.cast('INT'))
    df = df.withColumn('EventCode', df.EventCode.cast('STRING'))
    df = df.withColumn('EventBaseCode', df.EventBaseCode.cast('STRING'))
    df = df.withColumn('EventRootCode', df.EventRootCode.cast('STRING'))
    df = df.withColumn('QuadClass', df.QuadClass.cast('INT'))
    df = df.withColumn('GoldsteinScale', df.GoldsteinScale.cast('FLOAT'))
    df = df.withColumn('NumMentions', df.NumMentions.cast('INT'))
    df = df.withColumn('NumSources', df.NumSources.cast('INT'))
    df = df.withColumn('NumArticles', df.NumArticles.cast('INT'))
    df = df.withColumn('AvgTone', df.AvgTone.cast('FLOAT'))
    df = df.withColumn('Actor1Geo_Type', df.Actor1Geo_Type.cast('INT'))
    df = df.withColumn('Actor1Geo_FullName', df.Actor1Geo_FullName.cast('STRING'))
    df = df.withColumn('Actor1Geo_CountryCode', df.Actor1Geo_CountryCode.cast('STRING'))
    df = df.withColumn('Actor1Geo_ADM1Code', df.Actor1Geo_ADM1Code.cast('STRING'))
    df = df.withColumn('Actor1Geo_ADM2Code', df.Actor1Geo_ADM2Code.cast('STRING'))
    df = df.withColumn('Actor1Geo_Lat', df.Actor1Geo_Lat.cast('FLOAT'))
    df = df.withColumn('Actor1Geo_Long', df.Actor1Geo_Long.cast('FLOAT'))
    df = df.withColumn('Actor1Geo_FeatureID', df.Actor1Geo_FeatureID.cast('STRING'))
    df = df.withColumn('Actor2Geo_Type', df.Actor2Geo_Type.cast('INT'))
    df = df.withColumn('Actor2Geo_FullName', df.Actor2Geo_FullName.cast('STRING'))
    df = df.withColumn('Actor2Geo_CountryCode', df.Actor2Geo_CountryCode.cast('STRING'))
    df = df.withColumn('Actor2Geo_ADM1Code', df.Actor2Geo_ADM1Code.cast('STRING'))
    df = df.withColumn('Actor2Geo_ADM2Code', df.Actor2Geo_ADM2Code.cast('STRING'))
    df = df.withColumn('Actor2Geo_Lat', df.Actor2Geo_Lat.cast('FLOAT'))
    df = df.withColumn('Actor2Geo_Long', df.Actor2Geo_Long.cast('FLOAT'))
    df = df.withColumn('Actor2Geo_FeatureID', df.Actor2Geo_FeatureID.cast('STRING'))
    df = df.withColumn('ActionGeo_Type', df.ActionGeo_Type.cast('INT'))
    df = df.withColumn('ActionGeo_FullName', df.ActionGeo_FullName.cast('STRING'))
    df = df.withColumn('ActionGeo_CountryCode', df.ActionGeo_CountryCode.cast('STRING'))
    df = df.withColumn('ActionGeo_ADM1Code', df.ActionGeo_ADM1Code.cast('STRING'))
    df = df.withColumn('ActionGeo_ADM2Code', df.ActionGeo_ADM2Code.cast('STRING'))
    df = df.withColumn('ActionGeo_Lat', df.ActionGeo_Lat.cast('FLOAT'))
    df = df.withColumn('ActionGeo_Long', df.ActionGeo_Long.cast('FLOAT'))
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
        .load(source, schema=schema.IncidentSchema().getIncidentSchema()) \
        .select('incident_datetime', 'incident_date', 'incident_time', 'incident_year', 'incident_day_of_week', 'incident_id',
                'incident_category', 'incident_subcategory', 'incident_description', 'resolution',
                'analysis_neighborhood', 'latitude', 'longitude')

    write_dataframe_to_postgres(df, 'INCIDENT3', 'append')

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
    process_events()
    #cmd = str(sys.argv[1])
    #date = str(sys.argv[2])
    #print(f'Processing data. Process command: {cmd}, date: {date}')

    #if cmd == 'events':
     #   process_events(date)
    #elif cmd == 'mentions':
     #   process_mentions(date)
    #else:
     #   raise Exception("invalid command: {}".format(cmd))

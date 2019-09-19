import os

from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

INCIDENT_SCHEMA = StructType([
    StructField('incident_datetime', StringType(), True),
    StructField('incident_date', StringType(), True),
    StructField('incident_time', StringType(), True),
    StructField('incident_year', StringType(), True),
    StructField('incident_day_of_week', StringType(), True),
    StructField('report_datetime', StringType(), True),
    StructField('row_id', StringType(), True),
    StructField('incident_id', StringType(), True),
    StructField('incident_number', StringType(), True),
    StructField('cad_number', StringType(), True),
    StructField('report_type_code', StringType(), True),
    StructField('report_type_description', StringType(), True),
    StructField('filed_online', StringType(), True),
    StructField('incident_code', StringType(), True),
    StructField('incident_category', StringType(), True),
    StructField('incident_subcategory', StringType(), True),
    StructField('incident_description', StringType(), True),
    StructField('resolution', StringType(), True),
    StructField('intersection', StringType(), True),
    StructField('cnn', StringType(), True),
    StructField('police_district', StringType(), True),
    StructField('analysis_neighborhood', StringType(), True),
    StructField('supervisor_district', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('point', StringType(), True)
])


def process():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/sf_incidents.csv")
    target = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/sf_incidents_recent.json")
    spark = SparkSession.builder.appName('process-incident-data').getOrCreate()
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .schema(INCIDENT_SCHEMA) \
        .load(source)
    df.createOrReplaceTempView("incidents")
    sql = """
        SELECT latitude, longitude FROM incidents 
          WHERE incident_datetime > '2019/08/30'
            AND latitude != ''
            AND longitude != ''
    """
    spark.sql(sql).coalesce(1).write.json(target)


if __name__ == '__main__':
    process()

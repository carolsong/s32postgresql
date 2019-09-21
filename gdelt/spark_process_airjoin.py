import os

from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from schema import *

# AIRSCHEMA = StructType([
#     StructField('id', IntegerType(), True),
#     StructField('name', StringType(), True),
#     StructField('host_id', IntegerType(), True),
#     StructField('host_name', StringType(), True),
#     StructField('neighbourhood_group', StringType(), True),
#     StructField('neighbourhood', StringType(), True),
#     StructField('latitude', DoubleType(), True),
#     StructField('longitude', DoubleType(), True),
#     StructField('room_type', StringType(), True),
#     StructField('price', IntegerType(), True),
#     StructField('minimum_nights', IntegerType(), True),
#     StructField('number_of_reviews', IntegerType(), True),
#     StructField('last_review', DateType(), True),
#     StructField('reviews_per_month', DoubleType(), True),
#     StructField('calculated_host_listings_count', IntegerType(), True),
#     StructField('availability_365', IntegerType(), True)
# ])


def process():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/listings*.csv")
    target = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/parquet")
    spark = SparkSession.builder.appName('process-incident-data').getOrCreate()
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .schema(AIRBNB_LISTING_SCHEMA) \
        .load(source)
    df.createOrReplaceTempView("listings")
    sql = """
        SELECT id, name, host_id, host_name, neighbourhood, latitude, longitude, room_type, price, minimum_nights, 
        number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365 
        FROM listings 
        WHERE availability_365 > 100
            AND latitude != ''
            AND longitude != ''
    """
    spark.sql(sql).coalesce(1).write.parquet(target)


if __name__ == '__main__':
    process()

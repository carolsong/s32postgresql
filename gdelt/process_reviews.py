import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

SCHEMA = StructType([
    StructField('listing_id', StringType(), True),
    StructField('id', StringType(), True),
    StructField('date', StringType(), True),
    StructField('reviewer_id', StringType(), True),
    StructField('reviewer_name', StringType(), True),
    StructField('comments', StringType(), True),
    StructField('tone', StringType(), True)
])


def process():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/reviews2.csv")
    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores("I don't like this place")['compound']
    score = analyser.polarity_scores("So much noise! I really have a negative impression")['compound']
    score = analyser.polarity_scores("really nice")['compound']
    score = analyser.polarity_scores("good attitude")['compound']
    score = analyser.polarity_scores("I got depressed, there is too much code to write")['compound']
    score = analyser.polarity_scores("they have two cats")['compound']

    #target = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../data/reviews2_recent.csv")
    spark = SparkSession.builder.appName('process-review-data').getOrCreate()

    df = spark.read \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(multiLine="true") \
        .options(escape='"') \
        .load(source, schema=SCHEMA)

    df.createOrReplaceTempView("reviews")

    sql = """
        SELECT id, listing_id, reviewer_name, comments FROM reviews 
    """
    spark.sql(sql).show()


if __name__ == '__main__':
    process()

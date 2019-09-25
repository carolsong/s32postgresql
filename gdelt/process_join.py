import os

from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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


def cleanListings():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/listings*.csv")
    target = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/listing")
    spark = SparkSession.builder.appName('clean-listing-data').getOrCreate()
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)
    df = df.withColumn('id', df.id.cast('INT'))
    df = df.withColumn('latitude', df.latitude.cast('DOUBLE'))
    df = df.withColumn('longitude', df.longitude.cast('DOUBLE'))
    df = df.withColumn('price', df.price.cast('INT'))
    df = df.withColumn('minimum_nights', df.minimum_nights.cast('INT'))
    df = df.withColumn('number_of_reviews', df.number_of_reviews.cast('INT'))

    df.createOrReplaceTempView("listings")

    # sql = """
    #     SELECT COUNT(DISTINCT id),
    #     COUNT(DISTINCT (id, name, host_name, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews))
    #     FROM listings
    #     WHERE latitude != '' AND longitude != ''
    # """
    # temp_df = spark.sql(sql)
    # print(temp_df.first())
    #
    # sql = """
    #     SELECT id, count(name) FROM (
    #     SELECT DISTINCT id, name, host_name, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews
    #     FROM listings D
    #     WHERE number_of_reviews = (SELECT MAX(number_of_reviews) FROM listings WHERE id = D.id)
    #     AND price = (SELECT MAX(price) FROM listings WHERE id = D.id))
    #     GROUP BY id
    #     ORDER BY count(name) DESC
    # """
    # sql = """
    #     SELECT distinct number_of_reviews FROM listings ORDER BY number_of_reviews DESC
    # """
    # temp_df = spark.sql(sql)
    # # temp_df.createOrReplaceTempView("cleaner-listings")
    # print(temp_df.show(100))

    sql = """
        SELECT DISTINCT id, name, host_name, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews
        FROM listings D
        WHERE number_of_reviews = (SELECT MAX(number_of_reviews) FROM listings WHERE id = D.id) AND id is not null
    """

    # temp_df = spark.sql(sql)
    # #temp_df.createOrReplaceTempView("cleaner-listings")
    # print(temp_df.show(10))

    spark.sql(sql).coalesce(1).write.parquet(target)

def cleanListingDetails():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/detaillistings*.csv")
    target = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/detailedlisting")
    spark = SparkSession.builder.appName('clean-listing-detail-data').getOrCreate()
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)

    df = df.withColumn('id', df.id.cast('INT'))
    df = df.withColumn('review_scores_rating', df.review_scores_rating.cast('INT'))

    df.createOrReplaceTempView("listings_detail")
    df.printSchema()
    # COUNT(DISTINCT id, listing_url, review_scores_rating, review_scores_accuracy,
    # review_scores_cleanliness, review_scores_checkin, review_scores_communication,
    # review_scores_location, review_scores_value), COUNT(id)  WHERE id is not null AND review_scores_rating <= 100

    # sql = """
    #     SELECT DISTINCT id, listing_url, review_scores_rating
    #     FROM listings
    #     WHERE review_scores_rating >100
    #     ORDER BY review_scores_rating DESC
    #
    # """
    # sql = """
    #         SELECT COUNT( distinct id, listing_url, review_scores_rating)
    #         FROM listings_detail
    #     """
    # temp_df = spark.sql(sql)
    # # temp_df.createOrReplaceTempView("cleaner-listings")
    # print(temp_df.show(100))
    #

    sql = """
        SELECT  distinct id, listing_url, review_scores_rating
        FROM listings_detail
        WHERE  review_scores_rating<100 AND  review_scores_rating>0
    """
    # temp_df = spark.sql(sql)
    # temp_df.createOrReplaceTempView("cleaner-listings")
    # print(temp_df.show(100))
#     sql = """
#             SELECT COUNT(id)
#             FROM listings_detail
#             WHERE review_scores_rating <100
#         """
#     temp_df = spark.sql(sql)
#     #    temp_df.createOrReplaceTempView("cleaner-listings")
#     print(temp_df.first())
    # sql = """
    #         SELECT COUNT(id)
    #         FROM listings_detail
    #     """
    # temp_df = spark.sql(sql)
    # #    temp_df.createOrReplaceTempView("cleaner-listings")
    # print(temp_df.first())
    spark.sql(sql).coalesce(1).write.parquet(target)


def cleanReview():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/review*.csv")
    target = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/review")
    spark = SparkSession.builder.appName('process-listing-data').getOrCreate()
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)

    df = df.withColumn('listing_id', df.listing_id.cast('INT'))

    df.createOrReplaceTempView("reviews")
    sql = """
        SELECT DISTINCT listing_id, date, comments
        FROM reviews
        WHERE id is not null
    """
    # temp_df = spark.sql(sql)
    # #    temp_df.createOrReplaceTempView("cleaner-listings")
    #print(temp_df.first())
    spark.sql(sql).coalesce(1).write.parquet(target)

def reviewtest():
    source = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/reviews1.csv")
    target = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/review1")
    spark = SparkSession.builder.appName('process-listing-data').getOrCreate()
    df = spark.read.format('com.databricks.spark.csv') \
        .options(header='true') \
        .options(delimiter=',') \
        .options(quote='"') \
        .options(escape='"') \
        .options(multiLine='true') \
        .load(source)

    df = df.withColumn('listing_id', df.listing_id.cast('INT'))

    df.createOrReplaceTempView("reviews")
    sql = """
        SELECT DISTINCT listing_id, date, comments
        FROM reviews
        WHERE id is not null
    """
    # temp_df = spark.sql(sql)
    # #    temp_df.createOrReplaceTempView("cleaner-listings")
    #print(temp_df.first())
    spark.sql(sql).coalesce(1).write.parquet(target)
    review_df = spark.read.parquet("/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/review1") \
        .select('listing_id', 'comments')
    # review_df.createOrReplaceTempView("reviews")
    # temp_df = spark.sql('select listing_id, comments, 0.0 as toneScore \
    #                      FROM reviews')
    # temp_df = temp_df.withColumn('toneScore', temp_df.toneScore.cast('DOUBLE'))
    # temp_df.printSchema()

    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores("really nice")['compound']

    tone_udf = udf(lambda x:  analyser.polarity_scores(x)['compound'], DoubleType())

    new_df = review_df.withColumn("score", tone_udf(review_df.comments))
    new_df.createOrReplaceTempView("reviews")
    temp_df = spark.sql('select listing_id, SUM(score)/COUNT(listing_id) as avgTone \
                          FROM reviews GROUP BY listing_id')
    temp_df.show(20)


def dothejoin():
    spark = SparkSession.builder.appName('process-listing-data').getOrCreate()

    listing_df = spark.read.parquet("/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/listing") \
        .select('id', 'neighbourhood', 'number_of_reviews')
    # listing_detail_df = spark.read.parquet("/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/detailedlisting") \
    #     .select('id', 'listing_url', 'scrape_id', 'review_scores_rating', 'review_scores_accuracy', 'review_scores_cleanliness', \
    #             'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 'review_scores_value')
    listing_detail_df = spark.read.parquet("/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/detailedlisting") \
        .select('id', 'review_scores_rating')
    review_df = spark.read.parquet("/Users/libosong/desktop/data/brandnew/airbnb/localtest/parquet/review") \
        .select('listing_id', 'comments')
    # listing_df.printSchema()
    # listing_detail_df.printSchema()
    # review_df.printSchema()

    listing_df.createOrReplaceTempView("listings")
    listing_detail_df.createOrReplaceTempView("listing_detail")
    review_df.createOrReplaceTempView("reviews")

    temp_df = spark.sql('select neighbourhood, max(number_of_reviews) as max_number FROM listings group by neighbourhood')
    neighbourhood_dict = {}
    for row in temp_df.rdd.collect():
        neighbourhood_dict.update({row['neighbourhood'].strip(): row['max_number']})
    # print(neighbourhood_dict['Irwindale'])
    # print(neighbourhood_dict)

    temp_df = spark.sql('select listings.ID, 0.05 * SUM(review_scores_rating)/COUNT(listings.ID) as avgRating \
                             FROM listings INNER JOIN listing_detail \
                             ON listings.ID = listing_detail.ID \
                             GROUP BY listings.ID')

    temp_df = spark.sql('select listing_id, comments, 0.0 as toneScore \
                         FROM reviews')
    temp_df = temp_df.withColumn('toneScore', temp_df.toneScore.cast('DOUBLE'))
    temp_df.printSchema()

    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores("really nice")['compound']

    name = 'toneScore'
    udf = UserDefinedFunction(lambda x: analyser.polarity_scores(row['comments'])['compound'], DoubleType())
    new_df = temp_df.select(*[udf(column).alias(name) if column == name else column for column in temp_df.columns])

    temp_df = spark.sql('select listing_id, SUM(score)/COUNT(listing_id) as avgTone \
                          FROM reviews GROUP BY listing_id')
    print(temp_df.show(20))


    # temp_df = spark.sql('select listing_id, SUM(score)/COUNT(listing_id) as avgTone \
    #                      FROM reviews GROUP BY listing_id')
    #
    # # temp_df = spark.sql('select count(listing_id), count(distinct listing_id), count(distinct(listing_id, comments)) FROM reviews')
    # print(temp_df.rdd.count())
    # print(temp_df.show(50))

    # temp_df = spark.sql('select count(id),count(distinct id) FROM listings \
    #                     WHERE id is not null and latitude is not null and longitude is not null')
    # print(temp_df.first())
    # temp_df = spark.sql('select count(id), count(distinct id) FROM listing_detail WHERE id is not null')
    # print(temp_df.first())
    # temp_df = spark.sql('select count(listing_id), count(distinct listing_id), count(distinct(listing_id, date)) \
    #                     FROM reviews WHERE listing_id is not null')
    # print(temp_df.first())
    # temp_df = spark.sql('select count(listing_id + date), listing_id + date \
    #                     FROM reviews \
    #                     group by listing_id + date \
    #                     order by count(date) DESC')
    # print(temp_df.show(10))
    #
    # temp_df = spark.sql('select count(id ), id  \
    #                     FROM listings \
    #                     group by id \
    #                     order by count(id) DESC')
    # print(temp_df.show(10))
    #
    # temp_df = spark.sql('select * \
    #                     FROM listings \
    #                     WHERE id like \'66796\'')
    #
    # #temp_df = spark.sql('select distinct(*) \
    # #                      FROM reviews')
    #
    # print(temp_df.show(100))

    spark.catalog.dropTempView('listings')
    spark.catalog.dropTempView('listing_detail')
    spark.catalog.dropTempView('reviews')

    listing_df.unpersist()
    listing_detail_df.unpersist()
    review_df.unpersist()
    #temp_df.unpersist()

    # temp_df = spark.sql('select count(distinct id) FROM listings')
    # print(temp_df.first())
    #
    # temp_df = spark.sql('SELECT count(distinct (listings.id, name, review_scores_rating ))\
    #                     FROM listings inner join listing_detail on listings.ID = listing_detail.ID')
    # temp_df = spark.sql('SELECT COUNT(distinct (listings.ID, review_scores_rating ))\
    #                     FROM listings inner join listing_detail on listings.ID = listing_detail.ID \
    #                     WHERE review_scores_rating>100')
    # print(temp_df.first())

if __name__ == '__main__':
    # cleanListings()
    # cleanListingDetails()
    # cleanReview()
    #  dothejoin()
    reviewtest()

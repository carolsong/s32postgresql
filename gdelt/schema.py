from pyspark.sql.types import *
from pyspark.sql.types import StringType


class GDELTDataSchema(object):

    def getEventSchema(self):
        eventSchema = StructType([
            StructField('GLOBALEVENTID',StringType(),True),
            StructField('SQLDATE',StringType(),True),
            StructField('MonthYear',StringType(),True),
            StructField('Year',StringType(),True),
            StructField('FractionDate',StringType(),True),
            StructField('Actor1Code',StringType(),True),
            StructField('Actor1Name',StringType(),True),
            StructField('Actor1CountryCode',StringType(),True),
            StructField('Actor1KnownGroupCode',StringType(),True),
            StructField('Actor1EthnicCode',StringType(),True),
            StructField('Actor1Religion1Code',StringType(),True),
            StructField('Actor1Religion2Code',StringType(),True),
            StructField('Actor1Type1Code',StringType(),True),
            StructField('Actor1Type2Code',StringType(),True),
            StructField('Actor1Type3Code',StringType(),True),
            StructField('Actor2Code',StringType(),True),
            StructField('Actor2Name',StringType(),True),
            StructField('Actor2CountryCode',StringType(),True),
            StructField('Actor2KnownGroupCode',StringType(),True),
            StructField('Actor2EthnicCode',StringType(),True),
            StructField('Actor2Religion1Code',StringType(),True),
            StructField('Actor2Religion2Code',StringType(),True),
            StructField('Actor2Type1Code',StringType(),True),
            StructField('Actor2Type2Code',StringType(),True),
            StructField('Actor2Type3Code',StringType(),True),
            StructField('IsRootEvent',StringType(),True),
            StructField('EventCode',StringType(),True),
            StructField('EventBaseCode',StringType(),True),
            StructField('EventRootCode',StringType(),True),
            StructField('QuadClass',StringType(),True),
            StructField('GoldsteinScale',StringType(),True),
            StructField('NumMentions',StringType(),True),
            StructField('NumSources',StringType(),True),
            StructField('NumArticles',StringType(),True),
            StructField('AvgTone',StringType(),True),
            StructField('Actor1Geo_Type',StringType(),True),
            StructField('Actor1Geo_FullName',StringType(),True),
            StructField('Actor1Geo_CountryCode',StringType(),True),
            StructField('Actor1Geo_ADM1Code',StringType(),True),
            StructField('Actor1Geo_ADM2Code',StringType(),True),
            StructField('Actor1Geo_Lat',StringType(),True),
            StructField('Actor1Geo_Long',StringType(),True),
            StructField('Actor1Geo_FeatureID',StringType(),True),
            StructField('Actor2Geo_Type',StringType(),True),
            StructField('Actor2Geo_FullName',StringType(),True),
            StructField('Actor2Geo_CountryCode',StringType(),True),
            StructField('Actor2Geo_ADM1Code',StringType(),True),
            StructField('Actor2Geo_ADM2Code',StringType(),True),
            StructField('Actor2Geo_Lat',StringType(),True),
            StructField('Actor2Geo_Long',StringType(),True),
            StructField('Actor2Geo_FeatureID',StringType(),True),
            StructField('ActionGeo_Type',StringType(),True),
            StructField('ActionGeo_FullName',StringType(),True),
            StructField('ActionGeo_CountryCode',StringType(),True),
            StructField('ActionGeo_ADM1Code',StringType(),True),
            StructField('ActionGeo_ADM2Code',StringType(),True),
            StructField('ActionGeo_Lat',StringType(),True),
            StructField('ActionGeo_Long',StringType(),True),
            StructField('ActionGeo_FeatureID',StringType(),True),
            StructField('DATEADDED',StringType(),True),
            StructField('SOURCEURL',StringType(),True)])
        return eventSchema

    def getMentionSchema(self):
        mentionSchema =  StructType([
            StructField('GLOBALEVENTID',StringType(),True),
            StructField('EventTimeDate',StringType(),True),
            StructField('MentionTimeDate',StringType(),True),
            StructField('MentionType',StringType(),True),
            StructField('MentionSourceName',StringType(),True),
            StructField('MentionIdentifier',StringType(),True),
            StructField('SentenceID',StringType(),True),
            StructField('Actor1CharOffset',StringType(),True),
            StructField('Actor2CharOffset',StringType(),True),
            StructField('ActionCharOffset',StringType(),True),
            StructField('InRawText',StringType(),True),
            StructField('Confidence',StringType(),True),
            StructField('MentionDocLen',StringType(),True),
            StructField('MentionDocTone',StringType(),True),
            StructField('MentionDocTranslationInfo',StringType(),True),
            StructField('Extras',StringType(),True)])
        return mentionSchema

    def getGkgSchema(self):
        gkgSchema = StructType([
            StructField('GKGRECORDID',StringType(),True),
            StructField('DATE',StringType(),True),
            StructField('SourceCollectionIdentifier',StringType(),True),
            StructField('SourceCommonName',StringType(),True),
            StructField('DocumentIdentifier',StringType(),True),
            StructField('Counts',StringType(),True),
            StructField('V2Counts',StringType(),True),
            StructField('Themes',StringType(),True),
            StructField('V2Themes',StringType(),True),
            StructField('Locations',StringType(),True),
            StructField('V2Locations',StringType(),True),
            StructField('Persons',StringType(),True),
            StructField('V2Persons',StringType(),True),
            StructField('Organizations',StringType(),True),
            StructField('V2Organizations',StringType(),True),
            StructField('V2Tone',StringType(),True),
            StructField('Dates',StringType(),True),
            StructField('GCAM',StringType(),True),
            StructField('SharingImage',StringType(),True),
            StructField('RelatedImageEmbeds',StringType(),True),
            StructField('SocialImageEmbeds',StringType(),True),
            StructField('SocialVideoEmbeds',StringType(),True),
            StructField('Quotations',StringType(),True),
            StructField('AllNames',StringType(),True),
            StructField('Amounts',StringType(),True),
            StructField('TranslationInfo',StringType(),True),
            StructField('Extras',StringType(),True)])
        return gkgSchema

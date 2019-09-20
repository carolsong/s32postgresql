from pyspark.sql.types import *
from pyspark.sql.types import StringType

class IncidentSchema(object):

    def getIncidentSchema(self):
        incidentSchema = StructType([
            StructField('incident_datetime',StringType(),True),
            StructField('incident_date',StringType(),True),
            StructField('incident_time',StringType(),True),
            StructField('incident_year',StringType(),True),
            StructField('incident_day_of_week',StringType(),True),
            StructField('report_datetime',StringType(),True),
            StructField('row_id',StringType(),True),
            StructField('incident_id',StringType(),True),
            StructField('incident_number',StringType(),True),
            StructField('cad_number',StringType(),True),
            StructField('report_type_code',StringType(),True),
            StructField('report_type_description',StringType(),True),
            StructField('filed_online',StringType(),True),
            StructField('incident_code',StringType(),True),
            StructField('incident_category',StringType(),True),
            StructField('incident_subcategory',StringType(),True),
            StructField('incident_description',StringType(),True),
            StructField('resolution',StringType(),True),
            StructField('intersection',StringType(),True),
            StructField('cnn',StringType(),True),
            StructField('police_district',StringType(),True),
            StructField('analysis_neighborhood',StringType(),True),
            StructField('supervisor_district',StringType(),True),
            StructField('latitude',StringType(),True),
            StructField('longitude',StringType(),True),
            StructField('point',StringType(),True)])
        return incidentSchema

    def getAirReviewSchema(self):
        airreviewSchema = StructType([
            StructField('listing_id',StringType(),True),
            StructField('id',StringType(),True),
            StructField('date',StringType(),True),
            StructField('reviewer_id',StringType(),True),
            StructField('reviewer_name',StringType(),True),
            StructField('comments',StringType(),True),
            StructField('tone',StringType(),True)])
        return airreviewSchema

class GDELTDataSchema(object):

    def getEventSchema(self):
        eventSchema = StructType([
            StructField('GLOBALEVENTID',StringType(),True),
            StructField('SOURCEURL',StringType(),True)])
        return eventSchema

    def getMentionSchema(self):
        mentionSchema =  StructType([
            StructField('GLOBALEVENTID',StringType(),True),
           StructField('MentionDocTranslationInfo',StringType(),True),
            StructField('Extras',StringType(),True)])
        return mentionSchema

    def getGkgSchema(self):
        gkgSchema = StructType([
            StructField('GKGRECORDID',StringType(),True),
          StructField('Extras',StringType(),True)])
        return gkgSchema

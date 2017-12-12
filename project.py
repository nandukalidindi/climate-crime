%spark.pyspark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

crime_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/tmp/climate/rows.csv")
climate_df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true").load("/tmp/climate/climate1.csv")



# Date conversion

crime_df_date = crime_df.select("CMPLNT_FR_DT", "CMPLNT_FR_TM", "OFNS_DESC", "LAW_CAT_CD", "Latitude", "Longitude", "PD_DESC")

def crime_date_convert(date, time):
	k = date.split("/")
	full_date = "-".join(k[2:] + k[1:2] + k[0:1])
	full_date = full_date + " " + time.split(":")[0]
	return full_date


udf_crime_date_convert = udf(crime_date_convert, StringType())

with_crime_date_conversion = crime_df_date.withColumn("normalized_date", udf_crime_date_convert("CMPLNT_FR_DT", "CMPLNT_FR_TM")).select("normalized_date", "OFNS_DESC", "LAW_CAT_CD", "Latitude", "Longitude", "PD_DESC")



climate_df_date = climate_df.select("DATE", "HOURLYDRYBULBTEMPC")

def climate_date_convert(date):
	return date.split(":")[0]

udf_climate_date_convert = udf(climate_date_convert, StringType())

climate_with_date_conversion = climate_df_date.withColumn("normalized_date", udf_climate_date_convert("DATE")).select("normalized_date", "HOURLYDRYBULBTEMPC")

join_climate_crime = climate_with_date_conversion.join(with_crime_date_conversion, "normalized_date")

cleaned_join_climate_crime = join_climate_crime.na.drop()


def get_hour(date):
	return date.split(" ")[1]

def get_date(date):
	return date.split(" ")[0]


udf_get_hour = udf(get_hour, StringType())

udf_get_date = udf(get_date, StringType())


with_refined_date_columns = cleaned_join_climate_crime.withColumn("date", udf_get_date("normalized_date")).withColumn("hour", udf_get_hour("normalized_date"))



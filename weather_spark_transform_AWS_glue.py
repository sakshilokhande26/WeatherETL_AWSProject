
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, date_format, unix_timestamp
from pyspark.sql import functions as F
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_path = "s3://weather-etl-project-sakshi/raw_data/to-process/"
source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [s3_path]}
)

weather_df = source_dyf.toDF()
def process_astro(df):
    df_astro = df.select(col("location.name").alias("city"), explode(col("forecast.forecastday")).alias("forecast")).select(
                                    date_format(unix_timestamp("forecast.astro.sunrise", 'hh:mm a').cast("timestamp"), 'HH:mm:ss').alias("sunrise"),
                                    date_format(unix_timestamp("forecast.astro.sunset", 'hh:mm a').cast("timestamp"), 'HH:mm:ss').alias("sunset"),
                                    date_format(unix_timestamp("forecast.astro.moonrise", 'hh:mm a').cast("timestamp"), 'HH:mm:ss').alias("moonrise"),
                                    date_format(unix_timestamp("forecast.astro.moonset", 'hh:mm a').cast("timestamp"), 'HH:mm:ss').alias("moonset"),
                                    col("forecast.astro.moon_phase").alias("moon_phase"),
                                    col("forecast.date").alias("date"),
                                    col("city"))
    return df_astro
def process_hour(df):

    df_forecast_exploded = df.select(col("location.name").alias("city"), explode(col("forecast.forecastday")).alias("forecastday"))

    df_hour = df_forecast_exploded.select("city",explode(col("forecastday.hour")).alias("hour")).select(
                col("city"),
                F.to_date(col("hour.time")).alias("Date"),
                F.date_format("hour.time", "HH:mm:ss").alias("time"),
                col("hour.temp_c").alias("Temperature"),
                col("hour.condition.text").alias("Condition"),
                col("hour.wind_kph").alias("Wind Speed"),
                col("hour.humidity").alias("Humidity"),
                col("hour.cloud").alias("Cloud"),
                col("hour.feelslike_c").alias("Feels Like"),
                col("hour.uv").alias("UV"),
                col("hour.wind_dir").alias("Wind Direction"),
                col("hour.vis_km").alias("Visibility")
    )

    return df_hour
def process_city(df):
    df_city = df.select(col("location.name").alias("City"),
                 col("location.region").alias("Region"),
                 col("location.country").alias("Country"),
                 col("location.lat").alias("Latitude"),
                 col("location.lon").alias("Longitude"),
                 col("location.tz_id").alias("Timezone"),
                 col("location.localtime").alias("Local Time"))
    return df_city
astro_df = process_astro(weather_df)
city_df = process_city(weather_df)
hourly_forecast_df = process_hour(weather_df)
astro_df.show(5)
city_df.show(5)
hourly_forecast_df.show(5)
def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    glueContext.write_dynamic_frame_from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": f"s3://weather-etl-project-sakshi/transformed_data/{path_suffix}/"},
        format = format_type
    )
write_to_s3(astro_df, "astro/astro_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
write_to_s3(city_df, "city/city_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
write_to_s3(hourly_forecast_df, "hourly_forecast/hourly_forecast_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
job.commit()
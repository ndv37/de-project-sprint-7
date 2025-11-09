import pyspark.sql.functions as F
from pyspark.sql.window import Window


def load_data(date, depth, geo_path, events_path, spark):
    geo_df = spark.read.csv(geo_path, header=True, sep=";", inferSchema=True)
    geo_df = (
        geo_df
        .withColumn("lat", F.regexp_replace("lat", ",", ".").cast("double"))
        .withColumn("lng", F.regexp_replace("lng", ",", ".").cast("double"))
        .withColumnRenamed("lat", "city_lat")
        .withColumnRenamed("lng", "city_lng")
        .withColumnRenamed("id", "id")
    )
    end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
    events_df = spark.read.parquet(events_path).filter(F.col("date").between(F.date_sub(end_date, depth), end_date))
    return geo_df, events_df


def distance_to_center(lat_col1, lon_col1, lat_col2, lon_col2):
    distance = (2 * F.lit(radius) * F.asin(F.sqrt(
        F.pow(F.sin((F.radians(lat_col1) - F.radians(lat_col2)) / 2), 2) +
        F.cos(F.radians(lat_col1)) * F.cos(F.radians(lat_col2)) *
        F.pow(F.sin((F.radians(lon_col1) - F.radians(lon_col2)) / 2), 2))))
    return distance

radius = 6371

def geo_event_data(events_df, geo_df):
    joined_df = events_df.select(
        "event.message_id",
        F.col("event.message_from").alias("user_id"),
        F.to_timestamp("event.datetime").alias("datetime"),
        F.col("lat"),
        F.col("lon"),
        "date",
        "event_type"
    ).crossJoin(
        geo_df.select(
            F.col("id").alias("zone_id"),
            "city",
            "city_lat",
            "city_lng",
            F.concat(F.lit("Australia/"), "city").alias("timezone")
        )
    )
    window = Window().partitionBy(["message_id"]).orderBy("distance")
    geo_event_df = (
        joined_df.withColumn(
            "distance", distance_to_center(F.col('lat'), F.col('lon'), F.col('city_lat'), F.col('city_lng'))
        )
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
    )
    return geo_event_df


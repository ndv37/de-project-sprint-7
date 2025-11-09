import os
import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from user_function import load_data, distance_to_center, geo_event_data

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

findspark.init()
findspark.find()

spark = (SparkSession
         .builder
         .master('local')
         .appName(f'Recommendation datamart')
         .getOrCreate())

def process_dm_recommendations(events_df, geo_event_df):
    subs_df = (
        events_df.where("event.subscription_channel is not null")
        .select(F.col("event.user").alias("user_id"), "event.subscription_channel")
        .distinct()
    )
    subs_pairs_df = subs_df.join(
        subs_df.select(F.col("user_id").alias("user_right"), "subscription_channel"),
        "subscription_channel",
    )

    messages_df = (
        events_df.where(
            "event.message_from is not null and event.message_to is not null"
        )
        .select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_to").alias("user_right"),
        )
        .distinct()
    )
    messages_pairs_df = messages_df.union(
        messages_df.select(
            F.col("user_id").alias("user_right"),
            F.col("user_right").alias("user_id")
        )
    )

    window = Window().partitionBy(["user_id"]).orderBy(F.desc("datetime"))
    local_time_df = (
        geo_event_df.select("user_id", "datetime", "timezone")
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
        .withColumn(
            "local_time", F.from_utc_timestamp(F.col("datetime"), F.col("timezone"))
        )
        .drop("datetime", "timezone", "rn")
    )

    window = Window().partitionBy(["user_id", "date"]).orderBy(F.desc("datetime"))
    user_dt_coord_df = (
        geo_event_df.select(
            "user_id", "date", "datetime", "lat", "lon", "city", "zone_id"
        )
        .withColumn("rn", F.row_number().over(window))
        .where("rn=1")
        .select("user_id", "date", "lat", "lon", "city", "zone_id")
    )

    users_df = user_dt_coord_df.join(
        user_dt_coord_df.select(
            F.col("user_id").alias("user_right"),
            "date",
            F.col("lat").alias("lat_right"),
            F.col("lon").alias("lon_right"),
            F.col("city").alias("city_right"),
            F.col("zone_id").alias("zone_id_rigth")
        ),
        "date",
    )

    users_distance_df = users_df.withColumn(
        "distance", distance_to_center(F.col('lat'), F.col('lon'), F.col('lat_right'), F.col('lon_right'))
    ).where("distance<=1 and user_id<user_right")

    dm_recommendations_df = (
        users_distance_df.join(subs_pairs_df, on=["user_id", "user_right"], how="leftsemi")
        .join(messages_pairs_df, on=["user_id", "user_right"], how="leftanti")
        .join(local_time_df, on="user_id", how="left")
        .select(
            F.col("user_id").alias("user_left"),
            "user_right",
            F.col("date").alias("processed_dttm"),
            F.col("zone_id").alias("zone_id"),
        )
    )

    return dm_recommendations_df

def main():
    date = "2022-05-31"
    depth = 3
    geo_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/geo.csv"
    events_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/master/data/geo/events"
    dm_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/analytics/dm_recommendations'
    geo_df, events_df = load_data(date, depth, geo_path, events_path, spark)
    geo_event_df = geo_event_data(events_df, geo_df)

    dm_recommendations_df = process_dm_recommendations(events_df, geo_event_df)
    dm_recommendations_df.write.parquet(f"{dm_path}/date={date}/depth={depth}")


if __name__ == "__main__":
    main()
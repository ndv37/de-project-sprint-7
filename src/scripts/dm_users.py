import sys
import datetime
import math

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from user_function import load_data, geo_event_data
import findspark

findspark.init()
findspark.find()

spark = (SparkSession
         .builder
         .master('local')
         .appName(f'Users datamart')
         .getOrCreate())


def process_dm_users(geo_event_df):
    events_with_city_df = (geo_event_df.select('user_id', 'datetime', 'event_type', 'lat', 'lon', 'date', 'city', 'timezone')
        .withColumn('datetime_ts', F.to_timestamp(F.substring('datetime', 1, 19), 'yyyy-MM-dd HH:mm:ss'))
        )
    window_act = Window.partitionBy('user_id').orderBy(F.col('datetime_ts').desc_nulls_last())
    events_with_city_df = events_with_city_df.withColumn('rn_act', F.row_number().over(window_act))
    last_event_city_df = (
        events_with_city_df
        .filter(F.col('rn_act') == 1)
        .select('user_id', F.col('city').alias('act_city'), 'datetime_ts', 'timezone')
        .withColumn('local_time', F.from_utc_timestamp('datetime_ts', F.col('timezone')))
    )

    daily_df = events_with_city_df.groupBy('user_id', 'city', 'date').agg(F.count('*').alias('cnt'))
    window_daily = Window.partitionBy('user_id', 'city').orderBy('date')
    daily_df = (
        daily_df
        .withColumn('rnk', F.row_number().over(window_daily))
        .withColumn('day_idx', F.year('date') * 400 + F.dayofyear('date'))
        .withColumn('offset', F.col('day_idx') - F.col('rnk'))
    )

    duration_df = (
        daily_df.groupBy("user_id", "city", "offset")
        .agg(
            F.count("*").alias("duration_length"),
            F.max("date").alias("last_date_in_duration")
        )
        .filter(F.col("duration_length") >= 27)
    )

    window_duration = Window.partitionBy("user_id").orderBy(F.col("last_date_in_duration").desc())
    home_city_df = (
        duration_df
        .withColumn("rownum", F.row_number().over(window_duration))
        .filter(F.col("rownum") == 1)
        .select("user_id", F.col("city").alias("home_city"))
    )

    window_travel = Window.partitionBy("user_id").orderBy("datetime_ts").rowsBetween(Window.unboundedPreceding,
                                                                                     Window.unboundedFollowing)
    events_with_city_df = events_with_city_df.withColumn("city_list", F.collect_list("city").over(window_travel))

    window_desc = Window.partitionBy("user_id").orderBy(F.col("datetime_ts").desc_nulls_last())
    events_with_city_df = events_with_city_df.withColumn("rn2", F.row_number().over(window_desc))
    travel_df = (
        events_with_city_df
        .filter(F.col("rn2") == 1)
        .select("user_id", F.col("city_list").alias("travel_array"))
    )
    travel_df = travel_df.withColumn(
        "travel_count",
        F.size(F.array_distinct("travel_array"))
    )

    dm_users_df = (
        last_event_city_df
        .join(home_city_df, on="user_id", how="left")
        .join(travel_df, on="user_id", how="left")
        .select("user_id","act_city","home_city","travel_array","travel_count","local_time")
    )
    return dm_users_df

def main():
    date = '2022-05-31'
    depth = 3
    geo_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/geo.csv'
    events_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/master/data/geo/events'
    dm_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/analytics/dm_users'
    geo_df, events_df = load_data(date, depth, geo_path, events_path, spark)
    geo_event_df = geo_event_data(events_df, geo_df)

    users_df = process_dm_users(geo_event_df)
    #users_df.write.parquet(f"{dm_path}/date={date}/depth={depth}")


if __name__ == "__main__":
    main()
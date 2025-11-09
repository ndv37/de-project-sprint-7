import os
import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from user_function import load_data, geo_event_data

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'

findspark.init()
findspark.find()

spark = (SparkSession
         .builder
         .master('local')
         .appName(f'Zones datamart')
         .getOrCreate())


def process_dm_zones(geo_event_df):
    window_month = Window().partitionBy(['month', 'city'])
    zones_df = (
        geo_event_df.select('user_id', 'datetime', 'event_type', 'city', 'zone_id')
        .withColumn('month', F.trunc(F.col('datetime'), 'month'))
        .withColumn('week', F.trunc(F.col('datetime'), 'week'))
        .withColumn(
            'month_message',
            F.sum(F.when(geo_event_df.event_type == 'message', 1).otherwise(0)).over(window_month)
        )
        .withColumn(
            'month_reaction',
            F.sum(F.when(geo_event_df.event_type == 'reaction', 1).otherwise(0)).over(window_month)
        )
        .withColumn(
            'month_subscription',
            F.sum(F.when(geo_event_df.event_type == 'subscription', 1).otherwise(0)).over(window_month)
        )
        .withColumn('month_user', F.size(F.collect_set('user_id').over(window_month)))
        .withColumn('message', F.when(geo_event_df.event_type == 'message', 1).otherwise(0))
        .withColumn('reaction', F.when(geo_event_df.event_type == 'reaction', 1).otherwise(0))
        .withColumn('subscription', F.when(geo_event_df.event_type == 'subscription', 1).otherwise(0))
        .groupBy(
            'month','week','zone_id','month_message','month_reaction','month_subscription','month_user'
        )
        .agg(
            F.sum('message').alias('week_message'),
            F.sum('reaction').alias('week_reaction'),
            F.countDistinct('user_id').alias('week_user')
        )
    )
    return zones_df

def main():
    date = '2022-06-01'
    depth = 3
    geo_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/geo.csv'
    events_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/master/data/geo/events'
    dm_path = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/analytics/dm_zones'
    geo_df, events_df = load_data(date, depth, geo_path, events_path, spark)
    geo_event_df = geo_event_data(events_df, geo_df)

    zones_df = process_dm_zones(geo_event_df)
    zones_df.write.parquet(f'{dm_path}/date={date}/depth={depth}')


if __name__ == '__main__':
    main()
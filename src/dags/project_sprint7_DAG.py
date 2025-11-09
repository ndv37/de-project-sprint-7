from datetime import datetime
import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"


default_args = {
    'owner': 'airflow',
    'start_date':datetime(2025, 10, 11),
}

dag = DAG(
    dag_id = "project_sprint7_DAG",
    default_args=default_args,
    schedule_interval=None
)

step1 = SparkSubmitOperator(
    task_id="dm_users",
    dag=dag,
    application="/lessons/src/dm_users.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "3",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/geo.csv",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/master/data/geo/events",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/analytics/dm_users",
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_memory="2g",
    executor_cores=2,
)

step2 = SparkSubmitOperator(
    task_id="dm_zones",
    dag=dag,
    application="/lessons/src/dm_zones.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "3",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/geo.csv",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/master/data/geo/events",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/analytics/dm_zones",
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_memory="2g",
    executor_cores=2,
)

step3 = SparkSubmitOperator(
    task_id="dm_recommendations",
    dag=dag,
    application="/lessons/src/dm_recs.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "3",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/geo.csv",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/master/data/geo/events",
        "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net//user/naumovdv/data/analytics/dm_recs",
    ],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_memory="2g",
    executor_cores=2,
)

step1 >> step2 >> step3
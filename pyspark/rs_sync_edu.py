from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from clickhouse_driver import Client

import os
import pandas as pd
import json
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

# Нужно указать, чтобы spark подгрузил lib для kafka.
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.0 --packages org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.0 pyspark-shell'

# Загружаем конекты. Не выкладываем в гит файл с конектами.
with open('/opt/spark/Streams/credentials.json') as json_file:
    сonnect_settings = json.load(json_file)

ch_db_name = "default"
ch_dst_table = "rid_status"

client = Client(сonnect_settings['ch_local'][0]['host'],
                user=сonnect_settings['ch_local'][0]['user'],
                password=сonnect_settings['ch_local'][0]['password'],
                verify=False,
                database=ch_db_name,
                settings={"numpy_columns": True, 'use_numpy': True},
                compression=True)

# Разные переменные, задаются в params.json
spark_app_name = "ridstatus"
spark_ui_port = "8081"

kafka_host = сonnect_settings['kafka'][0]['host']
kafka_port = сonnect_settings['kafka'][0]['port']
#kafka_user = сonnect_settings['kafka'][0]['user']
#kafka_password = сonnect_settings['kafka'][0]['password']
kafka_topic = "ridstatus"
kafka_batch_size = 500000
processing_time = "50 second"

checkpoint_path = f'/opt/kafka_checkpoint_dir/{spark_app_name}/{kafka_topic}/v1'

# Создание сессии спарк.
spark = SparkSession \
    .builder \
    .appName(spark_app_name) \
    .config('spark.ui.port', spark_ui_port) \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.executor.cores", "1") \
    .config("spark.task.cpus", "1") \
    .config("spark.num.executors", "1") \
    .config("spark.executor.instances", "1") \
    .config("spark.default.parallelism", "1") \
    .config("spark.cores.max", "1") \
    .config('spark.ui.port', spark_ui_port) \
    .getOrCreate()

# убираем разные Warning.
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.debug.maxToStringFields", 500)

# Описание как создается процесс spark structured streaming.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{kafka_host}:{kafka_port}") \
    .option("subscribe", kafka_topic) \
    .option("maxOffsetsPerTrigger", kafka_batch_size) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .load()
    #.option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";') \
    #.option("kafka.sasl.mechanism", "PLAIN") \
    #.option("kafka.security.protocol", "SASL_PLAINTEXT") \

# Колонки, которые писать в ClickHouse. В kafka много колонок, не все нужны. Этот tuple нужен перед записью в ClickHouse.
columns_to_ch = ("rid", "shk_id", "dt", "chrt_id", "employee_id", "src_office_id", "dst_office_id", "sm_id", "entry", "status_id")


# Схема сообщений в топике kafka. Используется при формировании batch.
schema = StructType([
    StructField("rid", StringType(), False),
    StructField("shk_id", LongType(), True),
    StructField("dt", StringType(), False),
    StructField("chrt_id", LongType(), True),
    StructField("employee_id", LongType(), True), 
    StructField("src_office_id", LongType(), True),
    StructField("dst_office_id", LongType(), True),
    StructField("sm_id", LongType(), True),
    StructField("entry", StringType(), False),
    StructField("status_id", LongType(), False)

])

sql_tmp_create = f"""create table tmp.rid_status
    (
        rid           String,
        shk_id        Int64,
        dt            DateTime,
        chrt_id       UInt32,
        employee_id   UInt32,
        src_office_id UInt32,
        dst_office_id UInt32,
        sm_id         UInt16,
        entry LowCardinality(String),
        status_id     UInt8
    )
        engine = Memory
"""

sql_insert = f"""insert into {ch_db_name}.{ch_dst_table}
    select rid
        , shk_id
        , toString(dt)
        , chrt_id
        , employee_id
        , src_office_id
        , dst_office_id
        , sm_id
        , entry
        , status_id
        , status_name
    from tmp.{ch_dst_table} t1
    left any join
    (
        select status_id
            , status_name
        from default.OrderStatus
    ) t2
    on t1.status_id = t2.status_id
"""

client.execute(f"drop table if exists tmp.{ch_dst_table}")

def column_filter(df):
    # select только нужные колонки.
    col_tuple = []
    for col in columns_to_ch:
        col_tuple.append(f"value.{col}")
    return df.selectExpr(col_tuple)


def load_to_ch(df):
    # Преобразуем в dataframe pandas и записываем в ClickHouse.
    df_pd = df.toPandas()
    # df_pd.dt = df_pd.dt.str.slice(0, 10)
    df_pd.dt = pd.to_datetime(df_pd.dt, format='%Y-%m-%d %H:%M:%S', errors='ignore')

    client.insert_dataframe(f'INSERT INTO tmp.{ch_dst_table} VALUES', df_pd)

# Функция обработки batch. На вход получает dataframe-spark.
def foreach_batch_function(df2, epoch_id):

    df_rows = df2.count()
    # Если dataframe не пустой, тогда продолжаем.

    if df_rows > 0:
        # df2.printSchema()
        # df2.show(5)

        # Убираем не нужные колонки.
        df2 = column_filter(df2)

        client.execute(sql_tmp_create)

        # Записываем dataframe в ch.
        load_to_ch(df2)

        # Добавляем объем и записываем в конечную таблицу.
        client.execute(sql_insert)
        client.execute(f"drop table if exists tmp.{ch_dst_table}")


# Описание как создаются микробатчи. processing_time - задается вначале скрипта
query = df.select(from_json(col("value").cast("string"), schema).alias("value")) \
    .writeStream \
    .trigger(processingTime=processing_time) \
    .option("checkpointLocation", checkpoint_path) \
    .foreachBatch(foreach_batch_function) \
    .start()

query.awaitTermination()

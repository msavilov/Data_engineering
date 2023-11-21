import os
import warnings
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lag, lit, round, row_number

warnings.filterwarnings('ignore')


spark = SparkSession.builder.appName(
    'dz_spark_sql').master('local[2]').getOrCreate()

df = (
    spark.read.
    option('header', True).
    option('sep', ',').
    option('inferSchema', True).
    csv('owid-covid-data.csv')
)

# Task_1
date = '2021-03-31'

filt_df = df.select(
    'date',
    'iso_code',
    'location',
    'total_cases_per_million',
).filter(col('date') == date).filter(~col('iso_code').contains('OWID'))

result_1 = filt_df.select(
    'iso_code',
    'location',
    (round((col('total_cases_per_million') / 10000), 2)).alias(
        'percent cases, %')).orderBy(
            col('percent cases, %').desc()).limit(15)


# Task_2
df_last_week_march = df.select(
    'location',
    'new_cases',
    'date',
    ).filter((col('date') >= '2021-03-25')
             & (col('date') <= '2021-03-31')).filter(
                 ~col('iso_code').contains('OWID'))

window = Window().partitionBy('location').orderBy(col('new_cases').desc())

result_2 = df_last_week_march.withColumn(
    'row_number',
    row_number().over(window)
).filter(col('row_number') == '1').select(
    'location',
    'new_cases',
    'date',
).orderBy(col('new_cases').desc()).limit(10)


# Task_3

df_last_week_march_rus = df.select(
    'date',
    (col('new_cases').alias('today_new_cases')),
).filter((col('date') >= '2021-03-24')
         & (col('date') <= '2021-03-31')).filter(col('iso_code') == 'RUS')

window = Window().partitionBy(lit(0)).orderBy('date')

result_3 = df_last_week_march_rus.withColumn(
    'prev_day_new_cases',
    lag(col('today_new_cases'), 1).over(window)).withColumn(
    'delta_new_cases',
    (col('today_new_cases')
     - col('prev_day_new_cases'))).filter(col('date') > '2021-03-24')


# Save results

path = f'{os.getcwd()}/results/'

for n, item in enumerate([result_1, result_2, result_3]):
    item.write.option(
        'header',
        True).csv(f'{path}result_{n + 1}_{datetime.now().date()}')

spark.stop()

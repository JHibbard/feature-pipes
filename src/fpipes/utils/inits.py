# Standard Libraries
import multiprocessing

# External Libraries
import pyspark


_cpu_count = multiprocessing.cpu_count()

builder = (
    pyspark.sql.SparkSession.builder
      .master(f'local[{_cpu_count}]')
      .appName('app')
      .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
)

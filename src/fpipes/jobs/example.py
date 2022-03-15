from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DateType, TimestampType, IntegerType, FloatType, BooleanType
from delta import DeltaTable


# base class
class Job(ABC):

    def __init__(self, spark=None, init_conf=None):
        self.spark = self._prepare_spark(spark)
        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config()

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def _provide_config(self):
        return {}

    @abstractmethod
    def launch(self):
        pass


# schema file, e.g. ../schemas/invoice.py
VERSION = '1.0.0'
SCHEMA = StructType([
    StructField('invoice_number', IntegerType(), True, {'comment': 'invoice number'}, ),
    StructField('invoice_date', DateType(), True, {'comment': 'date invoice was received'}),
    # ...
])


# job file
class Example(Job):

    def launch(self):
        # Feature Table Config
        table_name = self.conf['table_name']
        table_comment = self.conf['table_comment']
        table_location = self.conf['table_location']

        # Source Config
        source = self.conf['source']
        source_format = self.conf['source_format']

        # Streaming Config
        read_options = self.conf['read_options']
        write_options = self.conf['write_options']
        trigger = self.conf['trigger']

        # Create Feature Table if Absent
        if not DeltaTable.isDeltaTable(self.spark, table_location):
            # DeltaTable.createOrReplace(self.spark) \
            DeltaTable.createIfNotExists(self.spark) \
                .tableName(table_name) \
                .comment(table_comment) \
                .location(table_location) \
                .addColumn('datasource', StringType(), comment='name of input file record was sourced from') \
                .addColumn('ingesttime', TimestampType(), comment='timestamp for when record was ingested') \
                .addColumns(SCHEMA) \
                .addColumn('p_ingestdate', DateType(), comment='(partition column) date record was ingested') \
                .partitionedBy('p_ingestdate') \
                .execute()

            # Update Feature
            example_bronze_query = self.spark.readStream \
                .format(source_format) \
                .schema(SCHEMA) \
                .options(**read_options) \
                .load(source) \
                .select(
                    f.input_file_name().alias('datasource'),
                    f.current_timestamp().alias('ingesttime'),
                    '*',
                    f.current_date.alias('p_ingestdate'),
                ) \
                .writeStream \
                    .trigger(**trigger) \
                    .format('delta') \
                    .options(**write_options, path=table_location) \
                    .start()

            example_bronze_query.status
            example_bronze_query.processAllAvailable()
            example_bronze_query.stop()
            # example_bronze_query.status
            # example_bronze_query.lastProgress()

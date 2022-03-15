from delta import DeltaTable

Class Example(Job):

    def launch(self):
        # table config
        table_name = self.conf['table_name']
        table_location = self.conf['table_location']

        if not DeltaTable.isDeltaTable(self.spark, table_location):
            # create table
            DeltaTable.createIfNotExists(self.spark) \
                .tableName(table_name) \
                .comment(table_comment) \
                .location(table_location) \
                .addColumn('datasource', StringType(), comment='name of input file record was sourced from') \
                .addColumn('ingesttime', TimeStamp(), comment='timestamp for when record was ingested') \
                .addColumns(SCHEMA) \
                .addColumn('p_ingestdate', DateType(), comment='(partition column) date record was ingested') \
                .partitionedBy('p_ingestdate') \
                .execute()

            example_query = self.spark.readStream \
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

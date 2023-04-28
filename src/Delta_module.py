from delta.tables import *


class DeltaModule:
    def __init__(self, spark, delta_table_path):
        self.spark = spark
        self.delta_table_path = delta_table_path
        self.delta_table = DeltaTable.forPath(spark, delta_table_path)

    def merge_data(self, new_data_df, merge_condition):
        self.delta_table.alias("old_data") \
            .merge(new_data_df.alias("new_data"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    def write_to_delta(self, data_df):
        data_df.write.format("delta").mode("overwrite").save(self.delta_table_path)

    def load_delta(self):
        return self.spark.read.format("delta").load(self.delta_table_path)
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import Union


class DeltaModule:
    def __init__(self, spark: SparkSession, delta_table_path: str):
        self.spark = spark
        self.delta_table_path = delta_table_path
        self.delta_table = DeltaTable.forPath(spark, delta_table_path)

    def merge_data(self, new_data_df: DataFrame, merge_condition: Union[str, Column]) -> None:
        self.delta_table.alias("old_data") \
            .merge(new_data_df.alias("new_data"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    def write_to_delta(self, df: DataFrame) -> None:
        df.write.format("delta").mode("overwrite").save(self.delta_table_path)

    def load_delta(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.delta_table_path)
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, when, split, to_date
from pyspark.sql.types import IntegerType


class SparkUtils:
    @staticmethod
    def create_spark_session(app_name: str, mysql_connector_path: str) -> SparkSession:
        conf = SparkConf().setAppName(app_name).setMaster("local[*]")
        conf.set("spark.logConf", "true")
        conf.set("spark.driver.extraClassPath", mysql_connector_path)
        conf.set("spark.executor.extraClassPath", mysql_connector_path)
        return SparkSession.builder.config(conf=conf).getOrCreate()

    @staticmethod
    def convert_to_int(value: str) -> DataFrame:
        if value.endswith('k'):
            value = value.replace('k', '')
            if '.' in value:
                value = float(value) * 1000
            else:
                value = int(value) * 1000
        return int(value)

    @staticmethod
    def convert_column_to_bool(df: DataFrame, column_name: str) -> DataFrame:
        return df.withColumn(column_name, when(df[column_name] == 1, True).otherwise(False))
    
    @staticmethod
    def extract_date_event(df: DataFrame, column: str) -> DataFrame:
        df = df.withColumn("Event", split(df[column], " ")[1])
        df = df.withColumn("Date", to_date(split(df[column], " ")[0], "yyyy-MM-dd"))
        return df
    
    @staticmethod
    def extract_kda(df: DataFrame, column: str) -> DataFrame:
        df = df.withColumn("Kills", split(df[column], "/")[0].cast("int"))
        df = df.withColumn("Deads", split(df[column], "/")[1].cast("int"))
        df = df.withColumn("Assists", split(df[column], "/")[2].cast("int"))
        df = df.drop(column)
        return df
    
    @staticmethod
    def create_row_id_column(df: DataFrame, column1: str, column2: str = None) -> DataFrame:
        if column2 is None:
            return df.withColumn("RowId", df[column1].astype("string"))
        else:
            return df.withColumn("RowId", df[column1].astype("string") + df[column2].astype("string"))

convert_to_int_udf = udf(SparkUtils.convert_to_int, IntegerType())
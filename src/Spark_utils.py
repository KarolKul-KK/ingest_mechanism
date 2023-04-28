from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when
from pyspark.sql.types import IntegerType


class SparkUtils:
    @staticmethod
    def create_spark_session(app_name, mysql_connector_path):
        conf = SparkConf().setAppName(app_name).setMaster("local[*]")
        conf.set("spark.logConf", "true")
        conf.set("spark.driver.extraClassPath", mysql_connector_path)
        conf.set("spark.executor.extraClassPath", mysql_connector_path)
        return SparkSession.builder.config(conf=conf).getOrCreate()

    @staticmethod
    def convert_to_int(value):
        if value.endswith('k'):
            value = value.replace('k', '')
            if '.' in value:
                value = float(value) * 1000
            else:
                value = int(value) * 1000
        return int(value)

    @staticmethod
    def convert_column_to_bool(df, column_name):
        return df.withColumn(column_name, when(df[column_name] == 1, True).otherwise(False))

convert_to_int_udf = udf(SparkUtils.convert_to_int, IntegerType())
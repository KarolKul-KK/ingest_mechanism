from src.Config import ConfigLoader
from src.MySql_connector import PyMySQL
from src.Riot_api import RiotAPI, DataDragonAPI
from src.Spark_utils import SparkUtils, convert_to_int_udf
from src.Delta_module import DeltaModule


def run_pipeline(generator, spark):
    while True:
        try:
            key, credenstials, tables_info = next(generator)
            pymysql = PyMySQL(credenstials['host'], credenstials['username'], credenstials['password'], credenstials['database'])
            if key == 'sql':
                df = pymysql.read_data_to_spark(spark, tables_info['name'], tables_info['query'])
            if tables_info['name'] == 'general_stats':
                df = SparkUtils.extract_date_event(df, 'Date')
            if tables_info['name'] == 'team_stats':
                df = SparkUtils.convert_column_to_bool(df, 'First_Blood')
                df = SparkUtils.convert_column_to_bool(df, 'First_Tower')
                df = df.withColumn("Gold", convert_to_int_udf(df["Gold"]))
            if tables_info['name'] == 'player_stats':
                df = SparkUtils.extract_kda(df, 'KDA')
            elif key == 'api':
                # handle api case
                pass
            elif key == 'file':
                # handle file case
                pass
        except StopIteration:
            print('End of pipeline')
            break
        except FileNotFoundError as e:
            print(f"File Error: {e}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    spark = SparkUtils.create_spark_session('ingest_mechanism', '/home/jovyan/mysql-connector-java-8.0.27')
    config = ConfigLoader('configs/', 'job_config.yml')
    generator = config.config_generator()
    run_pipeline(generator, spark)
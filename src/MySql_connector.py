import pymysql
from pyspark import SparkContext
from pyspark.sql import SQLContext
from typing import Dict, List, Tuple, Any


class PyMySQL:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()
        
    def _clean_query(self, query: str) -> str:
        return query.strip()

    def execute_query(self, query: str) -> List[Tuple[Any]]:
        query = self._clean_query(query)
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_data(self, table: str, data: Dict[str, Any]) -> None:
        placeholders = ', '.join(['%s'] * len(data))
        columns = ', '.join(data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.cursor.execute(query, tuple(data.values()))
        self.connection.commit()

    def read_data_to_spark(self, spark, table: str, query: str = None):
        url = f"jdbc:mysql://{self.host}/{self.database}"
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        if query:
            query = self._clean_query(query)
            df = spark.read.jdbc(url=url, table=f"({query}) as tmp", properties=properties)
        else:
            df = spark.read.jdbc(url=url, table=table, properties=properties)
        return df
    
    def close_connection(self) -> None:
        self.cursor.close()
        self.connection.close()
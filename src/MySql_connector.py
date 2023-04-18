import pymysql
from typing import Dict, List, Tuple, Any


class PyMySQL:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query: str) -> List[Tuple[Any]]:
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def insert_data(self, table: str, data: Dict[str, Any]) -> None:
        placeholders = ', '.join(['%s'] * len(data))
        columns = ', '.join(data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.cursor.execute(query, tuple(data.values()))
        self.connection.commit()
    
    def close_connection(self) -> None:
        self.cursor.close()
        self.connection.close()
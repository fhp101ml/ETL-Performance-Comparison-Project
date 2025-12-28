from etl_framework.utils.database.database_connection import DatabaseConnection
from etl_framework.abstract_etl_methods import Extractor
import pandas as pd


class SQLiteExtractor(Extractor):
    def __init__(self, db_path, query):
        self.connection = DatabaseConnection('sqlite', db_path)
        self.query = query

    def extract(self):
        print(self.connection, self.query)
        data = pd.read_sql_query(self.query, self.connection)
        return data


class MongoDBExtractor(Extractor):
    def __init__(self, uri, db_name, collection_name, query):
        self.client = DatabaseConnection('mongodb', uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.query = query

    def extract(self):
        data = pd.DataFrame(list(self.collection.find(self.query)))
        return data


class MySQLExtractor(Extractor):
    def __init__(self, host, user, password, database, query):
        self.connection = DatabaseConnection('mysql', host=host, user=user,
                                             password=password, database=database)
        self.query = query

    def extract(self):
        data = pd.read_sql_query(self.query, self.connection)
        return data


class PostgreSQLExtractor(Extractor):
    def __init__(self, host, user, password, database, query):
        self.connection = DatabaseConnection('postgresql', host=host, user=user,
                                             password=password, dbname=database)
        self.query = query

    def extract(self):
        data = pd.read_sql_query(self.query, self.connection)
        return data

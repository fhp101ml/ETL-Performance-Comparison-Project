from src.etl.utils.database.database_connection import  DatabaseConnection
from src.etl.abstract_etl_methods import Loader
import pandas as pd
import sqlite3

class SQLiteLoader(Loader):
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def create_table(self, data):
        # Create table if it doesn't exist
        columns = ', '.join([f'{col} TEXT' for col in data.columns])
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns})")
        self.connection.commit()

    def insert_data(self, data, exclude_columns=None):
            try:
                # Obtener las columnas del DataFrame
                columns = list(data.columns)

                # Determinar las columnas a insertar (excluyendo las especificadas)
                insert_columns = [col for col in columns if col not in exclude_columns]

                # Preparar los datos para insertar
                for index, row in data.iterrows():
                    values = tuple(row[col] for col in insert_columns)
                    query = f"INSERT INTO {self.table_name} ({', '.join(insert_columns)}) VALUES ({', '.join(['?']*len(insert_columns))})"
                    self.cursor.execute(query, values)

                # Confirmar los cambios
                self.connection.commit()
                print("Datos insertados correctamente en SQLite.")
            except Exception as e:
                print(f"Error al insertar datos en SQLite: {e}")

    def load(self, data):
        try:
            self.connect()
            self.create_table(data)
            self.insert_data(data, 'ID')
            print(f"Datos cargados en la base de datos SQLite {self.db_path}, tabla: {self.table_name}")
        except Exception as e:
            print(f"Error al cargar datos en SQLite: {e}")
        finally:
            self.close()

class MongoDBLoader(Loader):
    def __init__(self, uri, db_name, collection_name):
        self.client = DatabaseConnection('mongodb', uri=uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def load(self, data):
        self.collection.insert_many(data.to_dict('records'))
        print(f"Datos cargados en MongoDB, colección: {self.collection.name}")



class MySQLLoader(Loader):
    def __init__(self, host, user, password, database, table_name):
        self.connection = DatabaseConnection('mysql', host=host, user=user, password=password, database=database)
        self.table_name = table_name

    def load(self, data):
        conn = self.connection
        cursor = conn.cursor()

        # Create table if it doesn't exist
        columns = ', '.join([f'{col} TEXT' for col in data.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns})")
        print('insertion')
        # Insert data
        for index, row in data.iterrows():
            values = ', '.join([f"'{str(value)}'" for value in row])
            print(values)
            cursor.execute(f"INSERT INTO {self.table_name} VALUES ({values})")

        conn.commit()
        cursor.close()
        print(f"Datos cargados en la base de datos MySQL, tabla: {self.table_name}")



class PostgreSQLLoader(Loader):
    def __init__(self, host, user, password, database, table_name):
        self.connection = DatabaseConnection('postgresql', host=host, user=user, password=password, database=database)
        self.table_name = table_name

    def load(self, data):
        conn = self.connection
        cursor = conn.cursor()

        # Create table if it doesn't exist
        columns = ', '.join([f'{col} TEXT' for col in data.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns})")

        # Insert data
        for index, row in data.iterrows():
            values = ', '.join([f"'{str(value)}'" for value in row])
            cursor.execute(f"INSERT INTO {self.table_name} VALUES ({values})")

        conn.commit()
        cursor.close()
        print(f"Datos cargados en la base de datos PostgreSQL, tabla: {self.table_name}")

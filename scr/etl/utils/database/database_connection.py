import sqlite3
from pymongo import MongoClient
import mysql.connector
import psycopg2

class DatabaseConnection:
    _instances = {}
    _details = {}

    def __new__(cls, db_type, *args, **kwargs):
        if db_type not in cls._instances:
            if db_type == 'sqlite':
                cls._instances[db_type] = sqlite3.connect(*args, **kwargs)
            elif db_type == 'mongodb':
                cls._instances[db_type] = MongoClient(*args, **kwargs)
            elif db_type == 'mysql':
                cls._instances[db_type] = mysql.connector.connect(*args, **kwargs)
            elif db_type == 'mariadb':
                cls._instances[db_type] = mysql.connector.connect(*args, **kwargs)
            elif db_type == 'postgresql':
                cls._instances[db_type] = psycopg2.connect(*args, **kwargs)
            else:
                raise ValueError("Unknown database type")
            cls._details[db_type] = kwargs

        return cls._instances[db_type]

    @classmethod
    def get_connection_details(cls, db_type):
        if db_type in cls._details:
            return cls._details[db_type]
        else:
            raise ValueError("No details found for the specified database type")

    @classmethod
    def close_connection(cls, db_type):
        if db_type in cls._instances:
            instance = cls._instances[db_type]
            if db_type == 'sqlite':
                instance.close()
            elif db_type == 'mongodb':
                instance.close()
            elif db_type in ['mysql', 'mariadb']:
                instance.close()
            elif db_type == 'postgresql':
                instance.close()
            del cls._instances[db_type]
            del cls._details[db_type]
        else:
            raise ValueError(f"No connection found for {db_type}")

# ### el patrón singleton pervive mientras dure la sesión o se cierre específicamente la instancia de la conexión

# # Ejemplo de uso
# if __name__ == "__main__":
#     # Crear una conexión a una base de datos SQLite
#     sqlite_conn = DatabaseConnection('sqlite', db_path='testing.db')
#     # Obtener los detalles de la conexión SQLite
#     sqlite_details = DatabaseConnection.get_connection_details('sqlite')
#     print("SQLite details:", sqlite_details)

#     # Crear una conexión a una base de datos MongoDB
#     mongodb_conn = DatabaseConnection('mongodb', host='localhost', port=27017)
#     # Obtener los detalles de la conexión MongoDB
#     mongodb_details = DatabaseConnection.get_connection_details('mongodb')
#     print("MongoDB details:", mongodb_details)

#     # Crear una conexión a una base de datos MySQL
#     mysql_conn = DatabaseConnection('mysql', user='root', password='password', host='localhost', database='testdb')
#     # Obtener los detalles de la conexión MySQL
#     mysql_details = DatabaseConnection.get_connection_details('mysql')
#     print("MySQL details:", mysql_details)
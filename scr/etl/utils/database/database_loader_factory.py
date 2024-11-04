from scr.etl.utils.database.load_functions import *


class DatabaseLoaderFactory:
    def get_loader(self, **kwargs):
        print('factoryLoaderDatabase------------')
        db_type = kwargs['db_type']
        print(db_type, kwargs)
        if db_type == 'sqlite':
            return SQLiteLoader(kwargs['db_path'], kwargs['table_name'])
        elif db_type == 'mongodb':
            return MongoDBLoader(kwargs['uri'], kwargs['db_name'], kwargs['collection_name'])
        elif db_type == 'mysql':
            return MySQLLoader(kwargs['host'], kwargs['user'], kwargs['password'], kwargs['database'], kwargs['table_name'])
        elif db_type == 'postgresql':
            return PostgreSQLLoader(kwargs['host'], kwargs['user'], kwargs['password'], kwargs['database'], kwargs['table_name'])
        else:
            raise ValueError("Unsupported database type")
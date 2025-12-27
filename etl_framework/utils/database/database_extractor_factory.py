from etl_framework.utils.database.extract_functions import *


class DatabaseExtractorFactory:
    def get_extractor(self, **kwargs):
        print('factoryExtractorDatabase------------')
        print(kwargs)
        if kwargs["db_type"] == "sqlite":
            return SQLiteExtractor(kwargs['db_path'], kwargs['query'])
        elif  kwargs["db_type"] == "mongodb":
            return MongoDBExtractor(kwargs['uri'], kwargs['db_name'], kwargs['collection_name'], kwargs['query'])
        elif  kwargs["db_type"] == "mysql":
            return MySQLExtractor(kwargs['host'], kwargs['user'], kwargs['password'], kwargs['database'], kwargs['query'])
        elif  kwargs["db_type"] == "postgresql":
            return PostgreSQLExtractor(kwargs['host'], kwargs['user'], kwargs['password'], kwargs['database'], kwargs['query'])
        else:
            raise ValueError("Unknown database type")
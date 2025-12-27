import pandas as pd
import yaml

from src.etl.utils.database.load_functions import SQLiteLoader


def main():

    db_path = '/home/ETL-PCP/data/dataBaseTest/testing_SqliteLoader.db'
    table_name = 'usuarios'

    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age' : [25, 30, 35]
    })


    loader = SQLiteLoader(db_path, table_name)
    loader.load(data)


if __name__ == "__main__":
    main()
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from etl_framework.utils.database.load_functions import SQLiteLoader


def main():

    # Using a relative path for the test database
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, 'data_test', 'testing_SqliteLoader.db')
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
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
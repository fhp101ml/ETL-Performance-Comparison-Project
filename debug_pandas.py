import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'src'))
from etl_framework.library_pandas.pandas_etl_factory import PandasETLFactory

print("Import successful")

class MockMeta:
    pass

try:
    factory = PandasETLFactory(MockMeta())
    print("Instantiation successful")
    extractor = factory.get_extractor("csv", file_path="dummy.csv", separator=";")
    print("Extractor creation successful")
except Exception as e:
    print(f"Error: {e}")

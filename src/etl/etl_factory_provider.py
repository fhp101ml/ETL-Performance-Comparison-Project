from src.etl.library_pandas.pandas_etl_factory import PandasETLFactory
try:
    from src.etl.library_vaex.vaex_etl_factory import VaexETLFactory
    _VAEX_AVAILABLE = True
except ImportError:
    _VAEX_AVAILABLE = False
    
from src.etl.library_polars.polars_etl_factory import PolarsETLFactory
from src.etl.library_duckdb.duckdb_etl_factory import DuckDBETLFactory
from src.etl.library_dask.dask_etl_factory import DaskETLFactory

class ETLFactoryProvider:
    @staticmethod
    def get_factory(library_type, metadata):
        if library_type == "pandas":
            return PandasETLFactory(metadata)
        elif library_type == "vaex":
            if not _VAEX_AVAILABLE:
                raise ImportError("Vaex library is not installed.")
            return VaexETLFactory(metadata)
        elif library_type == "polars":
            return PolarsETLFactory(metadata)
        elif library_type == "duckdb":
            return DuckDBETLFactory(metadata)
        elif library_type == "dask":
            return DaskETLFactory(metadata)
        else:
            raise ValueError("Unknown library type")

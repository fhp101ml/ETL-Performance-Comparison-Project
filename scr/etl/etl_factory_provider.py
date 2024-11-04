from scr.etl.library_pandas.pandas_etl_factory import PandasETLFactory
from scr.etl.library_vaex.vaex_etl_factory import VaexETLFactory


class ETLFactoryProvider:
    @staticmethod
    def get_factory(library_type, metadata):
        if library_type == "pandas":
            return PandasETLFactory(metadata)
        elif library_type == "vaex":
            return VaexETLFactory(metadata)
        else:
            raise ValueError("Unknown library type")

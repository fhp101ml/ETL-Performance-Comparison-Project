from setuptools import setup, find_packages

setup(
    name="my_etl_framework",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "polars",
        "duckdb",
        "dask[complete]",
        "pyarrow",
        "pyyaml",
        "faker"
    ],
    author="Antigravity",
    description="Engine-agnostic ETL framework supporting Pandas, Polars, DuckDB, and Dask",
)

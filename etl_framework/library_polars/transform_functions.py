from etl_framework.abstract_etl_methods import Transformer
import polars as pl

class PolarsSimpleTransformer(Transformer):
    def transform(self, data):
        data.columns = [col.lower() for col in data.columns]
        return data

class PolarsComplexTransformer(Transformer):
    def transform(self, data):
        # Polars way: use expressions
        # data['NEW_COLUMN'] = data.apply(lambda row: row.sum(), axis=1) in pandas
        # In polars, sum horizontal can be tricky, typically explicitly summing list of columns.
        # Assuming numeric columns.
        numeric_cols = [col for col, dtype in zip(data.columns, data.dtypes) if dtype in (pl.Int64, pl.Float64, pl.Int32, pl.Float32)]
        if numeric_cols:
             return data.with_columns(pl.sum_horizontal(numeric_cols).alias("NEW_COLUMN"))
        return data

class PolarsRemoveDuplicatesTransformer(Transformer):
    def transform(self, data):
        return data.unique()

class PolarsHandleMissingValuesTransformer(Transformer):
    def transform(self, data):
        # fillna(method='ffill').fillna(method='bfill')
        return data.fill_null(strategy="forward").fill_null(strategy="backward")

class PolarsGenerateNewAttributesTransformer(Transformer):
    def transform(self, data):
        if 'price' in data.columns:
            return data.with_columns((pl.col("price") * 2).alias("new_attribute"))
        return data

class PolarsCalculateAgeTransformer(Transformer):
    def transform(self, data):
        if 'age' in data.columns:
            return data.with_columns((2024 - pl.col("age").cast(pl.Int32)).alias("year_of_birth"))
        return data

class PolarsApplyDiscountTransformer(Transformer):
    def transform(self, data):
        if 'price' in data.columns:
            return data.with_columns((pl.col("price") * 0.9).alias("discounted_price"))
        return data

class PolarsCapitalizeFirstLetterTransformer(Transformer):
    def transform(self, data):
        if 'name' in data.columns:
            # Polars string operations
            return data.with_columns(pl.col("name").str.to_titlecase().alias("name"))
        return data

class PolarsMovingAverageTransformer(Transformer):
    def transform(self, data):
        # data['moving_average'] = data['price'].rolling(window=10).mean()
        if 'price' in data.columns:
             return data.with_columns(pl.col("price").rolling_mean(window_size=10).alias("moving_average"))
        return data

class PolarsImputeMeanTransformer(Transformer):
    def transform(self, data):
        if 'salary' in data.columns:
            return data.with_columns(pl.col("salary").fill_null(pl.col("salary").mean()))
        return data

class PolarsDaysSinceTransformer(Transformer):
    def transform(self, data):
        if 'last_login' in data.columns:
            import datetime
            now = datetime.datetime.now()
            
            # Helper to cast if string
            col_expr = pl.col('last_login')
            # Check if likely string (Utf8 or String)
            if data.schema['last_login'] in (pl.Utf8, pl.String):
                 # Try common formats or let polars guess
                 col_expr = col_expr.str.to_datetime(strict=False)
            
            return data.with_columns(
                 (pl.lit(now) - col_expr).dt.total_days().alias("days_since_login")
            )
        return data

from src.etl.abstract_etl_methods import Transformer
import dask.dataframe as dd

class DaskSimpleTransformer(Transformer):
    def transform(self, data):
        data.columns = [col.lower() for col in data.columns]
        return data

class DaskComplexTransformer(Transformer):
    def transform(self, data):
        # Dask apply behaves similar to pandas
        # data['NEW_COLUMN'] = data.apply(lambda row: row.sum(), axis=1, meta=('x', 'float64'))
        # Using numeric sum for dask
        # Need to know columns. Dask is lazy.
        num_cols = data.select_dtypes(include=['number']).columns
        if len(num_cols) > 0:
            data['NEW_COLUMN'] = data[num_cols].sum(axis=1)
        return data

class DaskRemoveDuplicatesTransformer(Transformer):
    def transform(self, data):
        return data.drop_duplicates()

class DaskHandleMissingValuesTransformer(Transformer):
    def transform(self, data):
        return data.fillna(method='ffill').fillna(method='bfill')

class DaskGenerateNewAttributesTransformer(Transformer):
    def transform(self, data):
        if 'price' in data.columns:
            data['new_attribute'] = data['price'] * 2
        return data

class DaskCalculateAgeTransformer(Transformer):
    def transform(self, data):
        if 'age' in data.columns:
            data['year_of_birth'] = 2024 - data['age'].astype(int)
        return data

class DaskApplyDiscountTransformer(Transformer):
    def transform(self, data):
        if 'price' in data.columns:
            data['discounted_price'] = data['price'] * 0.9
        return data

class DaskCapitalizeFirstLetterTransformer(Transformer):
    def transform(self, data):
        if 'name' in data.columns:
             data['name'] = data['name'].str.capitalize()
        return data

class DaskMovingAverageTransformer(Transformer):
    def transform(self, data):
        # Rolling in dask is tricky, works best with time index or known partitions.
        # Assuming simple rolling if supported (usually needs map_overlap or similar if cross partition)
        # But Dask has rolling() on Series now.
        if 'price' in data.columns:
             data['moving_average'] = data['price'].rolling(window=10).mean()
        return data

class DaskImputeMeanTransformer(Transformer):
    def transform(self, data):
        if 'salary' in data.columns:
            mean_val = data['salary'].mean()
            data['salary'] = data['salary'].fillna(mean_val)
        return data

class DaskDaysSinceTransformer(Transformer):
    def transform(self, data):
        if 'last_login' in data.columns:
            import pandas as pd
            # Dask to_datetime
            data['last_login'] = dd.to_datetime(data['last_login'], errors='coerce')
            data['days_since_login'] = (pd.Timestamp.now() - data['last_login']).dt.days
        return data

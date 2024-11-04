from scr.etl.abstract_etl_methods import Transformer

import pandas as pd
import vaex


class PandasSimpleTransformer(Transformer):
    def transform(self, data):
        data.columns = [col.lower() for col in data.columns]
        return data

class PandasComplexTransformer(Transformer):
    def transform(self, data):
        data['NEW_COLUMN'] = data.apply(lambda row: row.sum(), axis=1)
        return data

class PandasRemoveDuplicatesTransformer(Transformer):
    def transform(self, data):
        return data.drop_duplicates()

class PandasHandleMissingValuesTransformer(Transformer):
    def transform(self, data):
        return data.fillna(method='ffill').fillna(method='bfill')

class PandasGenerateNewAttributesTransformer(Transformer):
    def transform(self, data):
        data['new_attribute'] = data['price'] * 2  # Ejemplo simple
        return data


class PandasCalculateAgeTransformer(Transformer):
    def transform(self, data):
        data['year_of_birth'] = ''
        data['year_of_birth'] = 2024 - data['age'].astype(int)

        return data

class PandasApplyDiscountTransformer(Transformer):
    def transform(self, data):
        data['discounted_price'] = data['price'] * 0.9
        return data

class PandasCapitalizeFirstLetterTransformer(Transformer):
    def transform(self, data):
        data['name'] = data['name'].str.capitalize()
        return data


# Definimos la operación demandante
class PandasMovingAverageTransformer(Transformer):
    def transform(self, data):

        data['moving_average'] = ''
        data['moving_average'] = data['price'].rolling(window=10).mean()
        return data

from scr.etl.abstract_etl_methods import Transformer
import vaex
import numpy as np


class VaexSimpleTransformer(Transformer):
    def transform(self, data):
        for column in data.get_column_names():
            data.rename(column, column.lower())
        return data


class VaexComplexTransformer(Transformer):
    def transform(self, data):
        data['NEW_COLUMN'] = data.sum(axis=1)
        return data


class VaexRemoveDuplicatesTransformer(Transformer):
    def transform(self, data):
        return data.drop_duplicates()


class VaexHandleMissingValuesTransformer(Transformer):
    def transform(self, data):
        return data.fillna(method='ffill').fillna(method='bfill')


class VaexGenerateNewAttributesTransformer(Transformer):
    def transform(self, data):
        data['new_attribute'] = data['Contrato'] * 2  # Ejemplo simple
        return data


class VaexCalculateAgeTransformer(Transformer):
    def transform(self, data):

        # Ensure that 'data' is a Vaex DataFrame
        if isinstance(data, vaex.dataframe.DataFrame):
            # Calculate 'antigüedad'
            try:
                data['year_of_birth'] = 2024 - data['age']
            except:
                data['year_of_birth'] = 2024 - data['edad']


        else:
            raise TypeError("Input data must be a Vaex DataFrame")

        return data
class VaexApplyDiscountTransformer(Transformer):
    def transform(self, data):
        data['discounted_price'] = data['price'] * 0.9
        return data


class VaexCapitalizeFirstLetterTransformer(Transformer):
    def transform(self, data):
        data['name'] = data['name'].str.capitalize()
        return data


class VaexMovingAverageTransformer(Transformer):
    def transform(self, data):
        window_size = 10
        prices = data['price'].to_numpy()
        moving_average = np.convolve(prices, np.ones(window_size)/window_size, mode='valid')

        # Pad the beginning of the result to match the length of the original data
        moving_average = np.concatenate([np.full(window_size-1, np.nan), moving_average])

        data['moving_average'] = moving_average
        print(type(data))
        print(data.columns)
        return data

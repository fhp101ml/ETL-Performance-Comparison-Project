from etl_framework.library_pandas.transform_function import *
from etl_framework.utils.translation_service.Translate_attributes import TranslateAttributesTransformer


class PandasTransformationStrategyFactory:
    def get_strategy(self, transform_type, idiom):
        if transform_type == "calculate_age":
            return PandasCalculateAgeTransformer()
        elif transform_type == "apply_discount":
            return PandasApplyDiscountTransformer()
        elif transform_type == "capitalize_first_letter":
            return PandasCapitalizeFirstLetterTransformer()
        elif transform_type == "simple":
            return PandasSimpleTransformer()
        elif transform_type == "complex":
            return PandasComplexTransformer()
        elif transform_type == "remove_duplicates":
            return PandasRemoveDuplicatesTransformer()
        elif transform_type == "handle_missing_values":
            return PandasHandleMissingValuesTransformer()
        elif transform_type == "generate_new_attributes":
            return PandasGenerateNewAttributesTransformer()
        elif transform_type == "moving_average":
            return PandasMovingAverageTransformer()
        elif transform_type == "impute_mean":
             return PandasImputeMeanTransformer()
        elif transform_type == "days_since":
             return PandasDaysSinceTransformer()
        elif transform_type == "translate_attributes":
            return TranslateAttributesTransformer(idiom)
        else:
            raise ValueError(f"Unknown transform type '{transform_type}'")
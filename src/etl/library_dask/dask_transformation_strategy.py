from src.etl.library_dask.transform_functions import *

class DaskTransformationStrategyFactory:
    def get_strategy(self, transform_type, idiom):
        if transform_type == "calculate_age":
            return DaskCalculateAgeTransformer()
        elif transform_type == "apply_discount":
            return DaskApplyDiscountTransformer()
        elif transform_type == "capitalize_first_letter":
            return DaskCapitalizeFirstLetterTransformer()
        elif transform_type == "simple":
            return DaskSimpleTransformer()
        elif transform_type == "complex":
            return DaskComplexTransformer()
        elif transform_type == "remove_duplicates":
            return DaskRemoveDuplicatesTransformer()
        elif transform_type == "handle_missing_values":
            return DaskHandleMissingValuesTransformer()
        elif transform_type == "generate_new_attributes":
            return DaskGenerateNewAttributesTransformer()
        elif transform_type == "moving_average":
            return DaskMovingAverageTransformer()
        elif transform_type == "impute_mean":
            return DaskImputeMeanTransformer()
        elif transform_type == "days_since":
             return DaskDaysSinceTransformer()
        else:
            raise ValueError(f"Unknown transform type '{transform_type}' for Dask")

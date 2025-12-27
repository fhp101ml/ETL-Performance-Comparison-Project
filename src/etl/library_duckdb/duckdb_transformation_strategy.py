from src.etl.library_duckdb.transform_functions import *

class DuckDBTransformationStrategyFactory:
    def get_strategy(self, transform_type, idiom):
        if transform_type == "calculate_age":
            return DuckDBCalculateAgeTransformer()
        elif transform_type == "apply_discount":
            return DuckDBApplyDiscountTransformer()
        elif transform_type == "capitalize_first_letter":
            return DuckDBCapitalizeFirstLetterTransformer()
        elif transform_type == "simple":
            return DuckDBSimpleTransformer()
        elif transform_type == "complex":
            return DuckDBComplexTransformer()
        elif transform_type == "remove_duplicates":
            return DuckDBRemoveDuplicatesTransformer()
        elif transform_type == "handle_missing_values":
            return DuckDBHandleMissingValuesTransformer()
        elif transform_type == "generate_new_attributes":
            return DuckDBGenerateNewAttributesTransformer()
        elif transform_type == "moving_average":
            return DuckDBMovingAverageTransformer()
        elif transform_type == "impute_mean":
            return DuckDBImputeMeanTransformer()
        elif transform_type == "days_since":
             return DuckDBDaysSinceTransformer()
        else:
            raise ValueError(f"Unknown transform type '{transform_type}' for DuckDB")

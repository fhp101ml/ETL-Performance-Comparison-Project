from src.etl.library_polars.transform_functions import *
# Assuming TranslateAttributesTransformer can handle different dataframe types or we might need a specific adapter.
# For now, let's omit translation or implement a polars specific one if needed.
# The original pandas mapping included TranslateAttributesTransformer. 
# We'll skip it for now or assume we just implement basic transformations first.

class PolarsTransformationStrategyFactory:
    def get_strategy(self, transform_type, idiom):
        if transform_type == "calculate_age":
            return PolarsCalculateAgeTransformer()
        elif transform_type == "apply_discount":
            return PolarsApplyDiscountTransformer()
        elif transform_type == "capitalize_first_letter":
            return PolarsCapitalizeFirstLetterTransformer()
        elif transform_type == "simple":
            return PolarsSimpleTransformer()
        elif transform_type == "complex":
            return PolarsComplexTransformer()
        elif transform_type == "remove_duplicates":
            return PolarsRemoveDuplicatesTransformer()
        elif transform_type == "handle_missing_values":
            return PolarsHandleMissingValuesTransformer()
        elif transform_type == "generate_new_attributes":
            return PolarsGenerateNewAttributesTransformer()
        elif transform_type == "moving_average":
            return PolarsMovingAverageTransformer()
        elif transform_type == "impute_mean":
            return PolarsImputeMeanTransformer()
        elif transform_type == "days_since":
             return PolarsDaysSinceTransformer()
        else:
            # Fallback or error
            # For simplicity in this "all libraries" sweep, we might just ignore unknown types or return a no-op 
            # but raising error is safer to detect missing features.
            raise ValueError(f"Unknown transform type '{transform_type}' for Polars")

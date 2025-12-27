from src.etl.library_vaex.transform_functions import *
from src.etl.utils.translation_service.Translate_attributes import TranslateAttributesTransformer


class VaexTransformationStrategyFactory:
    def get_strategy(self, transform_type,idiom):
        if transform_type == "calculate_age":
            return VaexCalculateAgeTransformer()
        elif transform_type == "apply_discount":
            return VaexApplyDiscountTransformer()
        elif transform_type == "capitalize_first_letter":
            return VaexCapitalizeFirstLetterTransformer()
        elif transform_type == "calculate_age":
            return VaexCalculateAgeTransformer()
        elif transform_type == "apply_discount":
            return VaexApplyDiscountTransformer()
        elif transform_type == "capitalize_first_letter":
            return VaexCapitalizeFirstLetterTransformer()
        elif transform_type == "simple":
            return VaexSimpleTransformer()
        elif transform_type == "moving_average":
            return VaexMovingAverageTransformer()
        elif transform_type == "translate_attributes":
            return TranslateAttributesTransformer(idiom)
        else:
            raise ValueError(f"Unknown transform type '{transform_type}'")
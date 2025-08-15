import pandas as pd
from typing import List

# We assume mra_data.py and slice_transformations.py are accessible
from mra_data import RelationSchema
from slice_transformations.slice_transformation import SliceTransformationBase

class RatioTransformation(SliceTransformationBase):
    """
    A parameterized transformation to compute the ratio of two columns.
    """
    def __init__(self, numerator_col: str, denominator_col: str, output_col: str):
        """
        Initializes the transformation with its specific parameters.

        Args:
            numerator_col: The name of the numerator column.
            denominator_col: The name of the denominator column.
            output_col: The name of the new column for the resulting ratio.
        """
        self.numerator_col = numerator_col
        self.denominator_col = denominator_col
        self.output_col = output_col

    @property
    def feature_schema(self) -> RelationSchema:
        """The required schema contains the numerator and denominator columns."""
        return RelationSchema([self.numerator_col, self.denominator_col])

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the ratio and adds it as a new column.
        Handles division by zero by replacing resulting NaNs with 0.
        """
        # A real implementation could validate data.columns against self.feature_schema here
        new_data = data.copy()
        new_data[self.output_col] = (new_data[self.numerator_col] / new_data[self.denominator_col]).fillna(0)
        return new_data

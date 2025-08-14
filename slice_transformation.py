import pandas as pd
from abc import ABC, abstractmethod
from typing import List

# We assume mra_data.py is in the same directory or accessible in the path
from mra_data import RelationSchema

class SliceTransformationBase(ABC):
    """
    Abstract Base Class for all slice transformations (the tau function in the paper).

    A slice transformation is a function that maps a feature table (DataFrame)
    to another feature table (DataFrame). Subclasses must define the specific
    parameters they require in their own initializer and must implement the
    `feature_schema` property and the `__call__` method.
    """
    @property
    @abstractmethod
    def feature_schema(self) -> RelationSchema:
        """
        The schema of the DataFrame this transformation is designed to operate on.
        This must be implemented by all subclasses.
        """
        pass

    @abstractmethod
    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the transformation's core logic on the given DataFrame.

        Args:
            data: The input feature table (DataFrame).

        Returns:
            The transformed feature table (DataFrame).
        """
        pass

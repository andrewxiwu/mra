import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Optional

# We assume mra_data.py is in the same directory or accessible in the path
from mra_data import RelationSchema

class SliceTransformation(ABC):
    """
    Abstract Base Class for all slice transformations (the tau function in the paper).
    """

    def __init__(self):
        # Reference data.
        self._reference_data: Optional[pd.DataFrame] = None

    @property
    @abstractmethod
    def feature_schema(self) -> RelationSchema:
        """
        The schema of the DataFrame this transformation is designed to operate on.
        This must be implemented by all subclasses.
        """
        pass

    @property
    def require_reference_data(self) -> bool:
        """
        Specifies whether this transformation requires reference data.
        Subclasses should override this property to return True if they
        depend on reference data.
        """
        return False

    @property
    def reference_data(self) -> Optional[pd.DataFrame]:
        """The reference data available to the transformation."""
        return self._reference_data

    @reference_data.setter
    def reference_data(self, data: Optional[pd.DataFrame]):
        """
        Sets the reference data for the transformation.

        Raises:
            ValueError: If reference data is required but `None` is provided.
        """
        if self.require_reference_data and data is None:
            raise ValueError(f"The transformation '{self.__class__.__name__}' requires reference data, but none was provided.")
        self._reference_data = data

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

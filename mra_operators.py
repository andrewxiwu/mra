"""MRA operators."""

import pandas as pd
from abc import ABC, abstractmethod
from typing import Union, List, Callable, Dict, Any
from itertools import chain, combinations

# Assuming you have a module named 'mra_data' with these classes defined.
# This file should be in the same directory.
from mra_data import RelationSpace, SliceRelation, RelationSchema, create_relation_tuple, RelationTuple

# ==============================================================================
# Type Alias for Data
# ==============================================================================
# Define a type alias for the data types our operators will handle.
MraData = Union[pd.DataFrame, RelationSpace, SliceRelation]

# ==============================================================================
# Operator Base Class and Pipeline
# ==============================================================================

class MraOperatorBase(ABC):
    """
    Abstract Base Class for all Multi-Relational Algebra (MRA) operators.

    This class provides the core functionality for chaining operators together
    using the pipe `|` symbol. Any class that inherits from this must
    implement the `_execute` method, which contains the operator's specific
    data transformation logic.
    """

    @abstractmethod
    def _execute(self, data: MraData) -> MraData:
        """
        The core logic of the operator.
        This method must be implemented by all concrete operator subclasses.
        """
        pass

    def __call__(self, data: MraData) -> MraData:
        """Executes the operator on the given data."""
        print(f"Executing {self.__class__.__name__}...")
        return self._execute(data)

    def __or__(self, other: 'MraOperatorBase') -> 'Pipeline':
        """Overloads the `|` operator to chain MRA operators."""
        if isinstance(other, Pipeline):
            return Pipeline([self] + other.operators)
        return Pipeline([self, other])

    def __ror__(self, other: MraData) -> MraData:
        """Overloads the right-sided `|` operator to start a pipeline."""
        return self(other)


class Pipeline(MraOperatorBase):
    """
    A special MRA operator that represents a sequence of other operators.
    It executes the operators in the order they were added.
    """
    def __init__(self, operators: list[MraOperatorBase]):
        self.operators = operators

    def _execute(self, data: MraData) -> MraData:
        """Executes the entire pipeline sequentially."""
        result = data
        for op in self.operators:
            result = op(result)
        return result

    def __or__(self, other: 'MraOperatorBase') -> 'Pipeline':
        """Appends another operator to the existing pipeline."""
        if isinstance(other, Pipeline):
            return Pipeline(self.operators + other.operators)
        return Pipeline(self.operators + [other])


# ==============================================================================
# Concrete Operator Implementations
# ==============================================================================

class CreateRelationSpaceByCube(MraOperatorBase):
    """
    Creates a RelationSpace by performing a "GROUP BY CUBE" operation.
    It generates all possible grouping sets from the provided keys and
    populates a RelationSpace with the aggregated results.
    """
    def __init__(self, grouping_keys: List[str], aggregations: Dict[str, Any]):
        self.grouping_keys = grouping_keys
        self.aggregations = aggregations

    def _execute(self, data: pd.DataFrame) -> RelationSpace:
        if not isinstance(data, pd.DataFrame):
            raise TypeError("CreateRelationSpaceByCube expects a pandas DataFrame.")

        # The dimensions of the space are the keys used for the cube.
        relation_space = RelationSpace(dimensions=RelationSchema(self.grouping_keys))

        # Generate the power set of grouping keys to simulate a CUBE.
        # This includes the empty set for the grand total.
        power_set = chain.from_iterable(combinations(self.grouping_keys, r) for r in range(len(self.grouping_keys) + 1))

        print(f"  > Generating cube for keys: {self.grouping_keys}")
        for group in power_set:
            group_list = list(group)
            
            if not group_list:
                # Handle the grand total (empty grouping set)
                agg_df = data.agg(self.aggregations).to_frame().T
            else:
                # Handle regular grouping sets
                agg_df = data.groupby(group_list).agg(self.aggregations).reset_index()

            # The dimensional schema for each relation is its grouping key.
            dimensional_schema = RelationSchema(group_list)
            relation_space.add_relation(agg_df, dimensional_schema)
            
        return relation_space


class Represent(MraOperatorBase):
    """
    Transforms a RelationSpace into a SliceRelation by partitioning relations
    based on a specified region key.
    """
    def __init__(self, region_key: str):
        self.region_key = region_key

    def _execute(self, data: RelationSpace) -> SliceRelation:
        if not isinstance(data, RelationSpace):
            raise TypeError("Represent expects a RelationSpace object.")

        print(f"  > Representing RelationSpace into slices by '{self.region_key}'")
        slice_relation = SliceRelation()
        
        for _, df in data._relations.items():
            if self.region_key in df.columns:
                feature_cols = [col for col in df.columns if col != self.region_key]
                feature_schema = RelationSchema(feature_cols)

                grouped = df.groupby(self.region_key)
                for region_value, feature_df_group in grouped:
                    region_tuple = create_relation_tuple({self.region_key: region_value})
                    feature_data = feature_df_group[feature_cols].copy().reset_index(drop=True)
                    
                    slice_relation.add_slice_tuple(region_tuple, feature_schema, feature_data)
                    
        return slice_relation


class SliceSelect(MraOperatorBase):
    """
    Filters a SliceRelation based on a predicate function that evaluates
    each slice tuple (region and its features).
    """
    def __init__(self, predicate_func: Callable[[RelationTuple, dict], bool]):
        self.predicate_func = predicate_func

    def _execute(self, data: SliceRelation) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceSelect expects a SliceRelation object.")
        
        print(f"  > Selecting slices...")
        new_slice_relation = SliceRelation()
        for region, features in data.data.items():
            if self.predicate_func(region, features):
                for feature_schema, feature_df in features.items():
                    new_slice_relation.add_slice_tuple(region, feature_schema, feature_df)
                    
        return new_slice_relation


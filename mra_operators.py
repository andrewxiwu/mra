import pandas as pd
from abc import ABC, abstractmethod
from typing import Union, List, Callable, Dict, Any, Tuple
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

        relation_space = RelationSpace(dimensions=RelationSchema(self.grouping_keys))
        power_set = chain.from_iterable(combinations(self.grouping_keys, r) for r in range(len(self.grouping_keys) + 1))

        print(f"  > Generating cube for keys: {self.grouping_keys}")
        for group in power_set:
            group_list = list(group)
            agg_df = data.agg(self.aggregations).to_frame().T if not group_list else data.groupby(group_list).agg(self.aggregations).reset_index()
            dimensional_schema = RelationSchema(group_list)
            relation_space.add_relation(agg_df, dimensional_schema)
            
        return relation_space


class Represent(MraOperatorBase):
    """
    Transforms a RelationSpace into a SliceRelation by partitioning relations
    based on specified region and feature schemas.
    """
    def __init__(self, region_schemas: List[RelationSchema], feature_schemas: List[RelationSchema]):
        self.region_schemas = region_schemas
        self.feature_schemas = feature_schemas

    def _execute(self, data: RelationSpace) -> SliceRelation:
        if not isinstance(data, RelationSpace):
            raise TypeError("Represent expects a RelationSpace object.")

        print(f"  > Representing RelationSpace into slices...")
        slice_relation = SliceRelation()

        # Iterate through every combination of region and feature schemas
        for r_schema in self.region_schemas:
            for f_schema in self.feature_schemas:
                # Determine the target dimensional schema for lookup in the RelationSpace
                # As per the paper: T = r_schema U f_schema, K = T intersect D
                target_attributes = r_schema.attributes + f_schema.attributes
                target_dim_attributes = [attr for attr in target_attributes if attr in data.dimensions.attributes]
                target_dim_schema = RelationSchema(target_dim_attributes)

                relation_to_partition = data.get_relation(target_dim_schema)

                if relation_to_partition is None:
                    continue
                
                region_cols = list(r_schema.attributes)
                feature_cols = list(f_schema.attributes)
                
                # Ensure all required columns exist in the dataframe
                if not all(col in relation_to_partition.columns for col in region_cols + feature_cols):
                    continue

                # Group by the region columns to create slices
                grouped = relation_to_partition.groupby(region_cols)
                for region_values, group_df in grouped:
                    # Ensure region_values is always a tuple, even for a single column
                    if not isinstance(region_values, tuple):
                        region_values = (region_values,)
                    
                    region_dict = dict(zip(region_cols, region_values))
                    region_tuple = create_relation_tuple(region_dict)
                    
                    feature_data = group_df[feature_cols].copy().reset_index(drop=True)
                    slice_relation.add_slice_tuple(region_tuple, f_schema, feature_data)
                    
        return slice_relation

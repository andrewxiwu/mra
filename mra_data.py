"""MRA data model."""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from itertools import chain, combinations
from typing import List, Dict, Any, Callable, Tuple, NewType


@dataclass(frozen=True)
class RelationSchema:
    """
    Represents the schema of a relation.
    
    Attributes are stored internally as a sorted tuple to ensure that schemas
    are immutable and that their order does not affect equality.
    (e.g., RelationSchema(['a', 'b']) == RelationSchema(['b', 'a']))
    """
    attributes: Tuple[str, ...]

    def __init__(self, attributes: List[str]):
        """
        Initializes the schema from a list of strings for convenience.
        The attributes are sorted and converted to a tuple internally.
        """
        # We use object.__setattr__ because the dataclass is frozen.
        object.__setattr__(self, 'attributes', tuple(sorted(attributes)))


class RelationSpace:
    """
    Represents a RelationSpace, a collection of relations (DataFrames)
    indexed by their dimensional schema.
    """
    def __init__(self, dimensions: RelationSchema):
        self.dimensions = dimensions
        self._relations: Dict[RelationSchema, pd.DataFrame] = {}

    def add_relation(self, relation: pd.DataFrame, dimensional_schema: RelationSchema):
        """Adds a relation to the space, indexed by its dimensional schema."""
        self._relations[dimensional_schema] = relation

    def get_relation(self, dimensional_schema: RelationSchema) -> pd.DataFrame:
        """Retrieves a relation by its dimensional schema."""
        return self._relations.get(dimensional_schema)

    def __repr__(self):
        """Provides a string representation of the RelationSpace."""
        rep = f"RelationSpace(Dimensions: {self.dimensions.attributes})\n"
        rep += "="*40 + "\n"
        if not self._relations:
            return rep + "Empty RelationSpace"
        for dims_schema, df in self._relations.items():
            rep += f"--> Relation with Dimensions: {dims_schema.attributes}\n"
            rep += df.to_string(index=False) + "\n\n"
        return rep


# Define a distinct type for a relation's tuple structure.
RelationTuple = NewType('RelationTuple', Tuple[Tuple[str, Any], ...])


def create_relation_tuple(key_values: Dict[str, Any]) -> RelationTuple:
    """
    Creates a canonical relation tuple from a dictionary.
    
    The key-value pairs are sorted by key to ensure that tuples created
    from dictionaries with different key orders are treated as equal.
    
    Args:
        key_values: A dictionary of attribute-value pairs.
        
    Returns:
        A new RelationTuple instance.
    """
    sorted_items = tuple(sorted(key_values.items()))
    return RelationTuple(sorted_items)


class SliceRelation:
    """
    Represents a SliceRelation, which structures data around entities (regions)
    with corresponding relation-valued features.
    
    The core data structure is a two-level dictionary:
    - The outer dictionary maps a RelationTuple to its feature tables.
    - The inner dictionary maps a RelationSchema to a specific feature DataFrame.
    """
    def __init__(self, dimensions: RelationSchema):
        """
        Initializes the SliceRelation.

        Args:
            dimensions: The dimension set for the SliceRelation, which is crucial
                        for operations like Flatten.
        """
        self.dimensions = dimensions
        self.data: Dict[RelationTuple, Dict[RelationSchema, pd.DataFrame]] = {}

    def add_slice_tuple(self, region: RelationTuple, feature_schema: RelationSchema, feature_data: pd.DataFrame):
        """Adds or updates a feature table for a specific region."""
        if region not in self.data:
            self.data[region] = {}
        self.data[region][feature_schema] = feature_data

    def __repr__(self):
        """Provides a string representation of the SliceRelation."""
        rep = f"SliceRelation(Dimensions: {self.dimensions.attributes})\n"
        rep += "="*40 + "\n"
        if not self.data:
            return rep + "Empty SliceRelation"
        for i, (relation_tuple, features) in enumerate(self.data.items()):
            rep += f"--- Slice Tuple {i+1} ---\n"
            # Create a readable string from the relation tuple
            region_str = ', '.join([f"{k}={v}" for k, v in relation_tuple])
            rep += f"Region: ({region_str})\n"
            for f_schema, df in features.items():
                rep += f"  Feature Schema: {f_schema.attributes}\n"
                rep += "  " + df.to_string(index=False).replace('\n', '\n  ') + "\n"
            rep += "\n"
        return rep

import pandas as pd
import numpy as np
from itertools import chain, combinations
from typing import List, Dict, Any, Callable, Tuple

# -----------------------------------------------------------------------------
# MRA Data Structures
# -----------------------------------------------------------------------------

class RelationSpace:
    """
    Represents a RelationSpace, a collection of relations (DataFrames)
    indexed by their dimensional schema. This is analogous to Definition 1
    in the provided paper. The dimensions are used to identify and retrieve
    specific relations within the space.
    """
    def __init__(self, dimensions: List[str]):
        self.dimensions = tuple(sorted(dimensions))
        self._relations: Dict[Tuple[str, ...], pd.DataFrame] = {}

    def add_relation(self, relation: pd.DataFrame, dimensional_schema: List[str]):
        """Adds a relation to the space, indexed by its dimensional schema."""
        key = tuple(sorted(dimensional_schema))
        self._relations[key] = relation

    def get_relation(self, dimensional_schema: List[str]) -> pd.DataFrame:
        """Retrieves a relation by its dimensional schema."""
        key = tuple(sorted(dimensional_schema))
        return self._relations.get(key)

    def __repr__(self):
        """Provides a string representation of the RelationSpace."""
        rep = f"RelationSpace(Dimensions: {self.dimensions})\n"
        rep += "="*40 + "\n"
        if not self._relations:
            return rep + "Empty RelationSpace"
        for dims, df in self._relations.items():
            rep += f"--> Relation with Dimensions: {dims}\n"
            rep += df.to_string(index=False) + "\n\n"
        return rep

class SliceRelation:
    """
    Represents a SliceRelation, which structures data around entities (regions)
    with corresponding relation-valued features. This corresponds to
    Definition 10 in the paper. Each row in a SliceRelation is a "slice tuple"
    containing a region and a dictionary of feature DataFrames.
    """
    def __init__(self, dimensions: List[str]):
        self.dimensions = tuple(sorted(dimensions))
        self.slice_tuples: List[Dict[str, Any]] = []

    def add_slice_tuple(self, region: Dict, features: Dict[Tuple[str, ...], pd.DataFrame]):
        """Adds a slice tuple to the relation."""
        self.slice_tuples.append({'region': region, 'features': features})

    def __repr__(self):
        """Provides a string representation of the SliceRelation."""
        rep = f"SliceRelation(Dimensions: {self.dimensions})\n"
        rep += "="*40 + "\n"
        if not self.slice_tuples:
            return rep + "Empty SliceRelation"
        for i, s_tuple in enumerate(self.slice_tuples):
            rep += f"--- Slice Tuple {i+1} ---\n"
            rep += f"Region: {s_tuple['region']}\n"
            for f_schema, df in s_tuple['features'].items():
                rep += f"  Feature Schema: {f_schema}\n"
                rep += "  " + df.to_string(index=False).replace('\n', '\n  ') + "\n"
            rep += "\n"
        return rep

# -----------------------------------------------------------------------------
# MRA Operators
# -----------------------------------------------------------------------------

def create_relation_space(
    base_table: pd.DataFrame,
    grouping_sets: List[List[str]],
    aggregations: Dict[str, Tuple[str, str]]
) -> RelationSpace:
    """
    Creates a RelationSpace by performing aggregations over specified
    grouping sets, similar to SQL's GROUP BY GROUPING SETS.

    Args:
        base_table: The initial DataFrame to aggregate.
        grouping_sets: A list of lists, where each inner list is a set of
                       columns to group by.
        aggregations: A dictionary defining the aggregations to perform.
                      e.g., {'TotalCost': ('Cost', 'sum')}

    Returns:
        A RelationSpace containing the aggregated relations.
    """
    dimensions = list(base_table.columns)
    rs = RelationSpace(dimensions)
    for grp_set in grouping_sets:
        if not grp_set: # Handle global aggregation
             agg_df = base_table.agg({v[0]: v[1] for k, v in aggregations.items()}).to_frame().T
             agg_df = agg_df.rename(columns={v[0]: k for k, v in aggregations.items()})
        else:
            agg_df = base_table.groupby(grp_set).agg(
                **{k: pd.NamedAgg(column=v[0], aggfunc=v[1]) for k, v in aggregations.items()}
            ).reset_index()
        rs.add_relation(agg_df, grp_set)
    return rs


def represent(
    relation_space: RelationSpace,
    region_schemas: List[List[str]],
    feature_schemas: List[List[str]]
) -> SliceRelation:
    """
    Transforms a RelationSpace into a SliceRelation. It partitions relations
    based on region schemas and aligns them into slice tuples. This is a core
    MRA operation for restructuring data for per-entity analysis.
    (See Section 3.2.2 in the paper).
    """
    slice_relation = SliceRelation(relation_space.dimensions)
    all_regions = {} # {region_tuple: {feature_schema: df}}

    for region_schema in region_schemas:
        for feature_schema in feature_schemas:
            required_cols = set(region_schema + feature_schema)
            
            # Find candidate relations that contain all the necessary columns
            candidate_relations = []
            for relation in relation_space._relations.values():
                if required_cols.issubset(relation.columns):
                    candidate_relations.append(relation)

            if not candidate_relations:
                continue
            
            # Heuristic: choose the candidate with the minimum number of columns
            # to get the most specific relation.
            source_relation = min(candidate_relations, key=lambda df: len(df.columns))

            # Partition the source relation by the region schema
            for region_values, group in source_relation.groupby(region_schema):
                # Ensure region_values is iterable for zip
                if not isinstance(region_values, tuple):
                    region_values_tuple = (region_values,)
                else:
                    region_values_tuple = region_values

                region_dict = dict(zip(region_schema, region_values_tuple))
                region_key = tuple(sorted(region_dict.items()))

                # Extract the feature table
                feature_table = group[feature_schema].copy()

                # Store the feature table for the region
                if region_key not in all_regions:
                    all_regions[region_key] = {}
                all_regions[region_key][tuple(sorted(feature_schema))] = feature_table

    # Assemble the final SliceRelation
    for region_key, features in all_regions.items():
        slice_relation.add_slice_tuple(dict(region_key), features)

    return slice_relation

def slice_transform(
    slice_relation: SliceRelation,
    transformations: List[Tuple[List[str], Callable, str]]
) -> SliceRelation:
    """
    Applies transformations to feature tables within each slice tuple.
    (See Section 4.3.3 in the paper).

    Args:
        slice_relation: The input SliceRelation.
        transformations: A list of tuples, where each is
                         (feature_schema, transform_function, new_feature_name).
    """
    new_slice_relation = SliceRelation(slice_relation.dimensions)
    for s_tuple in slice_relation.slice_tuples:
        new_features = s_tuple['features'].copy()
        for f_schema, func, new_name in transformations:
            f_schema_key = tuple(sorted(f_schema))
            if f_schema_key in new_features:
                # Apply the transformation function
                transformed_df = func(new_features[f_schema_key])
                # Add the new transformed feature table
                new_features[tuple(sorted(transformed_df.columns))] = transformed_df
        new_slice_relation.add_slice_tuple(s_tuple['region'], new_features)
    return new_slice_relation


def slice_select(
    slice_relation: SliceRelation,
    predicates: List[Callable[[Dict], bool]]
) -> SliceRelation:
    """
    Filters slice tuples based on a list of predicates.
    (See Section 4.3.2 in the paper).
    """
    filtered_slice_relation = SliceRelation(slice_relation.dimensions)
    for s_tuple in slice_relation.slice_tuples:
        # A slice tuple is kept if all predicates evaluate to True
        if all(p(s_tuple['features']) for p in predicates):
            filtered_slice_relation.add_slice_tuple(s_tuple['region'], s_tuple['features'])
    return filtered_slice_relation


def flatten(slice_relation: SliceRelation) -> RelationSpace:
    """
    Converts a SliceRelation back into a RelationSpace.
    (See Section 3.2.3 in the paper).
    """
    new_relation_space = RelationSpace(list(slice_relation.dimensions))
    all_relations = {} # {dimensional_schema: list_of_dfs}

    for s_tuple in slice_relation.slice_tuples:
        region_df = pd.DataFrame([s_tuple['region']])
        for f_schema, feature_df in s_tuple['features'].items():
            # Combine region and feature data
            # Create a cross product to join the single region row with all feature rows
            temp_df = region_df.assign(key=1).merge(feature_df.assign(key=1), on='key').drop('key', axis=1)

            dimensional_schema = tuple(sorted(s_tuple['region'].keys() | set(f_schema)))
            if dimensional_schema not in all_relations:
                all_relations[dimensional_schema] = []
            all_relations[dimensional_schema].append(temp_df)

    # Concatenate all DataFrames for each dimensional schema
    for dims, dfs in all_relations.items():
        new_relation_space.add_relation(pd.concat(dfs, ignore_index=True), list(dims))

    return new_relation_space


def crawl(
    relation_space: RelationSpace,
    region_schemas: List[List[str]],
    feature_schemas: List[List[str]],
    slice_transformations: List[Tuple[List[str], Callable, str]],
    predicates: List[Callable[[Dict], bool]]
) -> RelationSpace:
    """
    A "mega-operator" that encapsulates the common analytical pattern:
    Represent -> SliceTransform -> SliceSelect -> Flatten.
    (See Section 5 in the paper).
    """
    s1 = represent(relation_space, region_schemas, feature_schemas)
    s2 = slice_transform(s1, slice_transformations)
    s3 = slice_select(s2, predicates)
    final_rs = flatten(s3)
    return final_rs

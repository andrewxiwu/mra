import pandas as pd
from abc import ABC, abstractmethod
from typing import Union, List, Callable, Dict, Any, Set, Tuple
from itertools import chain, combinations

# Assuming you have a module named 'mra_data' with these classes defined.
# This file should be in the same directory.
from mra_data import RelationSpace, SliceRelation, RelationSchema, create_relation_tuple, RelationTuple
from slice_transformations.slice_transformation import SliceTransformationBase

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
        slice_relation = SliceRelation(dimensions=data.dimensions)

        for r_schema in self.region_schemas:
            for f_schema in self.feature_schemas:
                target_attributes = r_schema.attributes + f_schema.attributes
                target_dim_attributes = [attr for attr in target_attributes if attr in data.dimensions.attributes]
                target_dim_schema = RelationSchema(target_dim_attributes)
                relation_to_partition = data.get_relation(target_dim_schema)

                if relation_to_partition is None:
                    continue

                region_cols = list(r_schema.attributes)
                feature_cols = list(f_schema.attributes)

                if not all(col in relation_to_partition.columns for col in region_cols + feature_cols):
                    continue
                
                # Handle the special case for the empty region (reference data)
                if not region_cols:
                    region_tuple = create_relation_tuple({})
                    feature_data = relation_to_partition[feature_cols].copy().reset_index(drop=True)
                    slice_relation.add_slice_tuple(region_tuple, f_schema, feature_data)
                else:
                    grouped = relation_to_partition.groupby(region_cols)
                    for region_values, group_df in grouped:
                        if not isinstance(region_values, tuple):
                            region_values = (region_values,)
                        region_dict = dict(zip(region_cols, region_values))
                        region_tuple = create_relation_tuple(region_dict)
                        feature_data = group_df[feature_cols].copy().reset_index(drop=True)
                        slice_relation.add_slice_tuple(region_tuple, f_schema, feature_data)

        return slice_relation

class SliceTransform(MraOperatorBase):
    def __init__(self,
                 slice_transformations: List[SliceTransformationBase],
                 dimensions: RelationSchema,
                 drill_down_regions: Set[RelationTuple] = None,
                 parent_region_schemas: Set[RelationSchema] = None):
        self.slice_transformations = slice_transformations
        self.dimensions = dimensions
        self.drill_down_regions = drill_down_regions
        self.parent_region_schemas = parent_region_schemas

    def _is_descendant(self, region_to_check: RelationTuple,
                       parent_regions: Set[RelationTuple],
                       parent_schemas: Set[RelationSchema]) -> bool:
        """
        Checks if a region is a valid descendant based on its parents'
        schemas and values.
        """
        components = list(region_to_check)
        k = len(components)

        if k == 0:
            return True

        for i in range(k):
            projection_components = components[:i] + components[i+1:]
            
            proj_schema = RelationSchema(
                [key for key, val in projection_components])
            if proj_schema not in parent_schemas:
                continue

            parent_candidate = create_relation_tuple(
                dict(projection_components))
            if parent_candidate not in parent_regions:
                return False
        
        return True

    def _execute(self, data: SliceRelation) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceTransform expects a SliceRelation object.")

        print("  > Transforming slices...")
        new_slice_relation = SliceRelation(dimensions=self.dimensions)
        
        empty_region_tuple = create_relation_tuple({})
        reference_features = data.data.get(empty_region_tuple, {})
        
        for transformation in self.slice_transformations:
            if transformation.require_reference_data:
                ref_data = reference_features.get(
                    transformation.feature_schema)
                transformation.reference_data = ref_data

        for region, features in data.data.items():
            if region == empty_region_tuple:
                continue

            if (self.drill_down_regions is not None and
                    self.parent_region_schemas is not None):
                if not self._is_descendant(region, self.drill_down_regions,
                                           self.parent_region_schemas):
                    continue

            new_features = features.copy()
            for transformation in self.slice_transformations:
                if transformation.feature_schema in features:
                    feature_df = features[transformation.feature_schema]
                    transformed_df = transformation(feature_df.copy())
                    output_schema = RelationSchema(
                        list(transformed_df.columns))
                    new_features[output_schema] = transformed_df
            
            for f_schema, f_df in new_features.items():
                new_slice_relation.add_slice_tuple(region, f_schema, f_df)

        if reference_features:
            for f_schema, f_df in reference_features.items():
                new_slice_relation.add_slice_tuple(
                    empty_region_tuple, f_schema, f_df)

        return new_slice_relation


class SliceTransform(MraOperatorBase):
    """
    Transforms feature tables within a SliceRelation by applying a sequence
    of slice transformations.
    """
    def __init__(
            self,
            slice_transformations: List[SliceTransformationBase],
            dimensions: RelationSchema,
            drill_down_regions: Set[RelationTuple] = None,
            parent_region_schemas: Set[RelationSchema] = None,
    ):
        self.slice_transformations = slice_transformations
        self.dimensions = dimensions
        self.drill_down_regions = drill_down_regions
        self.parent_region_schemas = parent_region_schemas

    def _is_descendant(
            self,
            region_to_check: RelationTuple,
            parent_regions: Set[RelationTuple],
            parent_schemas: Set[RelationSchema]
    ) -> bool:
        """
        Checks if a region is a valid descendant based on its parents'
        schemas and values.
        """
        components = list(region_to_check)
        k = len(components)

        if k == 0:
            return True

        for i in range(k):
            projection_components = components[:i] + components[i+1:]
            
            proj_schema = RelationSchema(
                [key for key, val in projection_components])
            if proj_schema not in parent_schemas:
                continue

            parent_candidate = create_relation_tuple(
                dict(projection_components))
            if parent_candidate not in parent_regions:
                return False
        
        return True

    def _execute(
            self,
            data: SliceRelation
    ) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceTransform expects a SliceRelation object.")

        print("  > Transforming slices...")
        new_slice_relation = SliceRelation(dimensions=self.dimensions)

        transform_map = {t.feature_schema: t for t in
                         self.slice_transformations}

        empty_region_tuple = create_relation_tuple({})
        reference_features = data.data.get(empty_region_tuple, {})
        
        for transformation in self.slice_transformations:
            if transformation.require_reference_data:
                ref_data = reference_features.get(
                    transformation.feature_schema)
                transformation.reference_data = ref_data

        for region, features in data.data.items():
            if region == empty_region_tuple:
                continue

            if (self.drill_down_regions is not None and
                    self.parent_region_schemas is not None):
                if not self._is_descendant(region, self.drill_down_regions,
                                           self.parent_region_schemas):
                    continue

            processed_schemas = set()

            for feature_schema, feature_df in features.items():
                if feature_schema in transform_map:
                    transformation = transform_map[feature_schema]
                    transformed_df = transformation(feature_df.copy())
                    output_schema = RelationSchema(
                        list(transformed_df.columns))
                    new_slice_relation.add_slice_tuple(
                        region, output_schema, transformed_df)
                    processed_schemas.add(feature_schema)

            for feature_schema, feature_df in features.items():
                if feature_schema not in processed_schemas:
                    new_slice_relation.add_slice_tuple(
                        region, feature_schema, feature_df.copy())

        return new_slice_relation


class SliceSelect(MraOperatorBase):
    """
    Filters a SliceRelation based on a predicate function that evaluates
    each slice tuple (region and its features).
    """
    def __init__(self, predicate_func: Callable[[RelationTuple, Dict[RelationSchema, pd.DataFrame]], bool]):
        self.predicate_func = predicate_func

    def _execute(self, data: SliceRelation) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceSelect expects a SliceRelation object.")

        print(f"  > Selecting slices...")
        # The new SliceRelation inherits dimensions from the input.
        new_slice_relation = SliceRelation(dimensions=data.dimensions)

        for region, features in data.data.items():
            # The predicate function receives the entire slice tuple to make a decision
            if self.predicate_func(region, features):
                # If the predicate is true, add the entire slice tuple to the new relation
                for feature_schema, feature_df in features.items():
                    new_slice_relation.add_slice_tuple(region, feature_schema, feature_df)

        return new_slice_relation


class SliceProject(MraOperatorBase):
    """
    Filters a SliceRelation to keep only the specified region and feature schemas.
    """
    def __init__(self, region_schemas: List[RelationSchema], feature_schemas: List[RelationSchema] = None):
        self.region_schemas = region_schemas
        self.feature_schemas = feature_schemas

    def _execute(self, data: SliceRelation) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceProject expects a SliceRelation object.")
        
        print(f"  > Projecting slices to keep only specified regions...")
        new_slice_relation = SliceRelation(dimensions=data.dimensions)
        
        # Create a set for faster lookup
        allowed_region_schemas = {RelationSchema(list(rs.attributes)) for rs in self.region_schemas}

        for region, features in data.data.items():
            # Determine the schema of the current region
            current_region_schema = RelationSchema([k for k, v in region])
            
            # Keep the slice tuple only if its region schema is in the allowed list
            if current_region_schema in allowed_region_schemas:
                # If feature_schemas is None, keep all features. Otherwise, filter them.
                features_to_keep = features
                if self.feature_schemas is not None:
                    allowed_feature_schemas = set(self.feature_schemas)
                    features_to_keep = {fs: df for fs, df in features.items() if fs in allowed_feature_schemas}

                for f_schema, f_df in features_to_keep.items():
                    new_slice_relation.add_slice_tuple(region, f_schema, f_df)

        return new_slice_relation


class Flatten(MraOperatorBase):
    """
    Converts a SliceRelation back into a RelationSpace.
    """
    def __init__(self, dimensions: RelationSchema):
        self.dimensions = dimensions

    def _execute(self, data: SliceRelation) -> RelationSpace:
        if not isinstance(data, SliceRelation):
            raise TypeError("Flatten expects a SliceRelation object.")

        print("  > Flattening SliceRelation to RelationSpace...")
        new_relation_space = RelationSpace(dimensions=self.dimensions)

        # Group all feature tables by their region to handle merges correctly.
        relations_to_merge: Dict[RelationTuple, List[pd.DataFrame]] = {}

        for region, features in data.data.items():
            if region not in relations_to_merge:
                relations_to_merge[region] = []

            # Add all feature tables for this region to a list
            relations_to_merge[region].extend(features.values())

        # Process each region's collected feature tables
        for region, dfs_to_merge in relations_to_merge.items():
            region_df = pd.DataFrame([dict(region)], index=[0])
            region_schema = RelationSchema(list(region_df.columns))

            # Start with the region dataframe
            final_df = region_df

            # Merge all feature tables for this region together
            if dfs_to_merge:
                # Use a cross join for each feature table since they don't share columns
                # besides the implicit region key which is already handled.
                for feature_df in dfs_to_merge:
                    if not feature_df.empty:
                        final_df = final_df.merge(feature_df, how='cross')

            # Add the fully combined dataframe to the new relation space
            new_relation_space.add_relation(final_df, region_schema)

        return new_relation_space


class Crawl(MraOperatorBase):
    """
    A "mega-operator" that composes Represent, SliceTransform, SliceSelect,
    and Flatten into a single, convenient operation by building an internal pipeline.
    """
    def __init__(self, region_schemas: List[RelationSchema],
                 slice_transformations: List[SliceTransformationBase],
                 predicate_func: Callable[[RelationTuple, Dict[RelationSchema, pd.DataFrame]], bool],
                 dimensions: RelationSchema):
        self.region_schemas = region_schemas
        self.slice_transformations = slice_transformations
        self.feature_schemas = [t.feature_schema for t in self.slice_transformations]
        self.predicate_func = predicate_func
        self.dimensions = dimensions

        # The logic for handling the empty schema is now inside Crawl.
        represent_schemas = self.region_schemas[:] # Make a copy
        
        # Check if any transformation requires reference data.
        needs_ref_data = any(t.require_reference_data for t in self.slice_transformations)
        empty_schema = RelationSchema([])
        
        # If reference data is needed, automatically add the empty schema
        # to the list of schemas for the Represent operator.
        if needs_ref_data and empty_schema not in represent_schemas:
            represent_schemas.append(empty_schema)

        self.internal_pipeline = (
            Represent(
                region_schemas=represent_schemas,
                feature_schemas=self.feature_schemas
            ) |
            SliceTransform(
                slice_transformations=self.slice_transformations,
                dimensions=self.dimensions
            ) |
            SliceSelect(
                predicate_func=self.predicate_func
            ) |
            SliceProject(
                region_schemas=self.region_schemas
            ) |
            Flatten(
                dimensions=self.dimensions
            )
        )

    def _execute(self, data: RelationSpace) -> RelationSpace:
        if not isinstance(data, RelationSpace):
            raise TypeError("Crawl expects a RelationSpace as its initial input.")

        return self.internal_pipeline(data)

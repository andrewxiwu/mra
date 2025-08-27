import pandas as pd
from abc import ABC, abstractmethod
from typing import Union, List, Callable, Dict, Any, Tuple, Set
from itertools import chain, combinations

# Assuming you have a module named 'mra_data' with these classes defined.
from mra_data import (RelationSpace, SliceRelation, RelationSchema,
                      create_relation_tuple, RelationTuple)
from slice_transformations.slice_transformation import SliceTransformation

# ==============================================================================
# Type Alias for Data
# ==============================================================================
MraData = Union[pd.DataFrame, RelationSpace, SliceRelation]

# ==============================================================================
# Operator Base Class and Pipeline
# ==============================================================================

class MraOperatorBase(ABC):
    @abstractmethod
    def _execute(self, data: MraData) -> MraData:
        pass

    def __call__(self, data: MraData) -> MraData:
        print(f"Executing {self.__class__.__name__}...")
        return self._execute(data)

    def __or__(self, other: 'MraOperatorBase') -> 'Pipeline':
        if isinstance(other, Pipeline):
            return Pipeline([self] + other.operators)
        return Pipeline([self, other])

class Pipeline(MraOperatorBase):
    def __init__(self, operators: list[MraOperatorBase]):
        self.operators = operators

    def _execute(self, data: MraData) -> MraData:
        result = data
        for op in self.operators:
            result = op(result)
        return result

    def __or__(self, other: 'MraOperatorBase') -> 'Pipeline':
        if isinstance(other, Pipeline):
            return Pipeline(self.operators + other.operators)
        return Pipeline(self.operators + [other])

# ==============================================================================
# Concrete Operator Implementations
# ==============================================================================

class CreateRelationSpaceByCube(MraOperatorBase):
    def __init__(self, grouping_keys: List[str],
                 aggregations: Dict[str, Any]):
        self.grouping_keys = grouping_keys
        self.aggregations = aggregations

    def _execute(self, data: pd.DataFrame) -> RelationSpace:
        if not isinstance(data, pd.DataFrame):
            raise TypeError("CreateRelationSpaceByCube expects a DataFrame.")

        dims = RelationSchema(self.grouping_keys)
        relation_space = RelationSpace(dimensions=dims)
        power_set = chain.from_iterable(
            combinations(self.grouping_keys, r)
            for r in range(len(self.grouping_keys) + 1)
        )

        print(f"  > Generating cube for keys: {self.grouping_keys}")
        for group in power_set:
            group_list = list(group)
            if not group_list:
                agg_df = data.agg(self.aggregations).to_frame().T
            else:
                agg_df = data.groupby(group_list).agg(
                    self.aggregations).reset_index()
            dimensional_schema = RelationSchema(group_list)
            relation_space.add_relation(agg_df, dimensional_schema)

        return relation_space

class Represent(MraOperatorBase):
    def __init__(self, region_schemas: List[RelationSchema],
                 feature_schemas: List[RelationSchema]):
        self.region_schemas = region_schemas
        self.feature_schemas = feature_schemas

    def _execute(self, data: RelationSpace) -> SliceRelation:
        if not isinstance(data, RelationSpace):
            raise TypeError("Represent expects a RelationSpace object.")

        print(f"  > Representing RelationSpace into slices...")
        slice_relation = SliceRelation(dimensions=data.dimensions)

        for r_schema in self.region_schemas:
            for f_schema in self.feature_schemas:
                combined_attrs = set(r_schema.attributes + f_schema.attributes)
                target_dim_attrs = combined_attrs.intersection(
                    data.dimensions.attributes)
                target_dim_schema = RelationSchema(list(target_dim_attrs))
                source_relation = data.get_relation(target_dim_schema)

                if source_relation is None or not combined_attrs.issubset(
                        source_relation.columns):
                    continue

                region_cols = list(r_schema.attributes)
                feature_cols = list(f_schema.attributes)

                if not region_cols:
                    region_tuple = create_relation_tuple({})
                    feature_data = source_relation[feature_cols].copy()
                    slice_relation.add_slice_tuple(
                        region_tuple, f_schema, feature_data)
                else:
                    for region_vals, group_df in source_relation.groupby(
                            region_cols):
                        region_dict = dict(zip(region_cols,
                            region_vals if isinstance(region_vals, tuple)
                            else (region_vals,)))
                        region_tuple = create_relation_tuple(region_dict)
                        feature_data = group_df[feature_cols].copy(
                            ).reset_index(drop=True)
                        slice_relation.add_slice_tuple(
                            region_tuple, f_schema, feature_data)

        return slice_relation

class SliceTransform(MraOperatorBase):
    """
    Transforms feature tables within a SliceRelation by applying a sequence
    of slice transformations.
    """
    def __init__(
            self,
            slice_transformations: List[SliceTransformation],
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

            if self.drill_down_regions is not None:
                if not self._is_descendant(
                        region,
                        self.drill_down_regions,
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
    def __init__(self, predicate_func: Callable[
            [RelationTuple, Dict[RelationSchema, pd.DataFrame]], bool]):
        self.predicate_func = predicate_func

    def _execute(self, data: SliceRelation) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceSelect expects a SliceRelation object.")

        print(f"  > Selecting slices...")
        new_slice_relation = SliceRelation(dimensions=data.dimensions)
        empty_region_tuple = create_relation_tuple({})

        for region, features in data.data.items():
            if region == empty_region_tuple:
                continue

            if self.predicate_func(region, features):
                for feature_schema, feature_df in features.items():
                    new_slice_relation.add_slice_tuple(
                        region, feature_schema, feature_df)

        if empty_region_tuple in data.data:
            reference_features = data.data[empty_region_tuple]
            for f_schema, f_df in reference_features.items():
                new_slice_relation.add_slice_tuple(
                    empty_region_tuple, f_schema, f_df)

        return new_slice_relation

class SliceProject(MraOperatorBase):
    def __init__(self, region_schemas: List[RelationSchema],
                 feature_schemas: List[RelationSchema] = None):
        self.region_schemas = region_schemas
        self.feature_schemas = feature_schemas

    def _execute(self, data: SliceRelation) -> SliceRelation:
        if not isinstance(data, SliceRelation):
            raise TypeError("SliceProject expects a SliceRelation object.")
        
        print(f"  > Projecting slices...")
        new_slice_relation = SliceRelation(dimensions=data.dimensions)
        
        allowed_region_schemas = {
            RelationSchema(list(rs.attributes)) for rs in self.region_schemas
        }

        for region, features in data.data.items():
            current_region_schema = RelationSchema([k for k, v in region])
            
            if current_region_schema in allowed_region_schemas:
                features_to_keep = features
                if self.feature_schemas is not None:
                    allowed_f_schemas = set(self.feature_schemas)
                    features_to_keep = {
                        fs: df for fs, df in features.items()
                        if fs in allowed_f_schemas
                    }

                for f_schema, f_df in features_to_keep.items():
                    new_slice_relation.add_slice_tuple(
                        region, f_schema, f_df)

        return new_slice_relation

class Flatten(MraOperatorBase):
    def __init__(self, dimensions: RelationSchema):
        self.dimensions = dimensions

    def _execute(self, data: SliceRelation) -> RelationSpace:
        if not isinstance(data, SliceRelation):
            raise TypeError("Flatten expects a SliceRelation object.")

        print("  > Flattening SliceRelation to RelationSpace...")
        new_relation_space = RelationSpace(dimensions=self.dimensions)
        
        for region, features in data.data.items():
            if not region:
                continue
            
            region_df = pd.DataFrame([dict(region)])
            final_df = region_df
            
            for feature_df in features.values():
                if not feature_df.empty:
                    final_df = final_df.merge(feature_df, how='cross')

            dimensional_schema = RelationSchema(list(dict(region).keys()))
            new_relation_space.add_relation(final_df, dimensional_schema)

        return new_relation_space

class Crawl(MraOperatorBase):
    def __init__(self, region_schemas: List[RelationSchema],
                 slice_transformations: List[SliceTransformation],
                 predicate_func: Callable[
                     [RelationTuple, Dict[RelationSchema, pd.DataFrame]], bool
                 ],
                 dimensions: RelationSchema):
        self.region_schemas = region_schemas
        self.slice_transformations = slice_transformations
        self.feature_schemas = [t.feature_schema for t in
                                self.slice_transformations]
        self.predicate_func = predicate_func
        self.dimensions = dimensions

        represent_schemas = self.region_schemas[:]
        needs_ref_data = any(t.require_reference_data for t in
                             self.slice_transformations)
        empty_schema = RelationSchema([])
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
            raise TypeError("Crawl expects a RelationSpace as input.")

        return self.internal_pipeline(data)

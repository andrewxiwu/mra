import unittest
import pandas as pd
from typing import Set

# Import data structures and operators to be tested
from mra_data import (SliceRelation, RelationSchema, RelationTuple,
                      create_relation_tuple)
from mra_operators import SliceTransform

# A dummy transformation class for testing purposes
class CopyTransformation:
    def __init__(self, feature_schema):
        self.feature_schema = feature_schema
        self.require_reference_data = False

    def __call__(self, df):
        # The transformation logic itself doesn't matter for this test
        return df.copy()

class SliceTransformTest(unittest.TestCase):
    """
    Test suite for the SliceTransform operator.
    """

    def test_drill_down_bfs(self):
        """
        Tests drill-down for a Breadth-First Search (BFS) scenario where
        all parent schemas are known.
        """
        # --- Test Setup ---
        slice_relation = SliceRelation(
            dimensions=RelationSchema(['Device', 'Browser']))
        cost_schema = RelationSchema(['Cost'])
        
        pixel_region = create_relation_tuple({'Device': 'Pixel'})
        chrome_region = create_relation_tuple({'Browser': 'Chrome'})
        
        pixel_chrome = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Chrome'})
        pixel_safari = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Safari'})
            
        slice_relation.add_slice_tuple(
            pixel_chrome, cost_schema, pd.DataFrame({'Cost': [100]}))
        slice_relation.add_slice_tuple(
            pixel_safari, cost_schema, pd.DataFrame({'Cost': [200]}))

        drill_down_parents: Set[RelationTuple] = {
            pixel_region,
            chrome_region
        }
        # In BFS, we provide all possible parent schemas
        parent_schemas: Set[RelationSchema] = {
            RelationSchema(['Device']),
            RelationSchema(['Browser'])
        }
        # --- End Test Setup ---

        transform_op = SliceTransform(
            slice_transformations=[CopyTransformation(cost_schema)],
            dimensions=slice_relation.dimensions,
            drill_down_regions=drill_down_parents,
            parent_region_schemas=parent_schemas
        )
        result = transform_op(slice_relation)

        # Assertions
        self.assertIn(pixel_chrome, result.data)
        self.assertNotIn(pixel_safari, result.data)

    def test_drill_down_dfs(self):
        """
        Tests drill-down for a Depth-First Search (DFS) scenario where
        only a subset of parent schemas is relevant.
        """
        # --- Test Setup ---
        slice_relation = SliceRelation(
            dimensions=RelationSchema(['Device', 'Browser']))
        cost_schema = RelationSchema(['Cost'])
        
        pixel_region = create_relation_tuple({'Device': 'Pixel'})
        
        pixel_chrome = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Chrome'})
        pixel_safari = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Safari'})
            
        slice_relation.add_slice_tuple(
            pixel_chrome, cost_schema, pd.DataFrame({'Cost': [100]}))
        slice_relation.add_slice_tuple(
            pixel_safari, cost_schema, pd.DataFrame({'Cost': [200]}))

        # In a DFS-style exploration, we might only have processed the
        # 'Device' level parents so far.
        drill_down_parents: Set[RelationTuple] = {pixel_region}
        parent_schemas: Set[RelationSchema] = {RelationSchema(['Device'])}
        # --- End Test Setup ---

        transform_op = SliceTransform(
            slice_transformations=[CopyTransformation(cost_schema)],
            dimensions=slice_relation.dimensions,
            drill_down_regions=drill_down_parents,
            parent_region_schemas=parent_schemas
        )
        result = transform_op(slice_relation)

        # The logic should only check the projection to 'Device'. Since
        # the projection to 'Browser' is not in `parent_schemas`, it's
        # ignored. Both regions should pass this check.
        self.assertIn(pixel_chrome, result.data)
        self.assertIn(pixel_safari, result.data)

if __name__ == '__main__':
    unittest.main()

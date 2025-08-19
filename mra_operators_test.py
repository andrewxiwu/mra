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

    def test_drill_down(self):
        """
        Tests that SliceTransform correctly applies the drill-down filter.
        """
        # --- Test Setup ---
        slice_relation = SliceRelation(
            dimensions=RelationSchema(['Device', 'Browser']))
        
        cost_schema = RelationSchema(['Cost'])
        
        # Level 1 Regions
        pixel_region = create_relation_tuple({'Device': 'Pixel'})
        chrome_region = create_relation_tuple({'Browser': 'Chrome'})
        
        # Level 2 Regions (combinations of level 1)
        pixel_chrome = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Chrome'})
        pixel_safari = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Safari'})
            
        # Populate the slice relation
        slice_relation.add_slice_tuple(
            pixel_chrome, cost_schema, pd.DataFrame({'Cost': [100]}))
        slice_relation.add_slice_tuple(
            pixel_safari, cost_schema, pd.DataFrame({'Cost': [200]}))

        # Define the set of valid "parent" regions for the drill-down.
        # Note that (Browser=Safari) is intentionally excluded.
        drill_down_parents: Set[RelationTuple] = {
            pixel_region,
            chrome_region
        }
        # --- End Test Setup ---

        # Create the SliceTransform operator with the drill-down set.
        transform_op = SliceTransform(
            slice_transformations=[
                CopyTransformation(cost_schema)
            ],
            dimensions=slice_relation.dimensions,
            drill_down_regions=drill_down_parents
        )

        # Execute the transformation
        result_slice_relation = transform_op(slice_relation)

        # Assertions
        # 1. The valid descendant region (Pixel, Chrome) SHOULD be in the result.
        self.assertIn(pixel_chrome, result_slice_relation.data)

        # 2. The invalid descendant region (Pixel, Safari) should NOT be in the result.
        self.assertNotIn(pixel_safari, result_slice_relation.data)

if __name__ == '__main__':
    unittest.main()

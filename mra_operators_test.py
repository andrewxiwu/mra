import unittest
import pandas as pd
from typing import Set

# Import data structures and operators to be tested
from mra_data import (SliceRelation, RelationSchema, RelationTuple,
                      create_relation_tuple)
from mra_operators import SliceTransform

# A dummy transformation class for testing purposes
class MockTransformation:
    def __init__(self, feature_schema):
        self.feature_schema = feature_schema
        self.require_reference_data = False

    def __call__(self, df):
        # The transformation logic itself doesn't matter for this test
        return df.copy()

class SliceTransformDrillDownTest(unittest.TestCase):
    """
    Test suite for the drill-down functionality of the SliceTransform operator.
    """

    def setUp(self):
        """Set up a sample SliceRelation for the tests."""
        self.slice_relation = SliceRelation(
            dimensions=RelationSchema(['Device', 'Browser']))
        
        # Schemas
        self.cost_schema = RelationSchema(['Cost'])
        
        # Level 1 Regions
        self.pixel_region = create_relation_tuple({'Device': 'Pixel'})
        self.chrome_region = create_relation_tuple({'Browser': 'Chrome'})
        self.safari_region = create_relation_tuple({'Browser': 'Safari'})

        # Level 2 Regions (combinations of level 1)
        self.pixel_chrome = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Chrome'})
        self.pixel_safari = create_relation_tuple(
            {'Device': 'Pixel', 'Browser': 'Safari'})
            
        # Populate the slice relation
        self.slice_relation.add_slice_tuple(
            self.pixel_chrome, self.cost_schema, pd.DataFrame({'Cost': [100]}))
        self.slice_relation.add_slice_tuple(
            self.pixel_safari, self.cost_schema, pd.DataFrame({'Cost': [200]}))

    def test_drill_down_logic(self):
        """
        Tests that SliceTransform correctly applies the drill-down filter.
        """
        # Define the set of valid "parent" regions for the drill-down.
        # Note that (Browser=Safari) is intentionally excluded.

        drill_down_parents: Set[RelationTuple] = {
            self.pixel_region,
            self.chrome_region
        }

        transform_op = SliceTransform(
            slice_transformations=[
                MockTransformation(self.cost_schema)
            ],
            dimensions=self.slice_relation.dimensions,
            drill_down_regions=drill_down_parents
        )

        # Execute the transformation
        result_slice_relation = transform_op(self.slice_relation)

        # Assertions
        # 1. The valid descendant region (Pixel, Chrome) SHOULD be in the result.
        #    Its parents are (Pixel) and (Chrome), which are both in the set.
        self.assertIn(self.pixel_chrome, result_slice_relation.data)

        # 2. The invalid descendant region (Pixel, Safari) should NOT be in the result.
        #    Its parent (Safari) is not in the drill-down set.
        self.assertNotIn(self.pixel_safari, result_slice_relation.data)

if __name__ == '__main__':
    unittest.main()

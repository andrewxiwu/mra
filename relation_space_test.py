import unittest
import pandas as pd

# Import the classes to be tested from your data module
from mra_data import RelationSpace, RelationSchema

class RelationSpaceTest(unittest.TestCase):
    """
    Test suite for the validation logic in the RelationSpace.add_relation method.
    """

    def setUp(self):
        """
        Set up a common RelationSpace instance for all tests.
        The dimensions for this space are ('Browser', 'Device').
        """
        self.space_dimensions = RelationSchema(['Device', 'Browser'])
        self.relation_space = RelationSpace(dimensions=self.space_dimensions)

    def test_add_relation_valid(self):
        """
        Tests that a valid relation can be added without raising an error.
        
        A relation is valid if the intersection of its columns with the space's
        dimensions is exactly equal to its dimensional schema.
        """
        # This relation has columns ['Device', 'Cost'].
        # The intersection with the space dimensions ['Device', 'Browser'] is {'Device'}.
        # This matches the dimensional schema ['Device'], so it should be valid.
        valid_relation = pd.DataFrame({'Device': ['Pixel'], 'Cost': [100]})
        dimensional_schema = RelationSchema(['Device'])
        
        try:
            self.relation_space.add_relation(valid_relation, dimensional_schema)
        except ValueError:
            self.fail("add_relation() raised ValueError unexpectedly for a valid relation.")

    def test_add_relation_invalid(self):
        """
        Tests that adding an invalid relation correctly raises a ValueError.
        
        This relation is invalid because the intersection of its columns with
        the space's dimensions does not match its dimensional schema.
        """
        # This relation has columns ['Device', 'Browser', 'Revenue'].
        # The intersection with the space dimensions ['Device', 'Browser'] is {'Device', 'Browser'}.
        # This does NOT match the dimensional schema ['Device'], so it is invalid.
        invalid_relation = pd.DataFrame({'Device': ['Pixel'], 'Browser': ['Chrome'], 'Revenue': [500]})
        dimensional_schema = RelationSchema(['Device'])
        
        # We expect a ValueError to be raised when adding this invalid relation.
        # The `with self.assertRaises(...)` block will catch and pass the test if the
        # expected error is raised. The test will fail if it is not.
        with self.assertRaises(ValueError):
            self.relation_space.add_relation(invalid_relation, dimensional_schema)

if __name__ == '__main__':
    unittest.main()

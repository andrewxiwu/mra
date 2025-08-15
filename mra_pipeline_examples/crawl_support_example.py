import pandas as pd

# Import data structures
from mra_data import RelationSpace, RelationSchema, create_relation_tuple

# Import the necessary operators
from mra_operators import CreateRelationSpaceByCube, Crawl

# Import the transformation class
from slice_transformations.support_transformation import SupportTransformation

def main():
    """
    Demonstrates an end-to-end pipeline using the CreateRelationSpace and 
    Crawl mega-operators to find high-support regions.
    """
    print("🚀 End-to-End Pipeline: Crawl for High Support Regions 🚀")
    print("="*60 + "\n")

    # 1. Create a base DataFrame with more attributes.
    base_data = {
        'Device': ['Pixel', 'Pixel', 'iPhone', 'iPhone', 'Surface', 'Pixel'],
        'Browser': ['Chrome', 'Chrome', 'Safari', 'Safari', 'Edge', 'Edge'],
        'Cost': [100, 200, 50, 150, 1000, 80]
    }
    base_relation = pd.DataFrame(base_data)
    print("--- 1. Base Input DataFrame ---")
    print(base_relation)
    print("\n" + "="*60 + "\n")

    # 2. Define the transformation and a predicate for filtering.
    support_transformer = SupportTransformation(mass_column='Cost')

    def high_support_predicate(region, features):
        """Predicate to select regions with support > 0.5"""
        support_schema = RelationSchema(['support'])
        if support_schema in features:
            return features[support_schema]['support'].iloc[0] > 0.5
        return False

    # 3. Define all the non-empty region schemas we want to analyze.
    device_schema = RelationSchema(['Device'])
    browser_schema = RelationSchema(['Browser'])
    device_browser_schema = RelationSchema(['Device', 'Browser'])

    # 4. Define the full MRA pipeline using the high-level operators.
    pipeline = (
        # First, create a comprehensive space for everything ('Device' and 'Browser').
        CreateRelationSpaceByCube(
            grouping_keys=['Device', 'Browser'],
            aggregations={'Cost': 'sum'}
        )
        # Then, crawl and analyze all specified non-empty regions.
        | Crawl(
            region_schemas=[device_schema, browser_schema, device_browser_schema],
            slice_transformations=[support_transformer],
            predicate_func=high_support_predicate,
            dimensions=RelationSchema(['Device', 'Browser', 'Cost'])
          )
    )

    # Run the pipeline on the base data
    final_result_space = pipeline(base_relation)
    
    print("\n--- 2. Final Resulting RelationSpace ---")
    print("This space contains only the regions with support > 0.5.")
    print(final_result_space)

    print("="*60)
    print("✅ Crawl for Support Example Finished ✅")

if __name__ == '__main__':
    main()

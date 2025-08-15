import pandas as pd

# Import data structures
from mra_data import SliceRelation, RelationSchema, create_relation_tuple

# Import the necessary operators
from mra_operators import CreateRelationSpaceByCube, Represent, SliceProject

def main():
    """
    Demonstrates the use of the SliceProject operator to filter a 
    SliceRelation based on region schemas.
    """
    print("🚀 SliceProject Operator Example 🚀")
    print("="*60 + "\n")

    # 1. Create a base DataFrame.
    base_data = {
        'Device': ['Pixel', 'iPhone', 'Surface'],
        'Browser': ['Chrome', 'Safari', 'Edge'],
        'Cost': [300, 200, 1000]
    }
    base_relation = pd.DataFrame(base_data)

    # 2. Define the schemas for the regions we want to create.
    device_schema = RelationSchema(['Device'])
    browser_schema = RelationSchema(['Browser'])
    device_browser_schema = RelationSchema(['Device', 'Browser'])
    cost_schema = RelationSchema(['Cost'])

    # 3. Create a pipeline to generate a SliceRelation with multiple region types.
    # This gives us a rich dataset to filter.
    create_and_represent_pipeline = (
        CreateRelationSpaceByCube(
            grouping_keys=['Device', 'Browser'],
            aggregations={'Cost': 'sum'}
        )
        | Represent(
            region_schemas=[device_schema, browser_schema, device_browser_schema],
            feature_schemas=[cost_schema]
          )
    )
    
    initial_slice_relation = create_and_represent_pipeline(base_relation)
    
    print("--- 1. Initial SliceRelation (contains multiple region types) ---")
    print(initial_slice_relation)

    # 4. Define and apply the SliceProject operator.
    # We want to keep ONLY the regions with 'Device' or 'Browser' schemas,
    # discarding the more complex ('Device', 'Browser') regions.
    project_op = SliceProject(
        region_schemas=[device_schema, browser_schema]
    )

    projected_slice_relation = project_op(initial_slice_relation)

    print("\n--- 2. Projected SliceRelation ---")
    print("This relation contains only the regions with the specified schemas.")
    print(projected_slice_relation)

    print("="*60)
    print("✅ SliceProject Example Finished ✅")

if __name__ == '__main__':
    main()

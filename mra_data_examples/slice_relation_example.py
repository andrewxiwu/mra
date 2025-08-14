import pandas as pd
from mra_data import RelationSchema, create_relation_tuple, SliceRelation

def main():
    """
    This script demonstrates the creation and usage of the SliceRelation class.
    """
    print("🚀 SliceRelation Showcase Example �")
    print("="*50 + "\n")

    # 1. Create an empty SliceRelation instance.
    slice_relation = SliceRelation(dimensions=RelationSchema(['Device', 'Date']))
    print("--- 1. Created an empty SliceRelation ---")
    print(slice_relation)

    # 2. Define the regions (RelationTuples) that will be the primary keys.
    # We use the `create_relation_tuple` factory function for convenience.
    region1 = create_relation_tuple({'Device': 'Pixel'})
    region2 = create_relation_tuple({'Device': 'iPhone'})

    # 3. Define the schemas for our different feature tables.
    daily_cpc_schema = RelationSchema(['Date', 'Cpc'])
    total_cost_schema = RelationSchema(['TotalCost'])

    # 4. Create the feature tables (pandas DataFrames).
    
    # Feature data for the 'Pixel' region
    pixel_daily_cpc = pd.DataFrame({
        'Date': pd.to_datetime(['2025-01-01', '2025-01-02']),
        'Cpc': [5.0, 40.0]
    })
    pixel_total_cost = pd.DataFrame({'TotalCost': [300]})

    # Feature data for the 'iPhone' region
    iphone_daily_cpc = pd.DataFrame({
        'Date': pd.to_datetime(['2025-01-01', '2025-01-02']),
        'Cpc': [5.0, 5.0]
    })
    iphone_total_cost = pd.DataFrame({'TotalCost': [200]})

    # 5. Populate the SliceRelation using the `add_slice_tuple` method.
    # This builds the nested dictionary structure.
    slice_relation.add_slice_tuple(region1, daily_cpc_schema, pixel_daily_cpc)
    slice_relation.add_slice_tuple(region1, total_cost_schema, pixel_total_cost)
    slice_relation.add_slice_tuple(region2, daily_cpc_schema, iphone_daily_cpc)
    slice_relation.add_slice_tuple(region2, total_cost_schema, iphone_total_cost)

    print("--- 2. Populated the SliceRelation ---")
    print("The data is now organized into slice tuples, keyed by region.")
    print(slice_relation)
    
    print("="*50)
    print("✅ SliceRelation Example Finished ✅")


if __name__ == '__main__':
    main()

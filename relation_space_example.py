import pandas as pd
from mra_data import RelationSchema, RelationSpace

def main():
    """
    This script demonstrates creating a RelationSpace with specific dimensions
    and populating it with various aggregated relations.
    """
    print("🚀 RelationSpace Showcase: Identifying Relations by Dimension 🚀")
    print("="*60 + "\n")

    # 1. Define the core dimensions for the RelationSpace.
    # These attributes will be used to identify and retrieve relations.
    space_dimensions = RelationSchema(['Device', 'Browser'])

    # 2. Create the RelationSpace instance.
    relation_space = RelationSpace(dimensions=space_dimensions)
    print("--- 1. Created a RelationSpace with Dimensions: ('Browser', 'Device') ---")
    
    # 3. Create a base DataFrame with dimensions and other value attributes.
    base_data = {
        'Device': ['Pixel', 'Pixel', 'iPhone', 'iPhone', 'Surface', 'Pixel'],
        'Browser': ['Chrome', 'Chrome', 'Safari', 'Safari', 'Edge', 'Edge'],
        'Date': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-01', '2025-01-02', '2025-01-01', '2025-01-01']),
        'Cost': [100, 200, 150, 50, 1200, 80],
        'Clicks': [20, 5, 30, 10, 60, 10]
    }
    base_relation = pd.DataFrame(base_data)
    print("\n--- 2. Using this base data to create relations ---")
    print(base_relation.to_string(index=False))
    print("\n" + "="*60 + "\n")

    # 4. Create several aggregated relations from the base data.
    
    # Relation 1: Aggregated by 'Device'
    cost_by_device = base_relation.groupby('Device')['Cost'].sum().reset_index()
    device_schema = RelationSchema(['Device'])

    # Relation 2: Aggregated by 'Browser'
    cost_by_browser = base_relation.groupby('Browser')['Cost'].sum().reset_index()
    browser_schema = RelationSchema(['Browser'])

    # Relation 3: Aggregated by 'Device' and 'Browser'
    cost_by_device_browser = base_relation.groupby(['Device', 'Browser'])['Cost'].sum().reset_index()
    device_browser_schema = RelationSchema(['Device', 'Browser'])

    # 5. Add these relations to the RelationSpace.
    # Each relation is indexed by its unique dimensional schema.
    relation_space.add_relation(cost_by_device, device_schema)
    relation_space.add_relation(cost_by_browser, browser_schema)
    relation_space.add_relation(cost_by_device_browser, device_browser_schema)

    print("--- 3. Populated RelationSpace ---")
    print("The space now contains three distinct relations, each identified by its dimensions.")
    print(relation_space)

    # 6. Retrieve a specific relation using only its dimensional schema.
    print("--- 4. Retrieving the relation with dimensions ('Browser',) ---")
    
    # We create a new schema object to find the relation.
    # Note that the order of attributes in the list doesn't matter.
    schema_to_find = RelationSchema(['Browser'])
    retrieved_relation = relation_space.get_relation(schema_to_find)
    
    if retrieved_relation is not None:
        print(f"Successfully retrieved relation for schema {schema_to_find.attributes}:")
        print(retrieved_relation.to_string(index=False))
    else:
        print("Relation not found.")
        
    print("\n" + "="*60)
    print("✅ RelationSpace Example Finished ✅")


if __name__ == '__main__':
    main()

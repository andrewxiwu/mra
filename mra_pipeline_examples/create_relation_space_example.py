import pandas as pd

# Import the necessary operator and data structures
from mra_operators import CreateRelationSpaceByCube
from mra_data import RelationSpace, RelationSchema

def main():
    """
    Demonstrates the use of the CreateRelationSpaceByCube operator to generate
    a RelationSpace from a base DataFrame.
    """
    print("🚀 CreateRelationSpaceByCube Example �")
    print("="*60 + "\n")

    # 1. Create a base DataFrame with several attributes.
    base_data = {
        'Device': ['Pixel', 'Pixel', 'iPhone', 'iPhone', 'Surface', 'Pixel'],
        'Browser': ['Chrome', 'Chrome', 'Safari', 'Safari', 'Edge', 'Edge'],
        'Cost': [100, 200, 50, 150, 1000, 80]
    }
    base_relation = pd.DataFrame(base_data)
    print("--- 1. Base Input DataFrame ---")
    print(base_relation)
    print("\n" + "="*60 + "\n")

    # 2. Define the operator.
    # We want to create a "cube" by grouping by 'Device' and 'Browser'
    # and summing the 'Cost' for each group.
    create_space_op = CreateRelationSpaceByCube(
        grouping_keys=['Device', 'Browser'],
        aggregations={'Cost': 'sum'}
    )

    # 3. Execute the operator on the base DataFrame.
    # This will generate a RelationSpace containing relations for:
    # - () -> Global total cost
    # - ('Device',) -> Total cost per device
    # - ('Browser',) -> Total cost per browser
    # - ('Device', 'Browser') -> Total cost per device-browser combination
    relation_space_result = create_space_op(base_relation)

    print("--- 2. Resulting RelationSpace ---")
    print("The space contains all aggregations from the cube operation.")
    print(relation_space_result)

    print("="*60)
    print("✅ CreateRelationSpaceByCube Example Finished ✅")

if __name__ == '__main__':
    main()


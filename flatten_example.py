import pandas as pd
import numpy as np

# Import the MRA operators and data structures
from mra_operators import CreateRelationSpaceByCube, Represent, Flatten
from mra_data import RelationSpace, SliceRelation, RelationSchema

def run_flatten_example():
    """
    Demonstrates a pipeline using the Flatten operator with a more complex
    intermediate SliceRelation.
    """
    # 1. Create some sample data (a pandas DataFrame) from a list of rows
    data_rows = [
        # device,  browser,   clicks, cost
        ['Pixel',   'Chrome',   100,    10],
        ['Pixel',   'Firefox',   50,     8],
        ['iPhone',  'Safari',   200,    25],
        ['Surface', 'Edge',     150,    22],
        ['iPhone',  'Chrome',   300,    40]
    ]
    sample_data = pd.DataFrame(data_rows, columns=['device', 'browser', 'clicks', 'cost'])
    
    print("----------- Initial DataFrame -----------")
    print(sample_data.to_string(index=False))
    print("\n" + "="*50 + "\n")

    # 2. Define the full pipeline
    pipeline = (
        CreateRelationSpaceByCube(
            grouping_keys=['device', 'browser'],
            aggregations={'clicks': 'sum', 'cost': 'sum'}
        ) |
        Represent(
            # Create slices for both device and browser regions
            region_schemas=[
                RelationSchema(['device']),
                RelationSchema(['browser']),
            ],
            # Create separate feature tables for clicks and cost
            feature_schemas=[
                RelationSchema(['clicks']),
                RelationSchema(['cost'])
            ]
        ) |
        Flatten(
            dimensions=RelationSchema(['device', 'browser'])
        )
    )

    # 3. Execute the pipeline on the data
    print("----------- Executing Pipeline -----------")
    final_result = pipeline(sample_data)
    
    print("\n" + "="*50 + "\n")
    print("----------- Final Result (Flattened RelationSpace) -----------")
    print(final_result)


if __name__ == '__main__':
    run_flatten_example()

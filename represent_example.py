import pandas as pd
import numpy as np

# Import the MRA operators and data structures
from mra_operators import CreateRelationSpaceByCube, Represent
from mra_data import RelationSpace, SliceRelation, RelationSchema

def run_example():
    """
    Demonstrates a pipeline using MRA operators.
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

    # 2. Define a pipeline of operations using the `|` operator
    #    - Create a RelationSpace by cubing on 'device' and 'browser'.
    #    - Represent the resulting space as a SliceRelation.
    pipeline = (
        CreateRelationSpaceByCube(
            grouping_keys=['device', 'browser'],
            aggregations={'clicks': 'sum', 'cost': 'sum'}
        ) |
        Represent(
            region_schemas=[
                RelationSchema(['device']), 
                RelationSchema(['browser'])
            ],
            feature_schemas=[
                RelationSchema(['clicks', 'cost'])
            ]
        )
    )

    # 3. Execute the pipeline on the data by calling the pipeline object
    print("----------- Executing Pipeline -----------")
    final_result = pipeline(sample_data)
    
    print("\n" + "="*50 + "\n")
    print("----------- Final Result (SliceRelation) -----------")
    print(final_result)


if __name__ == '__main__':
    run_example()

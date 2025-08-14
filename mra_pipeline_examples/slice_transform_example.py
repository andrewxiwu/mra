import pandas as pd
import numpy as np

# Import the MRA operators and data structures
from mra_operators import CreateRelationSpaceByCube, Represent, SliceTransform
from mra_data import RelationSpace, SliceRelation, RelationSchema
from ratio_transformation import RatioTransformation

def run_transform_example():
    """
    Demonstrates a pipeline using the SliceTransform operator.
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
    
    # First, define the transformation we want to apply.
    # We'll calculate cost_per_click from the 'clicks' and 'cost' columns.
    cost_per_click_transformer = RatioTransformation(
        numerator_col='cost',
        denominator_col='clicks',
        output_col='cost_per_click'
    )
    
    # Now, build the operator pipeline
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
        ) |
        SliceTransform(
            slice_transformations=[cost_per_click_transformer],
            dimensions=RelationSchema(['device', 'browser'])
        )
    )

    # 3. Execute the pipeline on the data
    print("----------- Executing Pipeline -----------")
    final_result = pipeline(sample_data)
    
    print("\n" + "="*50 + "\n")
    print("----------- Final Result (Transformed SliceRelation) -----------")
    print(final_result)


if __name__ == '__main__':
    run_transform_example()

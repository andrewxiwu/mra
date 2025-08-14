import pandas as pd
import numpy as np
from typing import Dict

# Import the MRA operators and data structures
from mra_operators import CreateRelationSpaceByCube, Represent, SliceSelect
from mra_data import RelationSpace, SliceRelation, RelationSchema, RelationTuple

def run_select_example():
    """
    Demonstrates a pipeline using the SliceSelect operator.
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

    # 2. Define a predicate function for the SliceSelect operator.
    # This predicate will select slices where:
    #   a) The region is for the 'iPhone' device.
    #   b) The total clicks for that device are greater than 400.
    def iphone_with_high_clicks(region: RelationTuple, features: Dict[RelationSchema, pd.DataFrame]) -> bool:
        # Condition (a): Check the region tuple
        is_iphone = ('device', 'iPhone') in region
        if not is_iphone:
            return False

        # Condition (b): Check the feature table for total clicks
        feature_schema_to_check = RelationSchema(['clicks', 'cost'])
        feature_df = features.get(feature_schema_to_check)

        if feature_df is not None and not feature_df.empty:
            total_clicks = feature_df['clicks'].sum()
            return total_clicks > 400
            
        return False

    # 3. Define the full pipeline
    pipeline = (
        CreateRelationSpaceByCube(
            grouping_keys=['device', 'browser'],
            aggregations={'clicks': 'sum', 'cost': 'sum'}
        ) |
        Represent(
            region_schemas=[
                RelationSchema(['device']), 
            ],
            feature_schemas=[
                RelationSchema(['clicks', 'cost'])
            ]
        ) |
        SliceSelect(
            predicate_func=iphone_with_high_clicks
        )
    )

    # 4. Execute the pipeline on the data
    print("----------- Executing Pipeline -----------")
    final_result = pipeline(sample_data)
    
    print("\n" + "="*50 + "\n")
    print("----------- Final Result (Filtered SliceRelation) -----------")
    print(final_result)


if __name__ == '__main__':
    run_select_example()

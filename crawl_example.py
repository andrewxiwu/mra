import pandas as pd
import numpy as np
from typing import Dict

# Import the MRA operators and data structures
from mra_operators import CreateRelationSpaceByCube, Crawl
from mra_data import RelationSpace, SliceRelation, RelationSchema, RelationTuple
from ratio_transformation import RatioTransformation

def run_crawl_example():
    """
    Demonstrates a full pipeline using the Crawl "mega-operator".
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

    # 2. Define the components for the Crawl operator

    # Define the transformation to be applied
    cost_per_click_transformer = RatioTransformation(
        numerator_col='cost',
        denominator_col='clicks',
        output_col='cost_per_click'
    )

    # Define a predicate to filter slices
    # This will keep regions where the total cost is greater than 30
    def total_cost_predicate(region: RelationTuple, features: Dict[RelationSchema, pd.DataFrame]) -> bool:
        # The feature schema for the transformation's output will have all three columns
        feature_schema_to_check = RelationSchema(['clicks', 'cost', 'cost_per_click'])
        feature_df = features.get(feature_schema_to_check)

        if feature_df is not None and not feature_df.empty:
            total_cost = feature_df['cost'].sum()
            return total_cost > 30
        return False

    # 3. Define the full pipeline by chaining the operators
    #    Stage 1: Create the initial RelationSpace using a cube.
    #    Stage 2: Apply the Crawl operator to the RelationSpace.
    
    pipeline = (
        CreateRelationSpaceByCube(
            grouping_keys=['device', 'browser'],
            aggregations={'clicks': 'sum', 'cost': 'sum'}
        ) |
        Crawl(
            region_schemas=[
                RelationSchema(['device']),
                RelationSchema(['browser']),
                RelationSchema(['device', 'browser'])
            ],
            slice_transformations=[cost_per_click_transformer],
            predicate_func=total_cost_predicate,
            dimensions=RelationSchema(['device', 'browser'])
        )
    )

    # 4. Execute the entire pipeline with a single call
    print("----------- Executing Pipeline -----------")
    final_result = pipeline(sample_data)
    
    print("\n" + "="*50 + "\n")
    print("----------- Final Result (Crawled RelationSpace) -----------")
    print(final_result)


if __name__ == '__main__':
    run_crawl_example()

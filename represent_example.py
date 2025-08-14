import pandas as pd
import numpy as np

# Import the MRA operators and data structures
from mra_operators import CreateRelationSpaceByCube, Represent
from mra_data import RelationSpace, SliceRelation, RelationSchema

def run_example():
    """
    Demonstrates a pipeline using MRA operators.
    """
    # 1. Create some sample data (a pandas DataFrame)
    sample_data = pd.DataFrame({
        'device': ['Pixel', 'Pixel', 'iPhone', 'Surface', 'iPhone'],
        'browser': ['Chrome', 'Firefox', 'Safari', 'Edge', 'Chrome'],
        'clicks': [100, 50, 200, 150, 300],
        'cost': [10, 8, 25, 22, 40]
    })
    print("----------- Initial DataFrame -----------")
    print(sample_data.to_string(index=False))
    print("\n" + "="*50 + "\n")

    # 2. Define a pipeline of operations using the `|` operator
    #    - Create a RelationSpace by cubing on 'device' and 'browser'.
    #    - Represent the resulting space as a SliceRelation, with 'device' as the region.
    pipeline = (
        CreateRelationSpaceByCube(
            grouping_keys=['device', 'browser'],
            aggregations={'clicks': 'sum', 'cost': 'sum'}
        ) |
        Represent(region_key='device')
    )

    # 3. Execute the pipeline on the data
    print("----------- Executing Pipeline -----------")
    final_result = sample_data | pipeline
    
    print("\n" + "="*50 + "\n")
    print("----------- Final Result (SliceRelation) -----------")
    print(final_result)


if __name__ == '__main__':
    run_example()

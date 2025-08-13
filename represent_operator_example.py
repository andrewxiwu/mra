"""An example to show the represent operator.""""
import pandas as pd
from typing import Dict

# Import the MRA components from the algebra module
from mra_algebra import RelationSpace, represent, SliceRelation

# -----------------------------------------------------------------------------
# Example: Demonstrating the 'represent' Operator
# -----------------------------------------------------------------------------

def main():
    """
    This example focuses specifically on the 'represent' operator to show
    how it transforms a RelationSpace into a SliceRelation.
    """
    print("� MRA 'represent' Operator Example 🚀")
    print("="*50 + "\n")

    # 1. Create Sample Data and a RelationSpace
    # This is our starting point: a collection of flat tables (relations)
    # managed within a RelationSpace.
    data = {
        'Device': ['Pixel', 'Pixel', 'iPhone', 'iPhone', 'Surface'],
        'Date': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-01', '2025-01-02', '2025-01-01']),
        'Cost': [100, 200, 150, 50, 1200],
        'Clicks': [20, 5, 30, 10, 60]
    }
    X = pd.DataFrame(data)

    # Create the relations we'll put in the space
    daily_metrics = X.groupby(['Device', 'Date']).agg(
        Cost=('Cost', 'sum'),
        Clicks=('Clicks', 'sum')
    ).reset_index()
    daily_metrics['Cpc'] = daily_metrics['Cost'] / daily_metrics['Clicks']

    total_metrics = X.groupby('Device').agg(TotalCost=('Cost', 'sum')).reset_index()

    # Create the RelationSpace and add the relations
    rs = RelationSpace(dimensions=list(X.columns))
    rs.add_relation(total_metrics, ['Device'])
    rs.add_relation(daily_metrics, ['Device', 'Date'])

    print("--- 1. Initial RelationSpace ---")
    print("This space contains two flat relations: total cost per device, and daily metrics per device.")
    print(rs)
    print("="*50 + "\n")


    # 2. Apply the 'represent' Operator
    # We want to restructure the data to be centered around the 'Device' entity.
    # Each 'Device' will become a 'region', and for each region, we will have
    # two 'feature tables': one for its daily CPC and one for its total cost.
    print("--- 2. Applying 'represent' ---")
    print("Goal: Restructure the data around the 'Device' entity.")
    print("Region Schema: ['Device']")
    print("Feature Schemas: [['Date', 'Cpc']], [['TotalCost']]\n")

    slice_relation = represent(
        relation_space=rs,
        region_schemas=[['Device']],
        feature_schemas=[['Date', 'Cpc'], ['TotalCost']]
    )

    # 3. Print the Resulting SliceRelation
    # The output is a nested structure. Notice how the data from the two
    # original flat tables is now organized under each specific device.
    print("--- 3. Resulting SliceRelation ---")
    print("The data is now nested. Each 'Slice Tuple' corresponds to a Device.")
    print(slice_relation)
    print("="*50 + "\n")
    print("✅ 'represent' Example Finished ✅")


if __name__ == '__main__':
    main()


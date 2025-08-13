"""An example analysis using MRA."""
import pandas as pd
from typing import Dict, List, Callable

# Import the MRA components from the algebra module
from mra_algebra import RelationSpace, crawl

# -----------------------------------------------------------------------------
# Example Usage: Ad Performance Analysis (from the paper)
# -----------------------------------------------------------------------------

def main():
    """Main function to run the MRA prototype example."""
    print("🚀 Starting MRA Prototype Example 🚀")
    print("="*50 + "\n")

    # 1. Create Sample Data
    # A base table `X` with schema [Device, Browser, Date, Cost, Clicks]
    data = {
        'Device': ['Pixel', 'Pixel', 'iPhone', 'iPhone', 'Pixel', 'Surface', 'Surface'],
        'Browser': ['Chrome', 'Chrome', 'Safari', 'Safari', 'Edge', 'Edge', 'Edge'],
        'Date': pd.to_datetime(['2025-01-01', '2025-01-02', '2025-01-01', '2025-01-02', '2025-01-01', '2025-01-01', '2025-01-02']),
        'Cost': [100, 200, 150, 50, 80, 1200, 300],
        'Clicks': [20, 5, 30, 10, 10, 60, 20]
    }
    X = pd.DataFrame(data)
    X['Cpc'] = X['Cost'] / X['Clicks']
    print("--- 1. Base Input Table (X) ---")
    print(X.to_string(index=False))
    print("\n" + "="*50 + "\n")


    # 2. Create RelationSpace (Analogous to GROUP BY CUBE)
    # We need aggregations at different granularities.
    
    # We manually create the aggregated relations for this example.
    # A more robust implementation would use the `create_relation_space` operator.
    daily_cpc_table = X.groupby(['Device', 'Date']).agg(
        Cost=('Cost', 'sum'),
        Clicks=('Clicks', 'sum')
    ).reset_index()
    daily_cpc_table['Cpc'] = daily_cpc_table['Cost'] / daily_cpc_table['Clicks']

    total_cost_per_device = X.groupby('Device').agg(TotalCost=('Cost', 'sum')).reset_index()

    # Create the RelationSpace and add our relations
    rs = RelationSpace(dimensions=list(X.columns))
    rs.add_relation(total_cost_per_device, ['Device'])
    rs.add_relation(daily_cpc_table, ['Device', 'Date'])

    print("--- 2. Initial RelationSpace (ψ) ---")
    print(rs)
    print("="*50 + "\n")


    # 3. Use the Crawl operator to perform the analysis
    # Task: Find devices with cost-per-click timeseries anomalies and total cost > 1000.

    # Define a simple "anomaly detection" function for the transformation
    def causal_impact_analysis(df: pd.DataFrame) -> pd.DataFrame:
        """A mock anomaly detection function. It flags days where CPC > 10."""
        df_copy = df.copy()
        df_copy['IsAnomaly'] = df_copy['Cpc'] > 10
        return df_copy

    # Define the predicate for the selection step
    def total_cost_predicate(features: Dict) -> bool:
        """Predicate to check if TotalCost > 1000."""
        total_cost_schema = tuple(sorted(['TotalCost']))
        if total_cost_schema in features:
            cost = features[total_cost_schema]['TotalCost'].iloc[0]
            return cost > 1000
        return False

    print("--- 3. Executing Crawl Operator ---")
    print("Goal: Find devices with CPC anomalies and TotalCost > 1000.")
    print("This composes Represent -> SliceTransform -> SliceSelect -> Flatten.\n")

    final_relation_space = crawl(
        relation_space=rs,
        region_schemas=[['Device']],
        feature_schemas=[['Date', 'Cpc'], ['TotalCost']],
        slice_transformations=[
            (['Date', 'Cpc'], causal_impact_analysis, 'AnomalyResult')
        ],
        predicates=[total_cost_predicate]
    )

    print("--- 4. Final Resulting RelationSpace (ψ') ---")
    print(final_relation_space)
    print("="*50 + "\n")
    print("✅ MRA Prototype Example Finished ✅")


if __name__ == '__main__':
    main()

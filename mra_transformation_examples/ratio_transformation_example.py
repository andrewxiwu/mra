import pandas as pd

# We assume mra_data.py and slice_transformations.py are accessible
# in the same directory or in the Python path.
from mra_data import RelationSchema
from slice_transformations.ratio_transformation import RatioTransformation

def run_ratio_example():
    """
    Demonstrates how to use the RatioTransformation class.
    """
    # 1. Create an instance of the RatioTransformation.
    #    This is equivalent to creating a 'CostPerClick' function.
    cost_per_click_transformer = RatioTransformation(
        numerator_col='cost',
        denominator_col='clicks',
        output_col='cost_per_click'
    )

    # 2. We can inspect the required schema of the transformation.
    print(f"Transformation requires schema: {cost_per_click_transformer.feature_schema.attributes}")
    print("-" * 40)

    # 3. Create a sample DataFrame that matches the required schema.
    sample_df = pd.DataFrame({
        'cost': [10, 8, 25, 22, 40, 5],
        'clicks': [100, 50, 200, 150, 300, 0], # Includes a zero for division test
        'other_col': ['A', 'B', 'C', 'D', 'E', 'F']
    })
    print("Original DataFrame:")
    print(sample_df)
    print("-" * 40)

    # 4. Apply the transformation to the DataFrame.
    #    This is done by calling the instance directly.
    transformed_df = cost_per_click_transformer(sample_df)

    # 5. Print the result.
    print("Transformed DataFrame:")
    print(transformed_df)


if __name__ == '__main__':
    run_ratio_example()

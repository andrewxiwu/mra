Multi-Relational Algebra (MRA) Framework
This project provides a Python implementation of the concepts described in the paper "Multi-Relational Algebra for Multi-Granular Data Analytics." It introduces a set of data structures and composable operators that allow for complex, multi-granular data analysis in a clean and declarative way.

Core Concepts
The framework is built on two key data abstractions and a series of operators that transform them.

Data Structures
RelationSpace: A collection of relations (pandas DataFrames) indexed by their dimensional schemas. This is the primary structure for holding data at different levels of granularity.

SliceRelation: A structure that organizes data around entities (called "regions"), where each region is associated with one or more relation-valued features.

Operators
The framework uses a pipeline-based approach where operators can be chained together using the | symbol.

CreateRelationSpaceByCube: Creates a RelationSpace by performing a GROUP BY CUBE operation on a DataFrame.

Represent: Transforms a RelationSpace into a SliceRelation.

SliceTransform: Applies a series of transformations to the feature tables within a SliceRelation.

SliceSelect: Filters the slice tuples in a SliceRelation based on a predicate.

Flatten: Converts a SliceRelation back into a RelationSpace.

Crawl: A "mega-operator" that composes Represent, SliceTransform, SliceSelect, and Flatten into a single, powerful operation.

Project Structure
The project is organized into a main package and two sub-packages for examples.

.
├── mra_data.py                 # Core data structures (RelationSpace, SliceRelation)
├── mra_operators.py            # All MRA operators (Create..., Represent, Crawl, etc.)
├── slice_transformation.py     # Base class for slice transformations
├── ratio_transformation.py     # Example of a concrete transformation
├── __init__.py                 # Makes the root directory a package
│
├── mra_data_examples/
│   ├── __init__.py
│   └── ratio_transformation_example.py
│
└── mra_pipeline_examples/
    ├── __init__.py
    ├── represent_example.py
    ├── slice_select_example.py
    ├── slice_transform_example.py
    ├── flatten_example.py
    └── crawl_example.py

How to Run Examples
To run any of the example scripts, you must execute them as modules from the root directory of the project. This ensures that all internal imports (e.g., from mra_data import ...) work correctly.

Do not cd into the example directories to run the files.

Example Command
From the root mra directory, run the following command:

# To run the main crawl_example.py
python3 -m mra_pipeline_examples.crawl_example

Similarly, to run other examples:

python3 -m mra_pipeline_examples.flatten_example
python3 -m mra_data_examples.ratio_transformation_example

Basic Usage
The following is a brief example of how to build and execute a pipeline.

import pandas as pd
from mra_data import RelationSchema
from mra_operators import CreateRelationSpaceByCube, Crawl
from ratio_transformation import RatioTransformation

# 1. Your initial data
df = pd.DataFrame(...)

# 2. Define the components for your analysis
cost_per_click = RatioTransformation(
    numerator_col='cost',
    denominator_col='clicks',
    output_col='cost_per_click'
)

def my_predicate(region, features):
    # ... logic to filter slices ...
    return True

# 3. Build the pipeline using the | operator
pipeline = (
    CreateRelationSpaceByCube(
        grouping_keys=['device', 'browser'],
        aggregations={'clicks': 'sum', 'cost': 'sum'}
    ) |
    Crawl(
        region_schemas=[RelationSchema(['device'])],
        slice_transformations=[cost_per_click],
        predicate_func=my_predicate,
        dimensions=RelationSchema(['device', 'browser'])
    )
)

# 4. Execute the pipeline
final_result = pipeline(df)

print(final_result)

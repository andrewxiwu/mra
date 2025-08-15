# Python Prototype for Multi-Relational Algebra (MRA)

This project provides a Python prototype of **Multi-Relational Algebra (MRA)**, an extension of classical relational algebra designed for complex, multi-granular data analytics. The implementation is based on the concepts presented in the academic paper, "Multi-Relational Algebra for Multi-Granular Data Analytics."

The core idea is to move beyond traditional flat-table analysis by introducing data structures and operators that can handle collections of relations and nested, per-entity features in a structured and composable way.

---

## Project Structure

The project is organized into a set of core modules and corresponding example directories:

### Core Modules

1.  **`mra_data.py`**: Contains the core data structures of the MRA prototype. It defines `RelationSpace`, `SliceRelation`, and the underlying types like `RelationSchema` and `RelationTuple`.

2.  **`mra_operators.py`**: Contains the logic for all the MRA operators (`represent`, `flatten`, etc.). It is designed to support a chainable, pipe-based (`|`) syntax.

3.  **`slice_transformation.py`**: A dedicated module for defining specific, reusable transformation functions that can be applied to feature tables within a `SliceRelation`.

### Example Directories

-   **`mra_data_examples/`**: Contains scripts that provide practical demonstrations of how to create and use the core data structures from `mra_data.py`.
-   **`mra_transformation_examples/`**: Contains scripts that showcase how to use the functions defined in `slice_transformation.py`.
-   **`mra_pipeline_examples/`**: Contains scripts demonstrating end-to-end analytical workflows, chaining multiple operators and transformations together to solve a problem.

---

## Getting Started

### Prerequisites

To run this project, you will need:

-   **Python 3**: The code is written for Python 3.
-   **pandas**: This is the only external library required.

### Installation

1.  **Clone or download the project files.** Ensure you have the core modules and the example directories.

2.  **Set up your project directory.** All the `mra_*` modules and directories should be at the same level in your project. Your project's root directory should be in the Python path.

3.  **Install pandas.** If you do not have pandas installed:
    ```bash
    # Using pip
    pip install pandas

    # Or, if you use Anaconda/Miniconda
    conda install pandas
    ```

---

## How to Run the Examples

Because the project is structured as a collection of modules, you must run the examples from your project's **root directory** using the `python3 -m` flag. This ensures that Python can correctly handle the imports between the different files.

1.  Open your terminal and navigate to the root directory of the MRA project.
2.  Execute the desired example script using the following format:

    ```bash
    # To run an example from the data examples directory
    python3 -m mra_data_examples.relation_space_example

    # To run an example from the pipeline examples directory
    python3 -m mra_pipeline_examples.some_pipeline_example
    ```

    *Note: Replace `some_pipeline_example` with the actual name of the file you wish to run (without the `.py` extension).*

# Python Prototype for Multi-Relational Algebra (MRA)

This project provides a Python prototype of **Multi-Relational Algebra (MRA)**, an extension of classical relational algebra designed for complex, multi-granular data analytics. The implementation is based on the concepts presented in the academic paper, "Multi-Relational Algebra for Multi-Granular Data Analytics."

The core idea is to move beyond traditional flat-table analysis by introducing data structures and operators that can handle collections of relations and nested, per-entity features in a structured and composable way.

---

## Core Concepts

MRA introduces two new data abstractions to enable its powerful analytical capabilities:

-   **`RelationSpace`**: A container for managing a collection of relations (represented here as pandas DataFrames) defined at different granularities. It uses a "dimensional schema" to uniquely identify each relation, avoiding the need to manage numerous individual tables manually.

-   **`SliceRelation`**: A nested data structure that organizes data around specific entities (called "regions"). Each row in a `SliceRelation` is a "slice tuple" containing a region and its associated set of relation-valued features. This structure is ideal for performing complex, per-entity analyses like time-series anomaly detection or statistical modeling.

---

## Project Structure

The project is organized into two core modules and several example scripts:

1.  **`mra_data.py`**: This file contains the core data structures of the MRA prototype. It defines the `RelationSpace` and `SliceRelation` classes, as well as the underlying types like `RelationSchema` and `RelationTuple`.

2.  **`mra_operators.py`**: This file contains the logic for all the MRA operators (`represent`, `flatten`, `slice_transform`, `slice_select`, etc.). It is designed to support a chainable, pipe-based (`|`) syntax for creating readable analytical workflows.

3.  **Example Scripts**: Files like `ad_performance_analysis_example.py` and `relation_space_example.py` provide practical demonstrations of how to use the data structures and operators to perform analysis.

---

## Getting Started

### Prerequisites

To run this project, you will need:

-   **Python 3**: The code is written for Python 3. On many systems, the command to run it is `python3`.
-   **pandas**: This is the only external library required. It is used for all data manipulation.

### Installation

1.  **Clone or download the project files.** Make sure you have `mra_data.py`, `mra_operators.py`, and the example scripts you wish to run.

2.  **Place all files in the same directory.** This is necessary because the example scripts and operator module import the data module directly.

3.  **Install pandas.** If you do not have pandas installed, you can install it using `pip` or `conda`:

    ```bash
    # Using pip
    pip install pandas

    # Or, if you use Anaconda/Miniconda
    conda install pandas
    ```

---

## How to Run the Examples

Once you have set up the project and installed the dependency, you can run the examples from your terminal.

1.  Navigate to the directory where you saved the files.
2.  Execute the desired example script:

    ```bash
    # To run the full, end-to-end analysis example
    python3 ad_performance_analysis_example.py

    # To run the focused example on the RelationSpace
    python3 relation_space_example.py
    ```

    *Note: If `python3` doesn't work, your system may use `python` as the command for Python 3.*

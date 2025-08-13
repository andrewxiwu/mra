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

The project is organized into a core algebra module and several example scripts:

1.  **`mra_algebra.py`**: This file contains the core logic of the MRA prototype. It defines the `RelationSpace` and `SliceRelation` classes and implements all the fundamental MRA operators (`represent`, `flatten`, `slice_transform`, `slice_select`, `crawl`, etc.). This module is self-contained and is imported by the example scripts.

2.  **`ad_performance_analysis_example.py`**: This script provides a practical, end-to-end demonstration of the MRA algebra. It uses the high-level `crawl` operator to analyze a sample ad performance dataset.

3.  **`represent_operator_example.py`**: This script provides a focused demonstration of the fundamental `represent` operator, showing how it transforms a `RelationSpace` into an entity-centric `SliceRelation`.

---

## Getting Started

### Prerequisites

To run this project, you will need:

-   **Python 3**: The code is written for Python 3. On many systems, the command to run it is `python3`.
-   **pandas**: This is the only external library required. It is used for all data manipulation.

### Installation

1.  **Clone or download the project files.** Make sure you have `mra_algebra.py` and the example scripts you wish to run.

2.  **Place all files in the same directory.** This is necessary because the example scripts import the algebra module directly.

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

    # To run the focused example on the 'represent' operator
    python3 represent_operator_example.py
    ```

    *Note: If `python3` doesn't work, your system may use `python` as the command for Python 3.*

### What the Examples Demonstrate

-   **`ad_performance_analysis_example.py`**: This script walks through a common analytics task using the high-level `crawl` operator. It finds devices that meet two criteria: a total cost greater than 1000 and an anomalous daily Cost-Per-Click (CPC). The final output is a new `RelationSpace` containing only the filtered, relevant data.

-   **`represent_operator_example.py`**: This script isolates the `represent` operator to clearly show its core function: transforming a collection of flat tables (`RelationSpace`) into a nested, entity-centric structure (`SliceRelation`). It's a great starting point for understanding the fundamental data restructuring at the heart of MRA.

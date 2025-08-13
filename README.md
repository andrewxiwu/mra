# # Python Prototype for Multi-Relational Algebra (MRA)

This project provides a Python prototype of **Multi-Relational Algebra (MRA)**, an extension of classical relational algebra designed for complex, multi-granular data analytics. The implementation is based on the concepts presented in the academic paper, "Multi-Relational Algebra for Multi-Granular Data Analytics."

The core idea is to move beyond traditional flat-table analysis by introducing data structures and operators that can handle collections of relations and nested, per-entity features in a structured and composable way.

---

## Core Concepts

MRA introduces two new data abstractions to enable its powerful analytical capabilities:

-   **`RelationSpace`**: A container for managing a collection of relations (represented here as pandas DataFrames) defined at different granularities. It uses a "dimensional schema" to uniquely identify each relation, avoiding the need to manage numerous individual tables manually.

-   **`SliceRelation`**: A nested data structure that organizes data around specific entities (called "regions"). Each row in a `SliceRelation` is a "slice tuple" containing a region and its associated set of relation-valued features. This structure is ideal for performing complex, per-entity analyses like time-series anomaly detection or statistical modeling.

---

## Project Structure

The project is organized into two distinct Python modules:

1.  **`mra_algebra.py`**: This file contains the core logic of the MRA prototype. It defines the `RelationSpace` and `SliceRelation` classes and implements all the fundamental MRA operators (`represent`, `flatten`, `slice_transform`, `slice_select`, `crawl`, etc.). This module is self-contained and can be imported into any project.

2.  **`ad_performance_analysis_example.py`**: This file provides a practical demonstration of the MRA algebra. It uses the components from `mra_algebra.py` to analyze a sample ad performance dataset, identifying devices that meet specific criteria (e.g., high cost and anomalous behavior).

---

## Getting Started

### Prerequisites

To run this project, you will need:

-   **Python 3**: The code is written for Python 3. On many systems, the command to run it is `python3`.
-   **pandas**: This is the only external library required. It is used for all data manipulation.

### Installation

1.  **Clone or download the project files.** Make sure you have both `mra_algebra.py` and `ad_performance_analysis_example.py`.

2.  **Place both files in the same directory.** This is necessary because the example script imports the algebra module directly.

3.  **Install pandas.** If you do not have pandas installed, you can install it using `pip` or `conda`:

    ```bash
    # Using pip
    pip install pandas

    # Or, if you use Anaconda/Miniconda
    conda install pandas
    ```

---

## How to Run

Once you have set up the project and installed the dependency, you can run the example from your terminal.

1.  Navigate to the directory where you saved the files.
2.  Execute the example script:

    ```bash
    python3 ad_performance_analysis_example.py
    ```

    *Note: If `python3` doesn't work, your system may use `python` as the command for Python 3.*

## What the Example Does

The `ad_performance_analysis_example.py` script walks through a common analytics task:

1.  It starts with a base table of ad performance data (cost, clicks, etc.).
2.  It creates an initial `RelationSpace` containing aggregated data at different granularities (e.g., total cost per device, daily CPC per device).
3.  It uses the powerful `crawl` operator to find devices that meet two criteria:
    -   The total cost for the device is greater than 1000.
    -   The device's daily Cost-Per-Click (CPC) time series shows anomalous behavior.
4.  The final output is a new `RelationSpace` containing only the filtered, relevant data, demonstrating how MRA can systematically narrow down a large dataset to the most interesting insights.


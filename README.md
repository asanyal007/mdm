# mdm

# Master Data Management Codebase

This repository contains Python code that performs Master Data Management (MDM) tasks on a given dataset. The codebase focuses on cleaning, preprocessing, and resolving duplicate records within the dataset. It utilizes various techniques including text preprocessing, similarity calculations, and clustering to identify and manage duplicate records.

## Getting Started

To get started with this codebase, follow the steps below:

1. Clone the repository to your local machine:

   ```
   git clone <repository_url>
   ```

2. Install the required dependencies. It is recommended to use a virtual environment:

   ```
   pip install -r requirements.txt
   ```

3. Modify the configuration file (`config/config.json`) to provide necessary database connection details and other configurations.

## Code Overview

The codebase consists of several Python scripts and utility functions. Below is an overview of the main components:

- `data_source.database.postgres_db`: Module to connect and interact with the PostgreSQL database.
- `util`: Utility functions for loading configuration files and generating unique identifiers.
- `manager.string_preprocess`: Functions for tokenizing and correcting strings.
- `manager.text_mdm_ratio`: Functions related to text similarity calculations.
- `jaro`: Module containing the Jaro-Winkler similarity metric implementation.

### Main Functions

- `bad_cluster_selection(cluster_df)`: Identifies clusters with potentially bad data and returns a boolean indicating whether selection should be performed.
- `jaro_similarity_ratio(str_1, str_2)`: Calculates Jaro-Winkler similarity ratio between two strings.
- `avg_scores(a, b, c, d)`: Calculates the average of four input scores.
- `get_city(zip_code)`: Retrieves the city name based on the provided ZIP code.
- `line_correction(str_1, str_2)`: Performs correction on two strings by removing elements present in `str_2` from `str_1`.
- `df_text_transformation(df)`: Performs text preprocessing on DataFrame columns and returns a sorted DataFrame based on raw data length.
- `mdm_transformation(raw_df, config)`: Performs master data management transformation on raw DataFrame.
- `df_text_ranking(df, default=False, gen_uid=True)`: Ranks the DataFrame based on scores and assigns unique identifiers.
- `auto_resolve(df, cut_of_scores)`: Automatically resolves records with scores above a specified threshold.
- `main(presc_id)`: The main function that orchestrates the MDM process for a given `presc_id`.
- `df_to_gdb(mdm_df)`: Loads MDM data into a Graph Database (Note: The graph database part is currently commented out).
- `graph_relation_map()`: Defines and executes Cypher queries to establish relationships in the graph database.
- `get_presc_id()`: Retrieves a list of unique `ult_parent_num` (prescription IDs) from the dataset.
- `log_result(result)`: Logs the results of MDM processing.
- `ranking_gen()`: Generates rankings for records and performs MDM for each prescription ID.

## Usage

1. Ensure that the configuration file (`config/config.json`) is correctly set up with the required database connection details and configurations.

2. Run the `ranking_gen()` function to initiate the MDM process:

   ```python
   ranking_gen()
   ```

3. The MDM process will clean and preprocess the data, perform duplicate record resolution, and store the results in the specified PostgreSQL tables.

## Notes

- The code contains commented sections related to graph database operations using the `neo4j` library. If you intend to use a graph database, you can uncomment and adapt these sections as needed.

- Be sure to thoroughly test the code with your dataset and configurations before using it in a production environment.

## Contributors

- [Your Name]
- [Your Email]

Feel free to contribute to this codebase by submitting pull requests or reporting issues.

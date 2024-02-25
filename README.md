# SON Algorithm Implementation with Spark

## Overview
This assignment contains the implementation of the SON Algorithm using the Spark Framework, aimed at finding frequent itemsets within large datasets. The project is structured around two main tasks: analyzing a simulated dataset and a real-world dataset, specifically the Ta Feng dataset, to uncover patterns of frequent itemsets. It showcases the application of distributed computing techniques to efficiently process and analyze data at scale in a Spark environment. The code strictly adheres to Python's standard libraries, emphasizing the core functionality of Spark RDDs to understand and leverage Spark operations for big data analysis.

## Datasets
There are one simulated dataset and one real-world dataset, starting with task1 on the simulated CSV file provided, then moving to task2 with the Ta Feng dataset to simulate similar data structures.

## Task 1: Simulated Data
Analyze two CSV files (small1.csv and small2.csv) for frequent itemsets, focusing on businesses and users as singletons, pairs, triples, etc., based on a given support threshold.

## Task 2: Ta Feng Data
Utilize the Ta Feng dataset to explore frequent itemsets, focusing on product IDs associated with customer IDs, aggregating daily purchases into single transactions.
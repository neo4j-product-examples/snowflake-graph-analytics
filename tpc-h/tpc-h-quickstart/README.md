# TPC-H Quickstart

This example shows how to use Neo4j Graph Analytics within Snowflake to calculate similarity between parts based on their orders.

We will use the node_similarity algorithm to calculate the similarity.

## Dataset
The dataset is available on a Neo4j S3 bucket.

## Prerequisites

### 1) Get Source Data

The notebook uses Snowflake Stages to copy data from a Neo4j S3 bucket.

Stages in snowflake are places that you can land your data before it is uploaded to a Snowflake table. You might have a batch of CSV files living on a disk driver somewhere, and, in order to start querying the data via a table, the data must be landed within the Snowflake environment for a data upload to be possible.

In the exercise, we will be working with structured, comma-delimited data that has already been staged in a public, external AWS bucket. Before we can use this data, we first need to create a Stage that specifies the location of our external bucket.

## Running the Example

### Steps
The `TPC-H.ipynb` notebook has the full example.
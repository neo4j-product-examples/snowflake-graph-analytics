# IMDB

This dataset originates from https://github.com/seongjunyun/Graph_Transformer_Networks which in turn obtained the dataset from earlier sources.
The dataset has been processed to be compatible with the Snowflake application `Neo4j Graph Analytics`.

The graph contains node labels 'ACTOR', 'MOVIE' and 'DIRECTOR' and relationship types 'ACTED_IN' and 'DIRECTED_IN'.
All nodes contain 1256 dimensional feature vectors via the property "plot_keywords". Some of the MOVIE nodes have a 'GENRE' value of 0, 1 or 2.

A method is provided in `imdb/load_to_pandas.py` to load the dataset into dataframes describing nodes and relationships for the IMDB graph.

## Ingesting the IMDB dataset into Snowflake

A script `imdb/upload_imdb_to_snowflake.py` uploads the data into a snowflake account.
For usage of this script run `python upload_imdb_to_snowflake.py` from the imdb folder.
The script takes command line arguments about snowflake configuration. 
The snowflake host depends on region but can be for example, `snowflake.eu-west-2.aws.snowflakecomputing.com`.

## Running GraphSAGE on IMDB in Snowflake

The file `imdb/graph_sage.sql` is provided that you can run for example from inside the SnowsightUI after installing the Snowflake application `Neo4j Graph Analytics` inside your Snowflake account.
This sql file is a template in which you need to replace the placeholders.

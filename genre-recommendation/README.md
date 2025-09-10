# Genre Recommendation with IMDB Data

## Overview

Duration: 2

### What Is Neo4j Graph Analytics For Snowflake? 

Neo4j helps organizations find hidden relationships and patterns across billions of data connections deeply, easily, and quickly. **Neo4j Graph Analytics for Snowflake** brings to the power of graph directly to Snowflake, allowing users to run 65+ ready-to-use algorithms on their data, all without leaving Snowflake! 

### Genre Recommendation with IMDB Data

The IMDB dataset contains a rich graph of movies, actors, and directors with their relationships. This dataset originates from [Graph Transformer Networks](https://github.com/seongjunyun/Graph_Transformer_Networks) and has been processed to be compatible with the Snowflake application `Neo4j Graph Analytics`.

Using Graph Analytics for Snowflake, we can build powerful recommendation systems that leverage the complex relationships between movies, actors, and directors. This approach can be applied to various recommendation scenarios including content discovery, collaborative filtering, and personalized suggestions.

### Prerequisites

- The Native App [Neo4j Graph Analytics](https://app.snowflake.com/marketplace/listing/GZTDZH40CN) for Snowflake

### What You Will Need

- A [Snowflake account](https://signup.snowflake.com/?utm_cta=quickstarts) with appropriate access to databases and schemas.
- Neo4j Graph Analytics application installed from the Snowflake marketplace. Access the marketplace via the menu bar on the left hand side of your screen.

### What You Will Build

- A notebook that performs genre recommendation using GraphSAGE on IMDB data
- Graph embeddings for movie recommendation systems

### What You Will Learn

- How to prepare and project your data for graph analytics
- How to use GraphSAGE for node classification and embedding generation
- How to read and write directly from and to your snowflake tables
- How to build recommendation systems using graph neural networks

## Dataset Overview

Duration: 5

The IMDB dataset is a comprehensive graph containing:

- **Node Labels**: 'ACTOR', 'MOVIE', and 'DIRECTOR'
- **Relationship Types**: 'ACTED_IN' and 'DIRECTED_IN'
- **Features**: All nodes contain 1256-dimensional feature vectors via the "plot_keywords" property
- **Labels**: Some MOVIE nodes have a 'GENRE' value of 0, 1, or 2 for classification tasks

This dataset provides a rich foundation for understanding movie relationships and building recommendation systems.

## Loading The Data

Duration: 5

Let's name our database `IMDB`. We'll load the IMDB dataset into Snowflake using the provided scripts.

A script `genre-recommendation/upload_imdb_to_snowflake.py` uploads the data into a Snowflake account.

**Usage:**
```bash
python ./upload_imdb_to_snowflake.py --snowflake_connection=my_connection_name
                                     --snowflake_database=genre_classification_db
                                     --snowflake_schema=imdb
                                     --snowflake_role=accountadmin
```

`snowflake_connection` is a connection you have set up in Snowflake. See [Snowflake documentation](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session#connect-by-using-the-connections-toml-file). 


## Setting Up

### Import The Notebook

- We've provided a notebook to walk you through each SQL and Python step
- The notebook demonstrates GraphSAGE classification and embedding generation
- Don't forget to install required Python packages before you run

### Permissions

Before we run our algorithms, we need to set the proper permissions. Ensure you are using `accountadmin` to grant and create roles:

```sql
USE ROLE accountadmin;
```

Next, let's set up the necessary roles, permissions, and resource access to enable Graph Analytics to operate on data within the `imdb.public` schema:

```sql
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE NEO4J_GRAPH_ANALYTICS.app_user TO ROLE gds_role;
GRANT APPLICATION ROLE NEO4J_GRAPH_ANALYTICS.app_admin TO ROLE gds_role;
SET MY_USER = (SELECT CURRENT_USER());
GRANT ROLE gds_role TO USER IDENTIFIER($MY_USER);

-- Create database role for application privileges
USE DATABASE genre_classification_db;
CREATE DATABASE ROLE IF NOT EXISTS graph_analytics_db_role;

GRANT USAGE ON DATABASE genre_classification_db TO DATABASE ROLE graph_analytics_db_role;
GRANT USAGE ON SCHEMA genre_classification_db.imdb TO DATABASE ROLE graph_analytics_db_role;
GRANT USAGE ON SCHEMA genre_classification_db.results TO DATABASE ROLE graph_analytics_db_role;

-- Grant privileges to database role (following Neo4j Graph Analytics documentation pattern)
GRANT SELECT ON ALL TABLES IN SCHEMA genre_classification_db.results TO DATABASE ROLE graph_analytics_db_role;
GRANT SELECT ON ALL TABLES IN SCHEMA genre_classification_db.imdb TO DATABASE ROLE graph_analytics_db_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA genre_classification_db.imdb TO DATABASE ROLE graph_analytics_db_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA genre_classification_db.results TO DATABASE ROLE graph_analytics_db_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA genre_classification_db.imdb TO DATABASE ROLE graph_analytics_db_role;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA genre_classification_db.imdb TO DATABASE ROLE graph_analytics_db_role;
GRANT CREATE TABLE ON SCHEMA genre_classification_db.results TO DATABASE ROLE graph_analytics_db_role;
GRANT CREATE TABLE ON SCHEMA genre_classification_db.imdb TO DATABASE ROLE graph_analytics_db_role;

-- Grant database role to application
GRANT DATABASE ROLE graph_analytics_db_role TO APPLICATION NEO4J_GRAPH_ANALYTICS;

-- Grant consumer role access to tables created by the application
GRANT USAGE ON DATABASE genre_classification_db TO ROLE gds_role;
GRANT USAGE ON SCHEMA genre_classification_db.results TO ROLE gds_role;
GRANT SELECT ON FUTURE TABLES IN SCHEMA genre_classification_db.results TO ROLE gds_role;
```

Then switch to the role we created:

```sql
USE ROLE gds_role;
```

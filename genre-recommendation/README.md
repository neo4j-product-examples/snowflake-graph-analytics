# Genre Recommendation with IMDB Data

## Overview

Duration: 2

### What Is Neo4j Graph Analytics For Snowflake? 

Neo4j helps organizations find hidden relationships and patterns across billions of data connections deeply, easily, and quickly. **Neo4j Graph Analytics for Snowflake** brings the power of graph directly to Snowflake, allowing users to run 65+ ready-to-use algorithms on their data, all without leaving Snowflake! 

### Genre Recommendation with IMDB Data

The IMDB dataset contains a rich graph of movies, actors, and directors with their relationships. This dataset originates from [Graph Transformer Networks](https://github.com/seongjunyun/Graph_Transformer_Networks) and has been processed to be compatible with the Snowflake application `Neo4j Graph Analytics`.

Using Graph Analytics for Snowflake, we can build powerful recommendation systems that leverage the complex relationships between movies, actors, and directors. This approach can be applied to various recommendation scenarios including content discovery, collaborative filtering, and personalized suggestions.

### Prerequisites

- The Native App [Neo4j Graph Analytics](https://app.snowflake.com/marketplace/listing/GZTDZH40CN) for Snowflake

### What You Will Need

- A [Snowflake account](https://signup.snowflake.com/?utm_cta=quickstarts) with appropriate access to databases and schemas.
- Neo4j Graph Analytics application installed from the Snowflake marketplace. Access the marketplace via the menu bar on the left hand side of your screen.

### What You Will Build

- Graph embeddings for movie recommendation systems
- A notebook that performs genre recommendation using GraphSAGE on IMDB data

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
uv sync

# Set --snowflake_account option in form of "ABCDEFG-MY_ACCOUNT"
uv run ./upload_imdb_to_snowflake.py --snowflake_account SNOWFLAKE_ACCOUNT \
                                     --snowflake_user SNOWFLAKE_USER \
                                     --snowflake_password SNOWFLAKE_PASSWORD \
                                     --snowflake_role SNOWFLAKE_ROLE \
                                     --snowflake_warehouse SNOWFLAKE_WAREHOUSE \
                                     --snowflake_database SNOWFLAKE_DATABASE \
                                     --snowflake_schema SNOWFLAKE_SCHEMA
```
or if you have a connection set up in Snowflake. See [Snowflake documentation](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session#connect-by-using-the-connections-toml-file).:
```bash
uv run ./upload_imdb_to_snowflake.py --connection_name=my_connection
```



## Setting Up

### Import The Notebook

- We've provided a notebook to walk you through each SQL and Python step
- The notebook demonstrates GraphSAGE classification and embedding generation
- Don't forget to install required Python packages before you run

### Permissions

Before we run our algorithms, we need to set the proper permissions. Ensure you are using `accountadmin` to grant and create roles:

```
USE ROLE accountadmin;
```

Next, let's set up the necessary roles, permissions, and resource access to enable Graph Analytics to operate on data within the `imdb.public` schema. It creates a consumer role (gds_role) for users and administrators, grants the Neo4j Graph Analytics application access to read from and write to tables and views, and ensures that future tables are accessible.

It also provides the application with access to the required compute pool and warehouse resources needed to run graph algorithms at scale.

```
-- Create a consumer role for users and admins of the GDS application
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

```
USE ROLE gds_role;
```


## Generating graph embeddings with GraphSAGE

Duration: 20
GraphSAGE is a powerful algorithm for generating node embeddings by sampling and aggregating features from a node's local neighborhood. This approach allows us to capture the structural and feature information of nodes in a graph, making it ideal for tasks like node classification and recommendation systems.

To get node embeddings using GraphSAGE, users need to train a model and then use it to generate embeddings.

### Data preparation

Table `imdb.movie` contains an extra column `GENRE` which is not used in this example. Let's create a view without this column.

```
CREATE VIEW IF NOT EXISTS genre_classification_db.imdb.movie_plot AS
SELECT nodeid, plot_keywords
FROM genre_classification_db.imdb.movie;

GRANT SELECT ON ALL VIEWS IN SCHEMA genre_classification_db.imdb TO ROLE gds_role;
```

### Model training

Training a GraphSAGE model can be done with the following command:

```
-- Training stage of the GraphSAGE unsupervised algorithm
CALL NEO4J_GRAPH_ANALYTICS.graph.gs_unsup_train('GPU_NV_S', {
    'project': {
        'defaultTablePrefix': 'genre_classification_db.imdb',
        'nodeTables': ['actor', 'director', 'movie_plot'],
        'relationshipTables': {
            'acted_in': {'sourceTable': 'actor', 'targetTable': 'movie_plot', 'orientation': 'UNDIRECTED'},
            'directed_in': {'sourceTable': 'director', 'targetTable': 'movie_plot', 'orientation': 'UNDIRECTED'}
        }
    },
    'compute': {
        'modelname': 'unsup-imdb',
        'hiddenChannels': 32,
        'numEpochs': 10,
        'numSamples': [20, 20]
    }
});
```

The model will be saved into the internal stage of the Neo4j Graph Analytics application. This model can be referred to by its name `unsup-imdb` in the next step.

### Generating embeddings

The trained model can be used to generate embeddings for all nodes in the graph. The following command generates embeddings and stores them in a new table `results.movie_embeddings`:

```
-- Prediction stage of the GraphSAGE unsupervised algorithm - computing embeddings
CALL NEO4J_GRAPH_ANALYTICS.graph.gs_unsup_predict('GPU_NV_S', {
    'project': {
        'defaultTablePrefix': 'genre_classification_db.imdb',
        'nodeTables': ['actor', 'director', 'movie_plot'],
        'relationshipTables': {
            'acted_in': {'sourceTable': 'actor', 'targetTable': 'movie_plot', 'orientation': 'UNDIRECTED'},
            'directed_in': {'sourceTable': 'director', 'targetTable': 'movie_plot', 'orientation': 'UNDIRECTED'}
        }
    },
    'compute': {
        'modelname': 'unsup-imdb'
    },
    'write': [{
        'nodeLabel': 'movie_plot',
        'outputTable': 'genre_classification_db.results.movie_embeddings'
    }]
});
```

The resulting table `results.movie_embeddings` will contain the node IDs and their corresponding embeddings.

```
SELECT * FROM genre_classification_db.results.movie_embeddings LIMIT 5;
```

The model can be dropped when it is no longer needed:

```
CALL NEO4J_GRAPH_ANALYTICS.graph.drop_model('unsup-imdb');
```

## Node Classification with GraphSAGE

Duration: 20
GraphSAGE can also be used for node classification tasks. In this example, we will classify movies into genres based on their features and relationships in the graph.

### Model training
Training a GraphSAGE model for node classification can be done with the following command:

```
-- Training stage of the GraphSAGE node classification algorithm
CALL NEO4J_GRAPH_ANALYTICS.graph.gs_nc_train('GPU_NV_S', {
    'project': {
        'defaultTablePrefix': 'genre_classification_db.imdb',
        'nodeTables': ['actor', 'director', 'movie'],
        'relationshipTables': {
            'acted_in': {'sourceTable': 'actor', 'targetTable': 'movie', 'orientation': 'UNDIRECTED'},
            'directed_in': {'sourceTable': 'director', 'targetTable': 'movie', 'orientation': 'UNDIRECTED'}
        }
    },
    'compute': {
        'modelname': 'nc-imdb',
        'trainBatchSize': 512,
        'numEpochs': 10,
        'numSamples': [20, 20],
        'targetLabel': 'movie',
        'targetProperty': 'genre',
        'classWeights': true
    }
});
```

The model will be saved into the internal stage of the Neo4j Graph Analytics application. This model can be referred to by its name `nc-imdb` in the next step.

### Making predictions
The trained model can be used to make predictions on the movie nodes. The following command generates predictions and stores them in a new table `results.genre_predictions`:

```
-- Prediction stage of the GraphSAGE node classification algorithm
CALL NEO4J_GRAPH_ANALYTICS.graph.gs_nc_predict('GPU_NV_S', {
    'project': {
        'defaultTablePrefix': 'genre_classification_db.imdb',
        'nodeTables': ['actor', 'director', 'movie'],
        'relationshipTables': {
            'acted_in': {'sourceTable': 'actor', 'targetTable': 'movie', 'orientation': 'UNDIRECTED'},
            'directed_in': {'sourceTable': 'director', 'targetTable': 'movie', 'orientation': 'UNDIRECTED'}
        }
    },
    'compute': {
        'modelname': 'nc-imdb'
    },
    'write': [{
        'nodeLabel': 'movie',
        'outputTable': 'genre_classification_db.results.genre_predictions'
    }]
});
```

The resulting table `results.genre_predictions` will contain the node IDs, their predicted genres, and the associated probabilities.

```
SELECT * FROM genre_classification_db.results.genre_predictions LIMIT 5;
```

The model can be dropped when it is no longer needed:

```
CALL NEO4J_GRAPH_ANALYTICS.graph.drop_model('nc-imdb');
```

## Conclusions And Resources

Duration: 2

In this quickstart, you learned how to bring the power of graph insights into Snowflake using Neo4j Graph Analytics.

### What You Learned

Using our IMDB dataset, you were able to:

1. Set up the [Neo4j Graph Analytics](https://app.snowflake.com/marketplace/listing/GZTDZH40CN/neo4j-neo4j-graph-analytics) application within Snowflake.
2. Prepare and project your data into a graph model (movies, actors, and directors as nodes with relationships).
3. Generate graph embeddings using GraphSAGE for recommendation systems.
4. Perform node classification to predict movie genres using graph neural networks.

### Resources

- [Neo4j Graph Analytics GraphSAGE Documentation Page](https://neo4j.com/docs/snowflake-graph-analytics/current/algorithms/graphsage/)

USE ROLE ACCOUNTADMIN;

-- Let's assume you have a database named 'genre_classification_db' which contains data in the schema 'imdb'
-- and 'results' for storing the results of algorithm predictions.
-- Application name is Neo4j_GDS
GRANT USAGE ON DATABASE genre_classification_db TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA genre_classification_db.results TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA genre_classification_db.imdb TO APPLICATION Neo4j_GDS;

GRANT CREATE STAGE ON SCHEMA genre_classification_db.results TO APPLICATION Neo4j_GDS;
GRANT CREATE STAGE ON SCHEMA genre_classification_db.imdb TO APPLICATION Neo4j_GDS;

GRANT SELECT ON ALL TABLES IN SCHEMA genre_classification_db.results TO APPLICATION Neo4j_GDS;
GRANT SELECT ON ALL TABLES IN SCHEMA genre_classification_db.imdb TO APPLICATION Neo4j_GDS;

GRANT CREATE TABLE ON SCHEMA genre_classification_db.results TO APPLICATION Neo4j_GDS;
GRANT CREATE TABLE ON SCHEMA genre_classification_db.imdb TO APPLICATION Neo4j_GDS;

GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA genre_classification_db.results TO ROLE accountadmin;

-- `gds_role` is a consumer role for users of the application.
-- CREATE ROLE IF NOT EXISTS gds_role;
-- GRANT APPLICATION ROLE NEO4J_GRAPH_ANALYTICS.app_user TO ROLE gds_role;

GRANT USAGE, MONITOR ON DATABASE genre_classification_db TO role gds_role;
GRANT USAGE, MONITOR ON schema genre_classification_db.imdb TO role gds_role;
GRANT USAGE, MONITOR ON schema genre_classification_db.results TO role gds_role;
GRANT SELECT ON ALL TABLES IN SCHEMA genre_classification_db.results TO role gds_role;
GRANT SELECT ON ALL TABLES IN SCHEMA genre_classification_db.imdb TO role gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA genre_classification_db.results TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA genre_classification_db.imdb TO ROLE gds_role;

USE ROLE gds_role;

USE DATABASE genre_classification_db;
USE SCHEMA results;
USE WAREHOUSE <A_WAREHOUSE>;

-- Training stage of the GraphSAGE node classification algorithm
CALL Neo4j_GDS.graph.gs_nc_train('GPU_NV_S', {
    'graph_config': {
        'default_table_prefix': 'genre_classification_db.imdb',
        'node_tables': ['actor', 'director', 'movie'],
        'relationship_tables': {
            'acted_in': {'source_table': 'actor', 'target_table': 'movie', 'orientation': 'UNDIRECTED'},
            'directed_in': {'source_table': 'director', 'target_table': 'movie', 'orientation': 'UNDIRECTED'}
        }
    },
    'task_config': {
        'modelname': 'nc-imdb',
        'num_epochs': 10,
        'num_samples': [20, 20],
        'target_label': 'movie',
        'target_property': 'genre',
        'class_weights': true
    }
});

-- Prediction stage of the GraphSAGE node classification algorithm
CALL Neo4j_GDS.graph.gs_nc_predict('GPU_NV_S', {
    'graph_config': {
        'default_table_prefix': 'genre_classification_db.imdb',
        'node_tables': ['actor', 'director', 'movie'],
        'relationship_tables': {
            'acted_in': {'source_table': 'actor', 'target_table': 'movie', 'orientation': 'UNDIRECTED'},
            'directed_in': {'source_table': 'director', 'target_table': 'movie', 'orientation': 'UNDIRECTED'}
        }
    },
    'task_config': {
        'modelname': 'nc-imdb'
    },
    'output_config': [{'node_label': 'movie', 'output_table': 'genre_classification_db.results.genre_predictions'}]
});

-- Check the results of the predictions
SELECT * FROM genre_classification_db.results.genre_predictions LIMIT 20;

-- Training stage of the GraphSAGE unsupervised algorithm
CALL Neo4j_GDS.graph.gs_unsup_train('GPU_NV_S', {
    'graph_config': {
        'default_table_prefix': 'genre_classification_db.imdb',
        'node_tables': ['actor', 'director', 'movie'],
        'relationship_tables': {
            'acted_in': {'source_table': 'actor', 'target_table': 'movie', 'orientation': 'UNDIRECTED'},
            'directed_in': {'source_table': 'director', 'target_table': 'movie', 'orientation': 'UNDIRECTED'}
        }
    },
    'task_config': {
        'modelname': 'unsup-imdb',
        'num_epochs': 10,
        'num_samples': [20, 20]
    }
});

-- Prediction stage of the GraphSAGE unsupervised algorithm - computing embeddings
CALL Neo4j_GDS.graph.gs_unsup_predict('GPU_NV_S', {
    'modelname': 'unsup-imdb',
    'graph_config': {
        'default_table_prefix': 'genre_classification_db.imdb',
        'node_tables': ['actor', 'director', 'movie'],
        'relationship_tables': {
            'acted_in': {'source_table': 'actor', 'target_table': 'movie', 'orientation': 'UNDIRECTED'},
            'directed_in': {'source_table': 'director', 'target_table': 'movie', 'orientation': 'UNDIRECTED'}
        }
    },
    'task_config': {
        'modelname': 'unsup-imdb'
    },
    'output_config': [{
        'node_label': 'movie',
        'output_table': 'genre_classification_db.results.movie_embeddings'
    }]
});

-- Check the embeddings
SELECT * FROM genre_classification_db.results.movie_embeddings LIMIT 20;

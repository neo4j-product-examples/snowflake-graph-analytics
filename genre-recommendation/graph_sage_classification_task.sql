USE ROLE ACCOUNTADMIN;

-- Let's assume you have a database named 'genre_classification_db' which contains data in the schema 'imdb'
-- and 'results' for storing the results of algorithm predictions.
-- Application name is Neo4j_GDS
GRANT USAGE ON DATABASE genre_classification_db TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA genre_classification_db.results TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA genre_classification_db.imdb TO APPLICATION Neo4j_GDS;

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
        'numEpochs': 10,
        'numSamples': [20, 20],
        'targetLabel': 'movie',
        'targetProperty': 'genre',
        'classWeights': true
    }
});

-- Prediction stage of the GraphSAGE node classification algorithm
CALL Neo4j_GDS.graph.gs_nc_predict('GPU_NV_S', {
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
    'write': [{'nodeLabel': 'movie', 'outputTable': 'genre_classification_db.results.genre_predictions'}]
});

-- Check the results of the predictions
SELECT * FROM genre_classification_db.results.genre_predictions LIMIT 20;

-- Existing embeddings can be deleted if needed
CALL Neo4j_GDS.graph.drop_model('nc-imdb');

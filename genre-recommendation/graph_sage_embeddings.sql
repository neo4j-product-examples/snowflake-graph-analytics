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

-- Create a view for movie table to exclude the genre column
USE ROLE accountadmin;

CREATE VIEW IF NOT EXISTS genre_classification_db.imdb.movie_plot AS
SELECT nodeid, plot_keywords
FROM genre_classification_db.imdb.movie;

GRANT SELECT ON ALL VIEWS IN SCHEMA genre_classification_db.imdb TO ROLE gds_role;
GRANT SELECT ON ALL VIEWS IN SCHEMA genre_classification_db.imdb TO APPLICATION Neo4j_GDS;

USE ROLE gds_role;

-- Training stage of the GraphSAGE unsupervised algorithm
CALL Neo4j_GDS.graph.gs_unsup_train('GPU_NV_S', {
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

-- Prediction stage of the GraphSAGE unsupervised algorithm - computing embeddings
CALL Neo4j_GDS.graph.gs_unsup_predict('GPU_NV_S', {
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

-- Check the embeddings
SELECT * FROM genre_classification_db.results.movie_embeddings LIMIT 20;

-- Existing embeddings can be deleted if needed
CALL Neo4j_GDS.graph.drop_model('unsup-imdb');

-- The result embeddings can be used for various machine learning tasks, such as clustering or classification.

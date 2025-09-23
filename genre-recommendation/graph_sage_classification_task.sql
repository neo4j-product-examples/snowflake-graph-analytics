USE ROLE ACCOUNTADMIN;

-- Let's assume you have a database named 'genre_classification_db' which contains data in the schema 'imdb'
-- and 'results' for storing the results of algorithm predictions.
-- Application name is NEO4J_GRAPH_ANALYTICS

-- Create consumer role for users of the Graph Analytics application
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

GRANT USAGE ON WAREHOUSE <A_WAREHOUSE> TO ROLE gds_role;

USE ROLE gds_role;

USE DATABASE genre_classification_db;
USE SCHEMA results;
USE WAREHOUSE <A_WAREHOUSE>;

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

-- Check the results of the predictions
SELECT * FROM genre_classification_db.results.genre_predictions LIMIT 20;

-- Existing embeddings can be deleted if needed
CALL NEO4J_GRAPH_ANALYTICS.graph.drop_model('nc-imdb');

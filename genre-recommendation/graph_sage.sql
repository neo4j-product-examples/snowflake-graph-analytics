USE ROLE <A_ROLE>;

GRANT USAGE ON DATABASE <A_DATABASE> TO APPLICATION <APP_NAME>;
GRANT USAGE ON SCHEMA <A_DATABASE>.<A_SCHEMA> TO APPLICATION <APP_NAME>;
GRANT CREATE STAGE ON SCHEMA <A_DATABASE>.<A_SCHEMA> TO APPLICATION <APP_NAME>;
GRANT SELECT ON ALL TABLES IN SCHEMA <A_DATABASE>.<A_SCHEMA> TO APPLICATION <APP_NAME>;
GRANT CREATE TABLE ON SCHEMA <A_DATABASE>.<A_SCHEMA> TO APPLICATION <APP_NAME>;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA <A_DATABASE>.<A_SCHEMA> TO ROLE <A_ROLE>;
USE DATABASE <APP_NAME>;
USE SCHEMA gml;
USE WAREHOUSE <A_WAREHOUSE>;

CALL gml.create_session('GPU_NV_S');

SELECT gml.gs_nc_train({'modelname': 'nc-imdb', 'graph_config': {'database': '<A_DATABASE>', 'schema': '<A_SCHEMA>', 'node_tables': {'actor': ['plot_keywords'], 'director': ['plot_keywords'], 'movie': ['plot_keywords', 'genre']}, 'rel_tables': {'acted_in': [], 'directed_in': []}}, 'task_config': {'num_epochs': 10, 'num_samples': [20, 20], 'target_label': 'movie', 'target_property': 'genre', 'class_weights': true}});

SELECT gml.gs_nc_predict({'modelname': 'nc-imdb', 'graph_config': { 'node_output_table': 'genre_predictions', 'database': '<A_DATABASE>', 'schema': '<A_SCHEMA>', 'node_tables': {'actor': ['plot_keywords'], 'director': ['plot_keywords'], 'movie': ['plot_keywords', 'genre']}, 'rel_tables': {'acted_in': [], 'directed_in': []}}, 'task_config': {}});

SELECT * FROM <A_DATABASE>.<A_SCHEMA>.genre_predictions LIMIT 20;

-- The below uses unsupervised graphsage to train a model and generate node embeddings


SELECT gml.gs_unsup_train({'modelname': 'unsup-imdb', 'graph_config': {'database': '<A_DATABASE>', 'schema': '<A_SCHEMA>', 'node_tables': {'actor': ['plot_keywords'], 'director': ['plot_keywords'], 'movie': ['plot_keywords']}, 'rel_tables': {'acted_in': [], 'directed_in': []}}, 'task_config': {'num_epochs': 10, 'num_samples': [20, 20]}});

SELECT GML.gs_unsup_predict({'modelname': 'unsup-imdb', 'graph_config': { 'node_output_table': 'imdb_embeddings', 'database': '<A_DATABASE>', 'schema': '<A_SCHEMA>', 'node_tables': {'actor': ['plot_keywords'], 'director': ['plot_keywords'], 'movie': ['plot_keywords']}, 'rel_tables': {'acted_in': [], 'directed_in': []}}, 'task_config': {}});


SELECT * FROM <A_DATABASE>.<A_SCHEMA>.imdb_embeddings LIMIT 20;

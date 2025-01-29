-- ///////////////////////////////////////////////////////////
-- 1) SETUP
-- ///////////////////////////////////////////////////////////
-- Create a consumer role for users of the GDS application
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_user TO ROLE gds_role;
-- Create a consumer role for administrators of the GDS application
CREATE ROLE IF NOT EXISTS gds_admin_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_admin TO ROLE gds_admin_role;

-- Grant access to data
-- The application reads data to build a graph object, and it also writes results into new tables.
-- We therefore need to grant the right permissions to give the application access.
GRANT USAGE ON DATABASE FREIGHT_NETWORK TO APPLICATION neo4j_graph_analytics;
GRANT USAGE ON SCHEMA FREIGHT_NETWORK.PUBLIC TO APPLICATION neo4j_graph_analytics;

-- required to read view data into a graph
GRANT SELECT ON ALL VIEWS IN SCHEMA FREIGHT_NETWORK.PUBLIC TO APPLICATION neo4j_graph_analytics;
GRANT SELECT ON ALL TABLES IN SCHEMA FREIGHT_NETWORK.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- required to write computation results into a table
GRANT CREATE TABLE ON SCHEMA FREIGHT_NETWORK.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- optional, ensuring the consumer role has access to tables created by the application
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA FREIGHT_NETWORK.PUBLIC TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA FREIGHT_NETWORK.PUBLIC TO ROLE accountadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA FREIGHT_NETWORK.PUBLIC TO ROLE accountadmin;

-- Spin up Neo4j
CALL neo4j_graph_analytics.gds.create_session('CPU_X64_M');

-- show the various functions available
SHOW USER FUNCTIONS IN APPLICATION neo4j_graph_analytics;


-- ///////////////////////////////////////////////////////////
-- 2) Shortest Paths
-- ///////////////////////////////////////////////////////////
USE DATABASE FREIGHT_NETWORK;
USE SCHEMA FREIGHT_NETWORK.PUBLIC;

-- drop graph if already exists.
SELECT neo4j_graph_analytics.gds.graph_drop('path_graph', { 'failIfMissing': false });

-- create graph projection
SELECT neo4j_graph_analytics.gds.graph_project('path_graph', {
    'nodeTable': 'freight_network.public.nodes',
                                               'relationshipTable': 'freight_network.public.relationships',
                                               'readConcurrency': 28
           });

SELECT neo4j_graph_analytics.gds.dijkstra('path_graph', {
    'mutateRelationshipType': 'PATH_01',
                                          'sourceNode': 1,
                                          'targetNodes': [ 11, 2, 3, 4, 156, 197],
                                          'relationshipWeightProperty': 'AVG_EFFECTIVE_MINUTES'
           });


-- write relationships back ot table
SELECT neo4j_graph_analytics.gds.write_relationships('path_graph', {
'relationshipType': 'PATH_01',
                                                     'table': 'freight_network.public.path_01'
           });
SELECT * FROM PATH_01;
-- ///////////////////////////////////////////////////////////
-- 4) CLEANUP NEO4J
-- ///////////////////////////////////////////////////////////
-- turn off Neo4j and spins down compute pool
CALL neo4j_graph_analytics.gds.stop_session();
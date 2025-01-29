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
GRANT USAGE ON DATABASE RETAIL TO APPLICATION neo4j_graph_analytics;
GRANT USAGE ON SCHEMA RETAIL.PUBLIC TO APPLICATION neo4j_graph_analytics;

-- required to read view data into a graph
GRANT SELECT ON ALL VIEWS IN SCHEMA RETAIL.PUBLIC TO APPLICATION neo4j_graph_analytics;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- required to write computation results into a table
GRANT CREATE TABLE ON SCHEMA RETAIL.PUBLIC TO APPLICATION neo4j_graph_analytics;
-- optional, ensuring the consumer role has access to tables created by the application
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RETAIL.PUBLIC TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA RETAIL.PUBLIC TO ROLE accountadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA RETAIL.PUBLIC TO ROLE accountadmin;

-- Spin up Neo4j
CALL neo4j_graph_analytics.gds.create_session('CPU_X64_M');

-- show the various functions available
SHOW USER FUNCTIONS IN APPLICATION neo4j_graph_analytics;


-- ///////////////////////////////////////////////////////////
-- 2) graph algorithms for segmentation
-- ///////////////////////////////////////////////////////////
USE DATABASE RETAIL;
USE SCHEMA RETAIL.PUBLIC;

-- drop graph if already exists.
SELECT neo4j_graph_analytics.gds.graph_drop('customer_graph', { 'failIfMissing': false });

-- create graph projection
SELECT neo4j_graph_analytics.gds.graph_project('customer_graph', {
    'nodeTable': 'retail.public.customer_nodes',
                                               'relationshipTable': 'retail.public.customer_relationships',
                                               'readConcurrency': 28
           });

-- run louvain community detection
SELECT neo4j_graph_analytics.gds.louvain('customer_graph', {
    'relationshipWeightProperty': 'CO_PURCHASE_COUNT',
                                         'mutateProperty': 'customer_segment'
           });

-- write back to table
SELECT neo4j_graph_analytics.gds.write_nodeproperties('customer_graph',
           {'nodeProperties': ['customer_segment'], 'table': 'retail.public.customer_metrics'});
SELECT * FROM CUSTOMER_METRICS ORDER BY customer_segment;

-- ///////////////////////////////////////////////////////////
-- 3) CLEANUP NEO4J
-- ///////////////////////////////////////////////////////////
-- turn off Neo4j and spins down compute pool
CALL neo4j_graph_analytics.gds.stop_session();
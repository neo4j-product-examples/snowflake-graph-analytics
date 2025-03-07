-- ///////////////////////////////////////////////////////////
-- 1) SETUP
-- ///////////////////////////////////////////////////////////
-- Create a consumer role for users of the GDS application
CREATE ROLE IF NOT EXISTS gds_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_user TO ROLE gds_role;
-- Create a consumer role for administrators of the GDS application
CREATE ROLE IF NOT EXISTS gds_admin_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_admin TO ROLE gds_admin_role;

-- Grant access to consumer data
-- The application reads consumer data to build a graph object, and it also writes results into new tables.
-- We therefore need to grant the right permissions to give the application access.
GRANT USAGE ON DATABASE p2p_demo TO APPLICATION neo4j_graph_analytics;
GRANT USAGE ON SCHEMA p2p_demo.public TO APPLICATION neo4j_graph_analytics;

-- required to read tabular data into a graph
GRANT SELECT ON ALL TABLES IN SCHEMA p2p_demo.public TO APPLICATION neo4j_graph_analytics;
-- required to write computation results into a table
GRANT CREATE TABLE ON SCHEMA p2p_demo.public TO APPLICATION neo4j_graph_analytics;
-- optional, ensuring the consumer role has access to tables created by the application
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA p2p_demo.public TO ROLE gds_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA p2p_demo.public TO ROLE accountadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA p2p_demo.public TO ROLE accountadmin;

CALL neo4j_graph_analytics.gds.create_session('CPU_X64_M');

SELECT neo4j_graph_analytics.gds.graph_drop('transaction_graph', { 'failIfMissing': false });
USE DATABASE p2p_demo;

-- ///////////////////////////////////////////////////////////
-- 2) LOUVIAN & PAGERANK
-- ///////////////////////////////////////////////////////////
-- user table (nodes)
USE DATABASE p2p_demo;
USE SCHEMA public;
SELECT * FROM p2p_users;

-- transaction table relationships
SELECT * FROM p2p_agg_transactions;

-- Construct a graph projection from node and relationship tables
SELECT neo4j_graph_analytics.gds.graph_project('transaction_graph',{
  'nodeTables': {
    'p2p_demo.public.p2p_users': 'Node'
  },
  'relationshipTables': {
    'p2p_demo.public.p2p_agg_transactions': {
      'type': 'REL',
      'sourceTable': 'p2p_demo.public.p2p_users',
      'targetTable': 'p2p_demo.public.p2p_users',
      'orientation': 'NATURAL'
    }
  }
});

--calculate louvain communities
SELECT neo4j_graph_analytics.gds.louvain('transaction_graph', {'mutateProperty': 'community_id'});

-- Write  to table
--SELECT neo4j_graph_analytics.gds.write_nodeproperties('transaction_graph',
--           {'nodeProperties': ['community_id'], 'table': 'p2p_demo.public.p2p_louvain'});
SELECT neo4j_graph_analytics.gds.write_nodeproperties_to_table('transaction_graph', {
  'nodeLabels': ['Node'],
  'nodeProperties': ['community_id'],
  'tableSuffix': '_louvain'
});

-- Query result from table
SELECT * FROM p2p_users_louvain ORDER BY community_id;

-- Stats on the size of each community
SELECT community_id, count(nodeid) AS community_size
FROM p2p_users_louvain
GROUP BY community_id
ORDER BY community_size DESC LIMIT 10;

-- ///////////////////////////////////////////////////////////
-- 2)  Find the most influential people using PAGERANK
-- ///////////////////////////////////////////////////////////

--calculate transaction pagerank
SELECT neo4j_graph_analytics.gds.page_rank('transaction_graph', {'mutateProperty': 'page_rank'});

-- Write  to table
SELECT DF_SNOW_NEO4J_GRAPH_ANALYTICS.gds.write_nodeproperties('transaction_graph', {
    'nodeLabels': ['Node'],
    'nodeProperties': ['score'], 
    'tableSuffix': '_pagerank'
});

-- Query result from table
SELECT * FROM p2p_users_pagerank ORDER BY score DESC limit 5;

-- ///////////////////////////////////////////////////////////
-- 3) WCC Entity Resolution
-- ///////////////////////////////////////////////////////////
SELECT * FROM p2p_trans_w_shared_card;

-- Construct a graph projection from node and relationship tables
SELECT neo4j_graph_analytics.gds.graph_drop('entity_linking_graph', { 'failIfMissing': false });

-- Similar to calling the functions with simple or qualified names, we have to reference the tables wither with qualified names or with simple names while USE ing the database or schema.
SELECT neo4j_graph_analytics.gds.graph_project('entity_linking_graph', {
  'nodeTables': {
    'p2p_demo.public.p2p_users': 'Node'
  },
  'relationshipTables': {
    'p2p_demo.public.p2p_trans_w_shared_card': {
      'type': 'REL',
      'sourceTable': 'p2p_demo.public.p2p_users',
      'targetTable': 'p2p_demo.public.p2p_users',
      'orientation': 'NATURAL'
    }
  }
});

-- calculate wcc
SELECT neo4j_graph_analytics.gds.wcc('entity_linking_graph', {'mutateProperty': 'wcc_id'});

-- Write  to table
SELECT DF_SNOW_NEO4J_GRAPH_ANALYTICS.gds.write_nodeproperties('entity_linking_graph', {
    'nodeLabels': ['Node'],
    'nodeProperties': ['wcc_id'], 
    'tableSuffix': '_wcc'}
);

SELECT * FROM P2P_USERS_WCC ORDER BY wcc_id;

-- create a resolved entity view based on WCC
CREATE OR REPLACE VIEW resolved_p2p_users AS
SELECT p2p_users_wcc.wcc_id,
       count(*) AS user_count,
       TO_NUMBER(SUM(p2p_users.fraud_transfer_flag)>0) AS has_fraud_flag,
       ARRAY_AGG(p2p_users.nodeId) AS user_ids
FROM p2p_users JOIN p2p_users_wcc ON p2p_users.nodeId = p2p_users_wcc.nodeId
GROUP BY wcc_id ORDER BY user_count DESC;

-- ///////////////////////////////////////////////////////////
-- 4) CLEANUP
-- ///////////////////////////////////////////////////////////
CALL neo4j_graph_analytics.gds.stop_session();

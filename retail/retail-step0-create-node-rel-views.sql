USE DATABASE RETAIL;
USE SCHEMA RETAIL.PUBLIC;

-- create node mapping
CREATE OR REPLACE SEQUENCE node_id START = 0 INCREMENT = 1;

CREATE OR REPLACE TABLE CUSTOMER_NODE_MAPPING (NODEID, CUSTOMERID) AS
SELECT node_id.nextval, CUSTOMERID
FROM CUSTOMERS;
SELECT * FROM CUSTOMER_NODE_MAPPING;

-- final node view to use with Neo4j
CREATE OR REPLACE VIEW CUSTOMER_NODES(nodeId) AS
SELECT NODEID FROM CUSTOMER_NODE_MAPPING;
SELECT * FROM CUSTOMER_NODES;

-- relationship table
CREATE OR REPLACE VIEW CUSTOMER_RELATIONSHIPS(sourceNodeId, targetNodeId, co_purchase_count) AS
SELECT
    src_nodes.NODEID AS sourceNodeId,
    tgt_nodes.NODEID AS targetNodeId,
    TO_DOUBLE(COUNT(*))
FROM TRANSACTIONS AS t1
         FULL OUTER JOIN TRANSACTIONS AS t2
                         ON t1.articleId=t2.articleId
         INNER JOIN CUSTOMER_NODE_MAPPING AS src_nodes
                    ON t1.customerId = src_nodes.customerId
         INNER JOIN CUSTOMER_NODE_MAPPING  AS tgt_nodes
                    ON t2.customerId = tgt_nodes.customerId
WHERE t1.customerId <> t2.customerId
GROUP BY sourceNodeId, targetNodeId, t1.articleId;

SELECT count(*) FROM CUSTOMER_RELATIONSHIPS;
SELECT * FROM CUSTOMER_RELATIONSHIPS ORDER BY co_purchase_count DESC;
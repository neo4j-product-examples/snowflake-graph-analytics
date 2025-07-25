# Finding Similar Patient Journeys with Neo4j

## Overview

Duration: 2

### What Is Neo4j Graph Analytics For Snowflake? 

Neo4j helps organizations find hidden relationships and patterns across billions of data connections deeply, easily, and quickly. **Neo4j Graph Analytics for Snowflake** brings to the power of graph directly to Snowflake, allowing users to run 65+ ready-to-use algorithms on their data, all without leaving Snowflake! 

### Prerequisites

- The Native App [Neo4j Graph Analytics](https://app.snowflake.com/marketplace/listing/GZTDZH40CN) for Snowflake

### What You Will Need

- A [Snowflake account](https://signup.snowflake.com/?utm_cta=quickstarts) with appropriate access to databases and schemas.
- Neo4j Graph Analytics application installed from the Snowflake marketplace. Access the marketplace via the menu bar on the left hand side of your screen, as seen below:
  ![image](assets/marketplace.png)

### What You Will Build

- A method to identify similar patient journeys

### What You Will Learn

- How to prepare and project your data for graph analytics
- How to use node similarity to identify similar nodes
- How to read and write directly from and to your snowflake tables

## Loading The Data
Duration: 5

Dataset overview : This dataset is modelled to design and analyze patients and different procedures that they undergo using graph analytics. 

<img src="assets/datamodel.png" alt="image" width="400"/>


Let's name our database `NEO4J_PATIENT_DB`. Using the CSVs found [here](https://github.com/neo4j-product-examples/aura-graph-analytics/tree/main/patient_journey/data), We are going to add two new tables:

- One called `PROCEDURES` based on the Procedures.csv
- One called `PATIENTS based` on Patients.csv

Follow the steps found [here](https://docs.snowflake.com/en/user-guide/data-load-web-ui) to load in your data.

## Setting Up
Duration: 5

### Import The Notebook

- We’ve provided a Colab notebook to walk you through each SQL and Python step—no local setup required!
- Download the .ipynb found [here](https://github.com/neo4j-product-examples/snowflake-graph-analytics/blob/main/patient-journey/finding-similar-patients.ipynb), and import the notebook into snowflake.

### Permissions

Before we run our algorithms, we need to set the proper permissions. But before we get started granting different roles, we need to ensure that you are using `accountadmin` to grant and create roles. Lets do that now:


```sql
USE ROLE accountadmin;
```

Next let's set up the necessary roles, permissions, and resource access to enable Graph Analytics to operate on data within the `NEO4J_PATIENT_DB.PUBLIC.SCHEMA`. It creates a consumer role (gds_user_role) for users and administrators, grants the Neo4j Graph Analytics application access to read from and write to tables and views, and ensures that future tables are accessible. 

It also provides the application with access to the required compute pool and warehouse resources needed to run graph algorithms at scale.


```sql
-- Create a consumer role for users and admins of the GDS application
CREATE ROLE IF NOT EXISTS gds_user_role;
CREATE ROLE IF NOT EXISTS gds_admin_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_user TO ROLE gds_user_role;
GRANT APPLICATION ROLE neo4j_graph_analytics.app_admin TO ROLE gds_admin_role;

CREATE DATABASE ROLE IF NOT EXISTS gds_db_role;
GRANT DATABASE ROLE gds_db_role TO ROLE gds_user_role;
GRANT DATABASE ROLE gds_db_role TO APPLICATION neo4j_graph_analytics;

-- Grant access to consumer data
GRANT USAGE ON DATABASE NEO4J_PATIENT_DB TO ROLE gds_user_role;
GRANT USAGE ON SCHEMA NEO4J_PATIENT_DB.PUBLIC TO ROLE gds_user_role;

-- Required to read tabular data into a graph
GRANT SELECT ON ALL TABLES IN DATABASE NEO4J_PATIENT_DB TO DATABASE ROLE gds_db_role;

-- Ensure the consumer role has access to created tables/views
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA NEO4J_PATIENT_DB.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA NEO4J_PATIENT_DB.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT CREATE TABLE ON SCHEMA NEO4J_PATIENT_DB.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT CREATE VIEW ON SCHEMA NEO4J_PATIENT_DB.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA NEO4J_PATIENT_DB.PUBLIC TO DATABASE ROLE gds_db_role;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA NEO4J_PATIENT_DB.PUBLIC TO DATABASE ROLE gds_db_role;

-- Compute and warehouse access
GRANT USAGE ON WAREHOUSE NEO4J_GRAPH_ANALYTICS_APP_WAREHOUSE TO APPLICATION neo4j_graph_analytics;
```

Then we need to switch the role we created:


```sql
USE ROLE gds_user_role;
```

## Cleaning Our Data

Duration: 5

We need our data to be in a particular format in order to work with Graph Analytics. In general it should be like so:

### For The Table Representing Nodes:

The first column should be called `nodeId`, which represents the ids for the each node in our graph

### For The table Representing Relationships:

We need to have columns called `sourceNodeId` and `targetNodeId`. These will tell Graph Analytics the direction of the transaction, which in this case means:

- Who is the patient (sourceNodeId) and
- What is the procedure (targetNodeId)

Let's take a look at the starting point for our data. We have a table for patients and a table for procedures:


```sql
SELECT * FROM NEO4J_PATIENT_DB.PUBLIC.PROCEDURES LIMIT 10;
```

| STARTDATE          | STOP               | PATIENT                              | ENCOUNTER                            | SYSTEM                                           | CODE      | DESCRIPTION                                     | BASE_COST | REASONCODE | REASONDESCRIPTION\\          |
| ------------------ | ------------------ | ------------------------------------ | ------------------------------------ | ------------------------------------------------ | --------- | ----------------------------------------------- | --------- | ---------- | ---------------------------- |
| 2015-06-19 5:03:41 | 2015-06-19 5:18:41 | a0f3cc78-810a-3ec6-36cf-221b374350d6 | 3e3cd9ec-d27a-98ac-c4a4-51bc4420cb7b | [http://snomed.info/sct](http://snomed.info/sct) | 252160004 | Standard pregnancy test (procedure)             | 9046.75   | 72892002   | Normal pregnancy (finding)\\ |
| 2015-06-19 5:03:41 | 2015-06-19 5:18:41 | a0f3cc78-810a-3ec6-36cf-221b374350d6 | 3e3cd9ec-d27a-98ac-c4a4-51bc4420cb7b | [http://snomed.info/sct](http://snomed.info/sct) | 169230002 | Ultrasound scan for fetal viability (procedure) | 4049.7    | 72892002   | Normal pregnancy (finding)\\ |
| 2015-06-19 5:03:41 | 2015-06-19 5:18:41 | a0f3cc78-810a-3ec6-36cf-221b374350d6 | 3e3cd9ec-d27a-98ac-c4a4-51bc4420cb7b | [http://snomed.info/sct](http://snomed.info/sct) | 274804006 | Evaluation of uterine fundal height (procedure) | 3774.39   | 72892002   | Normal pregnancy (finding)\\ |


```sql
SELECT * FROM NEO4J_PATIENT_DB.PUBLIC.PATIENTS LIMIT 10;
```

| ID                                   | PREFIX | FIRST      | MIDDLE  | LAST          | SUFFIX | MAIDEN | MARITAL | RACE  | ETHNICITY   | GENDER | BIRTHPLACE                    | ADDRESS                   | CITY      | STATE         | ZIP  |
| ------------------------------------ | ------ | ---------- | ------- | ------------- | ------ | ------ | ------- | ----- | ----------- | ------ | ----------------------------- | ------------------------- | --------- | ------------- | ---- |
| 41313b42-6ce6-fa01-6021-6041fc6b1a26 | Mr.    | Linwood526 |         | Orn563        |        |        | M       | white | nonhispanic | M      | Boston  Massachusetts  US     | 414 Gusikowski Grove      | Worcester | Massachusetts | 1603 |
| 06df5af7-aabe-008f-2c56-edb2a080c52a | Mr.    | Beau391    | Gail741 | Goldner995    |        |        |         | black | nonhispanic | M      | Leominster  Massachusetts  US | 635 Ullrich Meadow Apt 38 | Boston    | Massachusetts | 2129 |
| 5c8a31b6-6309-f047-71f9-c78778250acd | Mr.    | Isreal8    | Dan465  | Jakubowski832 |        |        |         | white | nonhispanic | M      | Wrentham  Massachusetts  US   | 1082 Wehner Ferry Unit 83 | Dartmouth | Massachusetts | 0    |

We are then going to clean this up into two tables that just have the nodeids for both patient and procedure:


```sql
CREATE OR REPLACE TABLE NEO4J_PATIENT_DB.PUBLIC.PATIENT_NODE_MAPPING (nodeId) AS
SELECT DISTINCT p.ID from NEO4J_PATIENT_DB.PUBLIC.PATIENTS p;

CREATE OR REPLACE TABLE NEO4J_PATIENT_DB.PUBLIC.PROCEDURE_NODE_MAPPING (nodeId) AS
SELECT DISTINCT p.code from NEO4J_PATIENT_DB.PUBLIC.PROCEDURES p;
```

In order to keep this managable, we are just going to look at patients (and what procedures they underwent) in the context of kidney disease. So first we will filter down patients to only include those with kidney disease:


```sql
// create a subset of patients that have had any of the 4 kidney disease codes
CREATE OR REPLACE VIEW KidneyPatients_vw (nodeId) AS
    SELECT DISTINCT PATIENT_NODE_MAPPING.NODEID as nodeId
    FROM PROCEDURES
            JOIN PATIENT_NODE_MAPPING ON PATIENT_NODE_MAPPING.NODEID = PROCEDURES.PATIENT 
    WHERE PROCEDURES.REASONCODE IN (431857002,46177005,161665007,698306007)
;
```

Then we will only look at the procedures that those kidney patients have undergone:


```sql
// There are ~400K procedures - it is doubtful that the kidney patients even have used a small
// fraction of those.  To reduce GDS memory and speed algorithm execution, we want to load
// only those procedures that kidney patients have had.
CREATE OR REPLACE VIEW KidneyPatientProcedures_vw (nodeId) AS
    SELECT DISTINCT PROCEDURE_NODE_MAPPING.NODEID as nodeId
    FROM PROCEDURES 
        JOIN PROCEDURE_NODE_MAPPING ON PROCEDURE_NODE_MAPPING.nodeId = PROCEDURES.CODE
        JOIN KIDNEYPATIENTS_VW ON PATIENT = PROCEDURES.PATIENT;

// create the relationship view of kidney patients to the procedures they have had
CREATE OR REPLACE VIEW KidneyPatientProcedure_relationship_vw (sourceNodeId, targetNodeId) AS
    SELECT DISTINCT PATIENT_NODE_MAPPING.NODEID as sourceNodeId, PROCEDURE_NODE_MAPPING.NODEID as targetNodeId
    FROM PATIENT_NODE_MAPPING
         JOIN PROCEDURES ON PROCEDURES.PATIENT = PATIENT_NODE_MAPPING.NODEID
         JOIN PROCEDURE_NODE_MAPPING ON PROCEDURE_NODE_MAPPING.NODEID = PROCEDURES.CODE;
```

## Running Your Algorithms

Duration: 10

Now we are finally at the step where we create a projection, run our algorithms, and write back to snowflake. We will run louvain to determine communities within our data. Louvain identifies communities by grouping together nodes that have more connections to each other than to nodes outside the group.

You can find more information about writing this function in our [documentation](https://neo4j.com/docs/snowflake-graph-analytics/current/getting-started/).

Broadly, you will need a few things:

- A table for nodes, which for us is `neo4j_patient_db.public.KidneyPatients_vw`
- A table for relationships, which for us is `neo4j_patient_db.public.KidneyPatientProcedure_relationship_vw`
- The size of the compute pool you would like to use, which would be `CPU_X64_XS`
- A table to output results, which would be `neo4j_patient_db.public.PATIENT_PROCEDURE_SIMILARITY`
- A node label for our nodes, which would be `KidneyPatients_vw`


```sql
CALL neo4j_graph_analytics.graph.node_similarity('CPU_X64_L', {
  'project': {
    'defaultTablePrefix': 'neo4j_patient_db.public',
    'nodeTables': ['KidneyPatients_vw','KidneyPatientProcedures_vw'], 
    'relationshipTables': {
      'KidneyPatientProcedure_relationship_vw': {
        'sourceTable': 'KidneyPatients_vw',
        'targetTable': 'KidneyPatientProcedures_vw'
      }
    }
  },
  'compute': { 'topK': 10,
                'similarityCutoff': 0.3,
                'similarityMetric': 'JACCARD'
            },
  'write': [
    {
    'sourceLabel': 'KidneyPatients_vw',
    'targetLabel': 'KidneyPatients_vw',
    'relationshipProperty': 'similarity',
    'outputTable':  'neo4j_patient_db.public.PATIENT_PROCEDURE_SIMILARITY'
    }
  ]
});
```

Let's take a look at the results!


```sql
SELECT SOURCENODEID, TARGETNODEID, SIMILARITY
FROM NEO4J_PATIENT_DB.PUBLIC.PATIENT_PROCEDURE_SIMILARITY 
LIMIT 10;
```

| SOURCENODEID                         | TARGETNODEID                         | SIMILARITY   |
| ------------------------------------ | ------------------------------------ | ------------ |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | 87069a85-19bf-7d81-0cfe-609b3427d096 | 0.7555555556 |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | e4dbe477-7ad6-db78-79eb-87a82b448787 | 0.8536585366 |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | fe12b9bd-82c4-de9e-f5fb-1a292dc5020d | 0.8181818182 |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | a08d93d4-8fe9-7cb5-cca3-ecaed719f167 | 0.875        |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | 41078972-c38f-d0e5-b835-5f1ec4b69e58 | 0.9487179487 |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | a1ab30fc-76db-e043-0368-dde512b707d6 | 0.85         |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | 68d9406d-f0f8-a93a-377b-d9ff4be171a5 | 0.8          |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | 9048c50d-cf90-be81-5352-5f11335b6533 | 0.8536585366 |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | 752404e8-efac-b9bb-c9da-e64c5fe3948f | 0.8          |
| 00094d82-80de-4444-69cf-cf26fd56b1ac | e57de607-b7c4-d944-9e72-7d786a8a7058 | 0.85         |

You can see the similarity score between one patient and a set of another patients. The higher the similarity score the more similar the patients. From this, we could develop personalize plans based on other patients experiences. 

For example, if a patient had undergone 4 out of 5 of another patients procedures, we might predict that they will likely undergo the fifth procedure as well!

##  Conclusions And Resources

Duration: 2

In this quickstart, you learned how to bring the power of graph insights into Snowflake using Neo4j Graph Analytics. 

### What You Learned

By working with a patient transaction dataset, you were able to:

1. Set up the [Neo4j Graph Analytics](https://app.snowflake.com/marketplace/listing/GZTDZH40CN/neo4j-neo4j-graph-analytics) application within Snowflake.
2. Prepare and project your data into a graph model (patients as nodes, underwent_procedures as relationships).
3. Ran node similarity to identify patients who have a comparable history of medical procedures.

### Resources

- [Neo4j Graph Analytics Documentation](https://neo4j.com/docs/snowflake-graph-analytics/)
- [Installing Neo4j Graph Analytics on SPCS](https://neo4j.com/docs/snowflake-graph-analytics/installation/)
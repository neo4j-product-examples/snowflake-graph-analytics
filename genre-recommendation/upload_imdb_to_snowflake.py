from load_to_pandas import load_to_pandas
import argparse
from snowflake.snowpark import Session

def write_table(args, session, table_name, df):
    session.write_pandas(df, table_name, auto_create_table=True, overwrite=True, quote_identifiers=False)
    query = f"SELECT COUNT(*) from {args.snowflake_database}.{args.snowflake_schema}.{table_name}"
    result = session.sql(query).collect()
    print(f"Wrote {result} rows to {args.snowflake_database}.{args.snowflake_schema}.{table_name}")

def create_session(args):
    connection_params = {
        "account": args.snowflake_account,
        "host": args.snowflake_host,
        "user": args.snowflake_user,
        "password": args.snowflake_password,
        "role": args.snowflake_role,
        "warehouse": args.snowflake_warehouse,
        "database": args.snowflake_database,
        "schema": args.snowflake_schema,
    }
    return Session.builder.configs(connection_params).create()
parser = argparse.ArgumentParser(description='Load imdb into snowflake')

parser.add_argument('--snowflake_account', type=str, help='Snowflake account', required=True)
parser.add_argument('--snowflake_host', type=str, help='Snowflake host', required=True)
parser.add_argument('--snowflake_user', type=str, help='Snowflake user', required=True)
parser.add_argument('--snowflake_password', type=str, help='Snowflake password', required=True)
parser.add_argument('--snowflake_role', type=str, help='Snowflake role', required=True)
parser.add_argument('--snowflake_warehouse', type=str, help='Snowflake warehouse', required=True)
parser.add_argument('--snowflake_database', type=str, help='Snowflake database', required=True)
parser.add_argument('--snowflake_schema', type=str, help='Snowflake schema', required=True)

args = parser.parse_args()

nodedfs, reldfs = load_to_pandas()

with create_session(args) as session:
    session.sql(f"USE ROLE {args.snowflake_role}").collect()
    session.sql(f"USE DATABASE {args.snowflake_database}").collect()
    session.sql(f"USE SCHEMA {args.snowflake_schema}").collect()
    print(f"Using role {args.snowflake_role}")
    print(f"Using database {args.snowflake_database}")
    print(f"Using schema {args.snowflake_schema}")

    for lbl in nodedfs:
        print(f"Writing node table {lbl}")
        write_table(args, session, lbl, nodedfs[lbl])
        session.sql(f"ALTER TABLE {lbl} ADD COLUMN plot_keywords_v VECTOR(FLOAT, 1256);").collect()
        session.sql(f"UPDATE {lbl} SET plot_keywords_v = TO_ARRAY(plot_keywords)::VECTOR(FLOAT, 1256);").collect()
        session.sql(f"ALTER TABLE {lbl} DROP COLUMN plot_keywords;").collect()
        session.sql(f"ALTER TABLE {lbl} RENAME COLUMN plot_keywords_v TO plot_keywords;").collect()


    for rt in reldfs:
        print(f"Writing relationship table {rt}")
        write_table(args, session, rt, reldfs[rt])


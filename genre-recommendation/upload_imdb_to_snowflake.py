import argparse
import os
import pathlib

import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.types import VectorType, IntegerType


def load_to_pandas():
    base_path = pathlib.Path(__file__).resolve().parent
    nodes_path = base_path / "nodedfs"
    rels_path = base_path / "reldfs"

    nodedfs = {}
    for root, dirs, files in os.walk(nodes_path):
        for file in files:
            if file.endswith(".gzip"):
                node_label = file.split(".")[0].upper()
                nodedfs[node_label] = pd.read_parquet(os.path.join(root, file))
                nodedfs[node_label].columns = [col.upper() for col in nodedfs[node_label].columns]
    reldfs = {}
    for root, dirs, files in os.walk(rels_path):
        for file in files:
            if file.endswith(".gzip"):
                rel_label = file.split(".")[0].upper()
                reldfs[rel_label] = pd.read_parquet(os.path.join(root, file))
                reldfs[rel_label].columns = [col.upper() for col in reldfs[rel_label].columns]
    return nodedfs, reldfs


def write_table(session, table_name, df):
    table_name = table_name.upper()
    df = session.create_dataframe(df)
    if "PLOT_KEYWORDS" in df.columns:
        df = df.with_column("PLOT_KEYWORDS", df["PLOT_KEYWORDS"].cast(VectorType(float, 1256)))
    if "GENRE" in df.columns:
        df = df.with_column("GENRE", df["GENRE"].cast(IntegerType()))

    df.write.save_as_table(table_name, mode="overwrite")

    print(f"Wrote {df.count()} rows to "
          f"{session.get_current_database()}.{session.get_current_schema()}.\"{table_name}\"")


def create_session(args):
    if args.connection_name:
        session = Session.builder.config("connection_name", args.connection_name).create()
    else:
        connection_params = {
            "account": args.snowflake_account,
            "user": args.snowflake_user,
            "password": args.snowflake_password,
            "role": args.snowflake_role,
            "warehouse": args.snowflake_warehouse,
            "database": args.snowflake_database,
            "schema": args.snowflake_schema,
        }
        session = Session.builder.configs(connection_params).create()

    for attribute, name in [
        (session.get_current_role(), "Role"),
        (session.get_current_database(), "Database"),
        (session.get_current_schema(), "Schema"),
    ]:
        if attribute is None:
            raise ValueError(f"{name} must be set either via connection name or parameters")
        print(f"Using {name.lower()} {attribute}")

    return session


def main():
    parser = argparse.ArgumentParser(
        description='Load imdb into snowflake, please provide all connection parameters or use a connection name.'
                    'See https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session#connect-by'
                    '-using-the-connections-toml-file.')

    group_connection = parser.add_argument_group(title='Snowflake Connection via Connection Name')
    group_connection.add_argument('--connection_name', type=str, help='Snowflake connection configuration name')

    group_parameters = parser.add_argument_group(title='Snowflake Connection via Parameters')
    group_parameters.add_argument('--snowflake_account', type=str, help='Snowflake account, e.g. ABCDEFG-MY_ACCOUNT')
    group_parameters.add_argument('--snowflake_user', type=str, help='Snowflake user, e.g. "HELEN"')
    group_parameters.add_argument('--snowflake_password', type=str, help='Snowflake password')
    group_parameters.add_argument('--snowflake_role', type=str, help='Snowflake role, e.g. "ACCOUNTADMIN"')
    group_parameters.add_argument('--snowflake_warehouse', type=str, help='Snowflake warehouse')
    group_parameters.add_argument('--snowflake_database', type=str, help='Snowflake existing database to store loaded data')
    group_parameters.add_argument('--snowflake_schema', type=str, help='Snowflake existing schema in the database')

    args = parser.parse_args()

    nodedfs, reldfs = load_to_pandas()

    with create_session(args) as session:
        for lbl in nodedfs:
            print(f"Writing node table \"{lbl}\"")
            write_table(session, lbl, nodedfs[lbl])

        for rt in reldfs:
            print(f"Writing relationship table \"{rt}\"")
            write_table(session, rt, reldfs[rt])


if __name__ == "__main__":
    main()

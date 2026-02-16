

import argparse
import sys
from pathlib import Path
from google.cloud import bigquery


# config
PROJECT_ID = "tennis-etl-pipeline"
DATASET_ID = "tennis_raw"
DDL_FILE = Path(__file__).resolve().parent.parent/ "docs" / "bigquery_schema.sql"


# specify deletion order for tables
TABLE_ORDER = [
    "fact_match_stats",
    "fact_rankings",
    "fact_matches",
    "dim_tournaments",
    "dim_players"
]


# helper functions
def get_client() -> bigquery.Client:
    return bigquery.Client(project = PROJECT_ID)

def get_full_table_id(table_name: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_name}"


def resolve_tables(args_tables: list[str] | None) -> list[str]:
    """
    for --tables flag, validate them.
    else return all tables
    """

    if not args_tables:
        return TABLE_ORDER
    
    invalid = [t for t in args_tables if t not in TABLE_ORDER]
    if invalid:
        print(f"ERROR: unknown table(s): {', '.join(invalid)}")
        print(f"Valid tables: {', '.join(TABLE_ORDER)}")
        sys.exit(1)

    return [t for t in TABLE_ORDER if t in args_tables]


def confirm(action_description: str) -> bool:
    """
    ensure that the user's actions are intended
    """

    response = input(f"\n{action_description}\n Type 'yes' to continue")
    return response.strip().lower() == "yes"


# define various commands
def cmd_status(client: bigquery.Client, **kwargs):
    """
    show row counts, size, and last modified time for all tables.
    """

    print(f"\n{'Table':<25} {'ROws':>12} {'Size (MB)':>12} {'Last Modified'}")
    print("-"*75)

    try:
        dataset_ref = client.get_dataset(DATASET_ID)
    except Exception as e:
        print(f"ERROR: could not access dataset '{DATASET_ID}': {e}")
        return
    
    # compile set of table_ids within the dataest
    tables_found = {t.table_id for t in client.list_tables(dataset_ref)}

    # check that each table in managed list exists in BigQuery
    # output name, rows, size, modified for each found table
    for table_name in TABLE_ORDER:
        if table_name not in tables_found:
            print(f"    {table_name:<25} {'(not found)':>12}")
            continue

        table = client.get_table(get_full_table_id(table_name))
        size_mb = (table.num_bytes or 0) / (1024 * 1024)
        modified = table.modified.strftime("%Y-%m-%d %H:%M") if table.modified else "N/A"
        print(f"    {table_name:<25} {table.num_rows:>12,} {size_mb:>11.2f} {modified}")


    # check for tables in BQ but not in our list
    unmanaged = tables_found - set(TABLE_ORDER)
    if unmanaged:
        print(f"\nUnmanaged tables in dataset: {', '.join(sorted(unmanaged))}")

def cmd_truncate(client: bigquery.Client, tables: list[str] | None = None, **kwargs):
    """
    delete all rows from tables but keep schema intact
    """

    targets = resolve_tables(tables)
    table_list = ", ".join(targets)

    if not confirm(f"This will DELETE ALL ROWS from: {table_list}"):
        print("Aborted")
        return
    
    for table_name in targets:
        full_id = get_full_table_id(table_name)
        try: 
            # use DELETE ... WHERE TRUE
            query = f"DELETE FROM `{full_id}` WHERE TRUE"
            job = client.query(query)
            result = job.result()

            print(f"Truncated {table_name} ({result.num_dml_affected_rows or 0} rows deleted)")

        except Exception as e:
            print(f"ERROR {table_name}: {e}")

    
def cmd_drop(client: bigquery.Client, tables: list[str] | None = None, **kwargs):
    """
    drop tables entirely (schema + data)
    """
    targets = resolve_tables(tables)
    table_list = ", ".join(targets)

    if not confirm(f"This will DROP (delete schema + rows) from: {table_list}"):
        print("Aborted")
        return
    
    for table_name in targets:
        full_id = get_full_table_id(table_name)
        try:
            client.delete_table(full_id, not_found_ok = True)
            print(f"Dropped {table_name}")
        except Exception as e:
            print(f"ERROR {table_name}:{e}")


def cmd_create(client: bigquery.Client, **kwargs):
    """
    run DDL file (CREATE SCHEMA IF NOT EXISTS + CREATE TABLE IF NOT EXISTS).
    safe to run repeatedly - won't overwrite existing tables
    """

    if not DDL_FILE.exists():
        print(f"ERROR: DDL file not found at {DDL_FILE}")
        sys.exit(1)

    ddl_content = DDL_FILE.read_text()
    statements = [s.strip() for s in ddl_content.split(";") if s.strip()]


    print(f"Executing {len(statements)} DDL statements from {DDL_FILE.name}...\n")

    for i, statements in enumerate(statements, 1):

        label = statements[:80].replace("\n", " ")
        print(f"[{i}/{len(statements)}] {label}...")

        try:
            job = client.query(statements)
            job.result()
            print(f"Success")

        except Exception as e:
            print(f"Error: {e}")

    
    # verify 
    dataset_ref = client.get_dataset(DATASET_ID)
    tables = list(client.list_tables(dataset_ref))
    print(f"\nDataset '{DATASET_ID}' now has {len(tables)} tables:")
    for table in tables:
        print(f"  - {table.table_id}")




if __name__ == "__main__":
    main()
from google.cloud import bigquery
from pathlib import Path

def create_schema():

    client = bigquery.Client(project="tennis-etl-pipeline")

    # read DDL file
    ddl_file = Path("../docs/bigquery_schema.sql")
    ddl_content = ddl_file.read_text()

    statements = [s.strip() for s in ddl_content.split(';') if s.strip()]

    print(f"Found {len(statements)} SQL statements")

    for i, statement in enumerate(statements, 1):

        if not statement:
            continue

        print(f"\n[{i}/{len(statements)}] Executing statements...")
        print(f"First 100 chars: {statement[:100]}...")

        try:
            query_job = client.query(statement)
            query_job.result()
            print("Succes")
        except Exception as e:
            print(f"Error: {e}")

    print("\n" + "="*80)
    print("Schema creation complete!")
    print("="*80)

    # verify tables
    dataset = client.get_dataset('tennis_raw')
    tables = list(client.list_tables(dataset))

    print(f"\nCreated {len(tables)} tables:")
    for table in tables:
        print(f" - {table.table_id}")

if __name__ == "__main__":

    create_schema()
import pandas as pd

def cassandra_get_tables(session):
    keyspace = session.keyspace
    query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}';"
    rows = session.execute(query)
    return [row.table_name for row in rows]

def cassandra_fetch_data(session, table_name, columns=None, limit=1000):
    cols = ", ".join(columns) if columns else "*"
    query = f"SELECT {cols} FROM {table_name} LIMIT {limit};"
    rows = session.execute(query)
    df = pd.DataFrame(rows.all())
    return df

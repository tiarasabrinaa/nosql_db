# helpers/cassandra/aggregation.py

def aggregate_data_cassandra(session, table_name, agg_func, column):
    query = f"SELECT {agg_func.upper()}({column}) FROM {table_name};"
    result = session.execute(query)
    return result.one()[0]


def count_rows_cassandra(session, table_name):
    query = f"SELECT COUNT(*) FROM {table_name};"
    result = session.execute(query)
    return result.one()[0]

def group_by_cassandra(session, table_name, group_col, agg_func, agg_col):
    raise NotImplementedError("Cassandra does not support GROUP BY for arbitrary columns.")

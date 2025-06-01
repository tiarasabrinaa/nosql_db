import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import json

# MongoDB helpers
from helpers.mongodb.connector import connect_mongodb
from helpers.mongodb.aggregator import aggregate_data_mongo, group_by_mongo, top_n_mongo, distinct_mongo
from helpers.mongodb.fetcher import get_collections, mongodb_fetch_data

# Cassandra helpers
from helpers.cassandra.connector import connect_cassandra
from helpers.cassandra.fetcher import cassandra_get_tables, cassandra_fetch_data
from helpers.cassandra.aggregator import aggregate_data_cassandra

# Chart helper
from helpers.chart import generate_chart

# Streamlit Config
st.set_page_config(page_title="Multi-Database Aggregator", layout="wide")

# Sidebar Connections
st.sidebar.header("Database Connections")

# MongoDB Connect
st.sidebar.subheader("MongoDB Connection")
uri = st.sidebar.text_input("MongoDB URI", "mongodb://localhost:27017")
mongo_db_name = st.sidebar.text_input("MongoDB Database Name")

if st.sidebar.button("Connect MongoDB"):
    try:
        db = connect_mongodb(uri, mongo_db_name)
        collections = get_collections(db)
        st.session_state['mongo_db'] = db
        st.session_state['mongo_collections'] = collections
        st.success(f"Connected to MongoDB `{mongo_db_name}`")
    except Exception as e:
        st.error(f"MongoDB Connection Failed: {e}")

# Cassandra Connect
st.sidebar.subheader("Cassandra Connection")
hosts = st.sidebar.text_input("Cassandra Hosts (comma separated)", "127.0.0.1").split(",")
keyspace = st.sidebar.text_input("Cassandra Keyspace")

if st.sidebar.button("Connect Cassandra"):
    try:
        session = connect_cassandra(hosts, keyspace)
        tables = cassandra_get_tables(session)
        st.session_state['cassandra_session'] = session
        st.session_state['cassandra_tables'] = tables
        st.success(f"Connected to Cassandra keyspace `{keyspace}`")
    except Exception as e:
        st.error(f"Cassandra Connection Failed: {e}")

# Sidebar Mode Selection
st.sidebar.header("Mode Selection")
mode = st.sidebar.radio(
    "Select Mode",
    ["MongoDB Only", "Cassandra Only", "Merge MongoDB + Cassandra"]
)

# Function to display data and charts

def data_explorer(df, db_type="Database"):
    if df is None or df.empty:
        st.warning(f"No data loaded for {db_type}.")
        return
    st.dataframe(df, use_container_width=True)
    st.download_button(f"Download {db_type} Data", df.to_csv(index=False), f"{db_type.lower()}_data.csv", "text/csv")
    st.subheader(f"Generate Chart ({db_type})")
    chart_type = st.selectbox(f"Select Chart Type ({db_type})", ["Bar Chart", "Pie Chart", "Line Chart", "Histogram"])
    chart_column = st.selectbox(f"Select Column for Chart ({db_type})", df.columns.tolist())
    if st.button(f"Generate Chart {db_type}"):
        fig = generate_chart(df, chart_type, chart_column)
        if fig:
            st.pyplot(fig)

# MongoDB Only
if mode == "MongoDB Only":
    st.header("MongoDB")

    if 'mongo_collections' in st.session_state:
        # Insert Form
        with st.form("Insert MongoDB Document"):
            st.subheader("Insert New Document")
            collection_name = st.selectbox("Select Collection", st.session_state['mongo_collections'])
            raw_json = st.text_area("Insert Document (JSON format)", '{"field1": "value1", "field2": "value2"}')
            submit_insert = st.form_submit_button("Insert Document")
            if submit_insert:
                try:
                    document = json.loads(raw_json)
                    st.session_state['mongo_db'][collection_name].insert_one(document)
                    st.success("Document inserted successfully!")
                except Exception as e:
                    st.error(f"Insert failed: {e}")

        # Update Form
        with st.form("Update MongoDB Document"):
            st.subheader("Update Document")
            collection_name = st.selectbox("Select Collection to Update", st.session_state['mongo_collections'], key="update")
            filter_query = st.text_area("Filter Query (JSON format)", '{"field1": "value1"}')
            update_query = st.text_area("Update Query (JSON format)", '{"$set": {"field2": "newvalue"}}')
            submit_update = st.form_submit_button("Update Document")
            if submit_update:
                try:
                    filter_ = json.loads(filter_query)
                    update_ = json.loads(update_query)
                    result = st.session_state['mongo_db'][collection_name].update_many(filter_, update_)
                    st.success(f"Updated {result.modified_count} documents!")
                except Exception as e:
                    st.error(f"Update failed: {e}")

        # Delete Form
        with st.form("Delete MongoDB Document"):
            st.subheader("Delete Document")
            collection_name = st.selectbox("Select Collection to Delete From", st.session_state['mongo_collections'], key="delete")
            filter_query = st.text_area("Delete Filter Query (JSON format)", '{"field1": "value1"}')
            submit_delete = st.form_submit_button("Delete Document")
            if submit_delete:
                try:
                    filter_ = json.loads(filter_query)
                    result = st.session_state['mongo_db'][collection_name].delete_many(filter_)
                    st.success(f"Deleted {result.deleted_count} documents!")
                except Exception as e:
                    st.error(f"Delete failed: {e}")

        if 'mongo_db' in st.session_state:
            mongo_collection = st.selectbox("Select MongoDB Collection", st.session_state['mongo_collections'])
            df_mongo = mongodb_fetch_data(st.session_state['mongo_db'], mongo_collection, limit=5)

            if not df_mongo.empty:
                mongo_columns = df_mongo.columns.tolist()
                selected_mongo_columns = st.multiselect("Select Columns to View", mongo_columns, default=mongo_columns)

                if st.button("Fetch MongoDB Data"):
                    st.session_state['current_mongo_collection'] = mongo_collection
                    st.session_state['current_mongo_columns'] = selected_mongo_columns
                    st.success(f"Fetched data from `{mongo_collection}`")

            if 'current_mongo_collection' in st.session_state and 'current_mongo_columns' in st.session_state:
                df = mongodb_fetch_data(
                    st.session_state['mongo_db'],
                    st.session_state['current_mongo_collection'],
                    st.session_state['current_mongo_columns'],
                    limit=10000
                )

                # Aggregation
                st.subheader("Aggregation (MongoDB Native Query)")
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
                if numeric_cols:
                    agg_func = st.selectbox("Aggregation Function", ["sum", "avg", "min", "max", "count"])
                    agg_col = st.selectbox("Field to Aggregate", numeric_cols)

                    if st.button("Aggregate MongoDB"):
                        result = aggregate_data_mongo(st.session_state['mongo_db'], mongo_collection, agg_func, agg_col)
                        st.success(f"{agg_func.upper()} of `{agg_col}`: {result}")

                st.subheader("Group By Aggregation")
                group_col = st.selectbox("Group By Field", df.columns)
                agg_col2 = st.selectbox("Aggregation Field (Group By)", numeric_cols)
                agg_func2 = st.selectbox("Aggregation Function (Group By)", ["sum", "avg", "min", "max", "count"])
                if st.button("Group By Aggregate MongoDB"):
                    result = group_by_mongo(st.session_state['mongo_db'], mongo_collection, group_col, agg_func2, agg_col2)
                    group_df = pd.DataFrame(result)
                    st.dataframe(group_df)

                st.subheader("Distinct Values")
                distinct_col = st.selectbox("Field for Distinct Values", df.columns)
                if st.button("Show Distinct Values MongoDB"):
                    result = distinct_mongo(st.session_state['mongo_db'], mongo_collection, distinct_col)
                    st.write(result)

                st.subheader("Top-N Analysis (MongoDB)")
                topn_col = st.selectbox("Field for Top-N", df.columns)
                n = st.number_input("N (Top-N)", 1, 100)
                if st.button("Show Top-N MongoDB"):
                    result = top_n_mongo(st.session_state['mongo_db'], mongo_collection, topn_col, int(n))
                    if result:
                        topn_df = pd.DataFrame(result)
                        topn_df.columns = ["Field", "Count"]
                        st.dataframe(topn_df)
                    else:
                        st.warning("No data found for Top-N analysis.")
                st.subheader("View Data")
                data_explorer(df, "MongoDB")
    else:
        st.warning("Please connect to MongoDB first.")

# Cassandra Only
elif mode == "Cassandra Only":
    st.header("Cassandra")
    if 'cassandra_session' in st.session_state:

        # Insert Form
        with st.form("Insert Cassandra Data"):
            st.subheader("Insert Row")
            table_name = st.selectbox("Select Table", st.session_state['cassandra_tables'], key="insert_cassandra")
            columns = st.text_input("Columns (comma separated)", "id, name, age")
            values = st.text_input("Values (comma separated)", "uuid(), 'Alice', 30")
            submit_insert = st.form_submit_button("Insert Data")
            
            if submit_insert:
                try:
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                    st.session_state['cassandra_session'].execute(query)
                    st.success(f"Inserted data into `{table_name}` successfully!")
                except Exception as e:
                    st.error(f"Insert failed: {e}")

        # Update Form
        with st.form("Update Cassandra Data"):
            st.subheader("Update Row")
            table_name = st.selectbox("Select Table to Update", st.session_state['cassandra_tables'], key="update_cassandra")
            set_clause = st.text_input("Set Clause", "name = 'Bob'")
            where_clause = st.text_input("Where Clause", "id = uuid()")
            submit_update = st.form_submit_button("Update Data")
            
            if submit_update:
                try:
                    query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
                    st.session_state['cassandra_session'].execute(query)
                    st.success(f"Updated data in `{table_name}` successfully!")
                except Exception as e:
                    st.error(f"Update failed: {e}")

        # Delete Form
        with st.form("Delete Cassandra Data"):
            st.subheader("Delete Row")
            table_name = st.selectbox("Select Table to Delete From", st.session_state['cassandra_tables'], key="delete_cassandra")
            where_clause = st.text_input("Where Clause for Delete", "id = uuid()")
            submit_delete = st.form_submit_button("Delete Data")
            if submit_delete:
                try:
                    query = f"DELETE FROM {table_name} WHERE {where_clause};"
                    st.session_state['cassandra_session'].execute(query)
                    st.success(f"Deleted data from `{table_name}` successfully!")
                except Exception as e:
                    st.error(f"Delete failed: {e}")

        cassandra_table = st.selectbox("Select Cassandra Table", st.session_state['cassandra_tables'])
        df_cassandra = cassandra_fetch_data(st.session_state['cassandra_session'], cassandra_table, limit=5)

        if not df_cassandra.empty:
            cassandra_columns = df_cassandra.columns.tolist()
            selected_cassandra_columns = st.multiselect("Select Cassandra Columns", cassandra_columns, default=cassandra_columns)

            if st.button("Fetch Cassandra Data"):
                st.session_state['current_cassandra_table'] = cassandra_table
                st.session_state['current_cassandra_columns'] = selected_cassandra_columns
                st.success(f"Fetched data from `{cassandra_table}`")

        if 'current_cassandra_table' in st.session_state and 'current_cassandra_columns' in st.session_state:
            df = cassandra_fetch_data(
                st.session_state['cassandra_session'],
                st.session_state['current_cassandra_table'],
                st.session_state['current_cassandra_columns'],
                limit=10000
            )

            # Aggregation
            st.subheader("Aggregation (Cassandra Native Query)")
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
            if numeric_cols:
                agg_func = st.selectbox("Aggregation Function (Cassandra)", ["sum", "min", "max"])
                agg_col = st.selectbox("Field to Aggregate", numeric_cols)

                if st.button("Aggregate Cassandra"):
                    result = aggregate_data_cassandra(st.session_state['cassandra_session'], cassandra_table, agg_func, agg_col)
                    st.success(f"{agg_func.upper()} of `{agg_col}`: {result}")
            # View Data & Chart
            data_explorer(df, "Cassandra")
    else:
        st.warning("Please connect to Cassandra first.")

# Merge MongoDB + Cassandra
elif mode == "Merge MongoDB + Cassandra":
    st.header("Merge MongoDB and Cassandra")
    if 'mongo_db' in st.session_state and 'cassandra_session' in st.session_state:
        st.subheader("MongoDB Settings")
        mongo_collection = st.selectbox("Select MongoDB Collection", st.session_state['mongo_collections'])
        df_mongo = mongodb_fetch_data(st.session_state['mongo_db'], mongo_collection, limit=5)

        if not df_mongo.empty:
            mongo_columns = df_mongo.columns.tolist()
            selected_mongo_columns = st.multiselect("Select MongoDB Columns", mongo_columns, default=mongo_columns)

        st.subheader("Cassandra Settings")
        cassandra_table = st.selectbox("Select Cassandra Table", st.session_state['cassandra_tables'])
        df_cassandra = cassandra_fetch_data(st.session_state['cassandra_session'], cassandra_table, limit=5)

        if not df_cassandra.empty:
            cassandra_columns = df_cassandra.columns.tolist()
            selected_cassandra_columns = st.multiselect("Select Cassandra Columns", cassandra_columns, default=cassandra_columns)

        st.subheader("Merge Settings")
        if selected_mongo_columns and selected_cassandra_columns:
            common_cols = list(set(selected_mongo_columns) & set(selected_cassandra_columns))
            if common_cols:
                join_key = st.selectbox("Select Join Key", common_cols)
                join_type = st.selectbox("Select Join Type", ["inner", "left", "outer"])

                if st.button("Fetch and Merge Data"):
                    try:
                        mongo_data = mongodb_fetch_data(
                            st.session_state['mongo_db'],
                            mongo_collection,
                            selected_mongo_columns,
                            limit=10000
                        )

                        if '_id' in mongo_data.columns:
                            mongo_data = mongo_data.drop(columns=['_id'])

                        cassandra_data = cassandra_fetch_data(
                            st.session_state['cassandra_session'],
                            cassandra_table,
                            selected_cassandra_columns,
                            limit=10000
                        )

                        mongo_data[join_key] = mongo_data[join_key].astype(str)
                        cassandra_data[join_key] = cassandra_data[join_key].astype(str)

                        merged_df = pd.merge(mongo_data, cassandra_data, on=join_key, how=join_type)

                        st.session_state['merged_df'] = merged_df
                        st.success(f"Merged {len(merged_df)} rows with `{join_type.upper()}` join on `{join_key}`")
                    except Exception as e:
                        st.error(f"Merge failed: {e}")

        if 'merged_df' in st.session_state:
            data_explorer(st.session_state['merged_df'], "Merged Data")
    else:
        st.warning("Please connect to both MongoDB and Cassandra first.")

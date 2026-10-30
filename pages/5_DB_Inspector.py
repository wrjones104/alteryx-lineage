import streamlit as st
import pandas as pd
import database_manager as db

if 'workspace' not in st.session_state:
    st.session_state.workspace = None

st.set_page_config(layout="wide", page_title="DB Inspector")
st.header("🕵️‍♂️ DB Inspector")

if not st.session_state.get('workspace'):
    st.warning("⬅️ Please select a workspace in the sidebar to begin.")
    st.stop()

conn = db.create_connection()
if conn:
    try:
        st.write(f"Inspecting database tables.")
        table_to_inspect = st.selectbox("Select a table to inspect:", ["workspaces", "workflows", "tools", "tool_fields", "connections"])
        if table_to_inspect:
            query = f"SELECT * FROM {table_to_inspect}"
            df = pd.read_sql_query(query, conn)
            st.write(f"Contents of `{table_to_inspect}` table:")
            st.dataframe(df, use_container_width=True)
    finally:
        conn.close()
else:
    st.error("Could not connect to the database.")
import streamlit as st
import pandas as pd
import reports

if 'workspace' not in st.session_state:
    st.session_state.workspace = None

st.set_page_config(layout="wide", page_title="Raw I/O Log")
st.header("🗂️ Raw I/O Log")

if not st.session_state.get('workspace'):
    st.warning("⬅️ Please select a workspace in the sidebar to begin.")
    st.stop()

st.write(f"Displaying raw I/O for workspace: **'{st.session_state.workspace}'**")
raw_io_log_data = reports.get_raw_io_list(st.session_state.workspace)

if not raw_io_log_data:
    st.info("No workflows have been analyzed in this workspace yet. Add data via the 'Manage Data' page.")
else:
    df_log = pd.DataFrame(raw_io_log_data)
    cols = ['workflow_name', 'io_type', 'tool_id', 'plugin', 'source_detail', 'workflow_db_id', 'tool_db_id']
    existing_cols = [c for c in cols if c in df_log.columns]
    st.dataframe(df_log[existing_cols], use_container_width=True)
import streamlit as st
import pandas as pd
import database_manager as db
import reports
import shared  # Import the shared components

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Impact Analysis")

# --- Session State Initialization ---
if 'workspace' not in st.session_state:
    st.session_state.workspace = None

# --- Sidebar ---
shared.create_sidebar()

# --- Main Page Content ---
st.header("💥 Impact Analysis")

if not st.session_state.workspace:
    st.warning("⬅️ Please select a workspace in the sidebar to begin.")
    st.stop()

st.write(f"Displaying reports for workspace: **{st.session_state.workspace}**")
st.markdown("---")

# --- Report Logic ---
raw_io = reports.get_raw_io_list(st.session_state.workspace)
st.subheader("Blast Radius Report")
view_by = st.radio("Group report by:", ("Data Source", "Workflow"), horizontal=True, label_visibility="collapsed")

if view_by == 'Data Source':
    st.write("This report shows which **data sources** have the biggest downstream impact.")
else:
    st.write("This report shows which **workflows** have the highest total number of downstream consumers.")

impact_df = reports.generate_impact_report(raw_io, view_by)

if impact_df.empty:
    st.info("No data sources found. Upload or process workflows on the 'Home' page.")
else:
    st.dataframe(impact_df, use_container_width=True)

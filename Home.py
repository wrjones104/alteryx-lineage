import streamlit as st
import pandas as pd
from parser import parse_workflow
import database_manager as db
import server_client
import gc
import time
import os
import shared

# --- Page Config and DB Setup ---
st.set_page_config(layout="wide", page_title="Manage Data")
db.create_tables()

# --- Session State Initialization ---
if 'workspace' not in st.session_state:
    st.session_state.workspace = None
if 'server_url' not in st.session_state: st.session_state.server_url = ""
if 'client_id' not in st.session_state: st.session_state.client_id = ""
if 'client_secret' not in st.session_state: st.session_state.client_secret = ""

# --- Sidebar ---
shared.create_sidebar()

# --- Main Page Content ---
st.title("Alteryx Workflow Lineage Tool 🔗")
st.header("⚙️ Manage Data")
st.markdown("---")

# --- Create New Workspace ---
with st.expander("Create a New Workspace", expanded=not st.session_state.workspace):
    new_workspace_name = st.text_input("New workspace name:")
    workspace_list = db.get_all_workspaces()
    if st.button("Create Workspace"):
        if new_workspace_name and new_workspace_name not in workspace_list:
            conn = db.create_connection()
            if conn:
                db._add_workspace(conn, new_workspace_name)
                conn.close()
                db.get_all_workspaces.clear()
                st.session_state.workspace = new_workspace_name
                st.success(f"Workspace '{new_workspace_name}' created and selected!")
                time.sleep(1)
                st.rerun()
        elif new_workspace_name in workspace_list:
            st.error("Workspace name already exists.")
        else:
            st.error("Workspace name cannot be empty.")

# --- Data Management (conditional on workspace selection) ---
if not st.session_state.workspace:
    st.info("⬅️ Create or select a workspace to begin adding data.")
    st.stop()

st.write(f"Managing data for workspace: **{st.session_state.workspace}**")

# --- Server Connection & Download ---
with st.expander("🔗 Connect to Alteryx Server to Download Workflows"):
    saved_connections = db.load_connections()
    if saved_connections:
        saved_conn_name = st.selectbox("Load Saved Connection", options=[""] + list(saved_connections.keys()))
        if saved_conn_name:
            conn_details = saved_connections[saved_conn_name]
            st.session_state.server_url = conn_details['url']
            st.session_state.client_id = conn_details['client_id']
            st.session_state.client_secret = conn_details['client_secret']

    st.text_input("Server URL", key="server_url")
    st.text_input("Client ID", key="client_id")
    st.text_input("Client Secret", type="password", key="client_secret")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Connect to Server"):
            with st.spinner("Authenticating..."):
                s_key = server_client.get_session_key(st.session_state.server_url, st.session_state.client_id, st.session_state.client_secret)
                if s_key:
                    st.session_state.session_key = s_key
                    st.session_state.gallery_workflows = server_client.get_workflows(st.session_state.server_url, s_key)
                    if st.session_state.gallery_workflows:
                        df = pd.DataFrame(st.session_state.gallery_workflows)
                        df['Select'] = False
                        st.session_state.workflow_df = df
                    st.success("Connection successful!")
                else:
                    st.session_state.session_key = None
                    st.session_state.gallery_workflows = []
                    st.error("Authentication failed.")
    with col2:
        save_conn_name = st.text_input("Save connection as:")
        if st.button("Save Current Connection"):
            if save_conn_name and st.session_state.get('server_url') and st.session_state.get('client_id') and st.session_state.get('client_secret'):
                db.save_connection(save_conn_name, st.session_state.server_url, st.session_state.client_id, st.session_state.client_secret)
                st.success(f"Connection '{save_conn_name}' saved!")
            else:
                st.warning("Please provide a name and fill in all credential fields to save.")

# Server Workflow Download Table
if st.session_state.get('session_key') and 'workflow_df' in st.session_state and not st.session_state.workflow_df.empty:
    st.markdown("---")
    st.subheader("Download Workflows from Server")

    display_df = st.session_state.workflow_df.copy()
    
    search_term = st.text_input("Search workflows by name or owner:", placeholder="Filter list...")

    if search_term:
        search_term = search_term.lower()
        display_df = display_df[
            display_df['name'].str.lower().str.contains(search_term, na=False) |
            display_df['ownerName'].str.lower().str.contains(search_term, na=False)
        ]

    edited_df = st.data_editor(
        display_df,
        key='data_editor',
        column_config={
            "Select": st.column_config.CheckboxColumn("Select", default=False),
            "id": None, "name": "Workflow Name", "ownerName": "Owner",
            "dateCreated": "Date Created", "publishedVersionNumber": "Version"
        },
        disabled=["name", "ownerName", "dateCreated", "publishedVersionNumber"],
        hide_index=True, use_container_width=True
    )
    
    if not edited_df.equals(display_df):
        st.session_state.workflow_df.update(edited_df)
        st.rerun()

    d_col1, d_col2, d_col3, _ = st.columns([1.2, 1.4, 1, 4])
    with d_col1:
        if st.button("Select All Visible"):
            st.session_state.workflow_df.loc[display_df.index, 'Select'] = True
            st.rerun()
    with d_col2:
        if st.button("Deselect All Visible"):
            st.session_state.workflow_df.loc[display_df.index, 'Select'] = False
            st.rerun()
    with d_col3:
        selected_rows = st.session_state.workflow_df[st.session_state.workflow_df.Select]
        if st.button(f"Download ({len(selected_rows)})", disabled=selected_rows.empty):
            st.session_state.workflows_to_download = selected_rows['id'].tolist()
            st.session_state.cancel_download = False
            st.rerun()

if st.session_state.get('workflows_to_download'):
    st.info("Processing workflows from server...")
    if st.button("Cancel Operation"):
        st.session_state.cancel_download = True
        st.rerun()
    
    workflow_ids = st.session_state.workflows_to_download
    total_files = len(workflow_ids)
    progress_bar = st.progress(0, "Starting...")
    temp_dir = "temp_downloads"
    try:
        for i, wf_id in enumerate(workflow_ids):
            if st.session_state.get('cancel_download'):
                st.warning("Operation cancelled by user.")
                break
            workflow_name = st.session_state.workflow_df.loc[st.session_state.workflow_df['id'] == wf_id, 'name'].iloc[0]
            progress_bar.progress(i / total_files, text=f"({i+1}/{total_files}) Downloading: {workflow_name}...")
            yxmd_path = server_client.download_and_unpack_workflow(st.session_state.server_url, st.session_state.session_key, wf_id, temp_dir)
            if yxmd_path:
                progress_bar.progress((i + 0.5) / total_files, text=f"({i+1}/{total_files}) Parsing: {os.path.basename(yxmd_path)}...")
                tools_list, connections_list, _ = parse_workflow(yxmd_path)
                db.log_workflow_details(st.session_state.workspace, os.path.basename(yxmd_path), tools_list, connections_list)
                os.remove(yxmd_path)
                del tools_list, connections_list
                gc.collect()
        if not st.session_state.get('cancel_download'):
            progress_bar.progress(1.0, "Processing complete!")
            st.success("All selected workflows have been processed.")
            time.sleep(2)
    finally:
        st.session_state.workflows_to_download = []
        st.session_state.cancel_download = False
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# --- Local Upload Section ---
st.subheader("Upload Local Workflows")
uploaded_files = st.file_uploader("Upload .yxmd files from your computer", type="yxmd", accept_multiple_files=True, key='file_uploader_key')

if st.button("Process Local Files", disabled=not uploaded_files):
    total_files = len(uploaded_files)
    st.info("Processing local files...")
    progress_bar = st.progress(0)
    for i, uploaded_file in enumerate(uploaded_files):
        progress_text = f"Analyzing ({i+1}/{total_files}): {uploaded_file.name}"
        progress_bar.progress((i + 1) / total_files, text=progress_text)
        uploaded_file.seek(0)
        tools_list, connections_list, _ = parse_workflow(uploaded_file)
        db.log_workflow_details(st.session_state.workspace, uploaded_file.name, tools_list, connections_list)
        del tools_list, connections_list
        gc.collect()

    st.success("Local file processing complete!")
    st.cache_data.clear()
    time.sleep(1)
    st.rerun()

st.markdown("---")

# --- Manage Workspace Workflows ---
st.subheader("Manage Workspace Workflows")
workspace_workflows_df = db.get_workflows_in_workspace(st.session_state.workspace)

if workspace_workflows_df.empty:
    st.info("No workflows have been processed in this workspace yet.")
else:
    # Initialize a session state variable to track confirmation state
    if 'confirming_delete' not in st.session_state:
        st.session_state.confirming_delete = None

    for index, row in workspace_workflows_df.iterrows():
        wf_id = row['id']
        wf_name = row['workflow_name']
        last_parsed = row['last_parsed_at']
        
        col1, col2 = st.columns([4, 1])
        with col1:
            st.markdown(f"**{wf_name}** (Last Parsed: {last_parsed})")
        
        with col2:
            # If this workflow is the one pending confirmation, show the confirm button
            if st.session_state.confirming_delete == wf_id:
                if st.button("Confirm Delete", key=f"confirm_delete_{wf_id}", type="primary"):
                    if db.delete_workflow(wf_id):
                        st.success(f"Successfully deleted '{wf_name}'.")
                        st.session_state.confirming_delete = None # Reset state
                        st.cache_data.clear() # Clear caches to reflect change
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"Failed to delete '{wf_name}'.")
                        st.session_state.confirming_delete = None # Reset state
            else:
                if st.button("Delete", key=f"delete_{wf_id}"):
                    # Set this workflow as pending confirmation and rerun
                    st.session_state.confirming_delete = wf_id
                    st.rerun()
        
        # Add a separator line after each entry
        st.markdown("---")


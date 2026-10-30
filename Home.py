import streamlit as st
import pandas as pd
from parser import parse_workflow
import database_manager as db
import server_client
import server_sync
import gc
import time
import os
import shutil
import shared

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Manage Data")

# --- Session State Initialization ---
if 'workspace' not in st.session_state:
    st.session_state.workspace = None
if 'server_url' not in st.session_state: st.session_state.server_url = ""
if 'client_id' not in st.session_state: st.session_state.client_id = ""
if 'client_secret' not in st.session_state: st.session_state.client_secret = ""

def set_gallery(workflows, keep_selection=True):
    """
    Stores the server's workflow list (None if it couldn't be fetched) and rebuilds the download table
    from it, keeping the rows already ticked there.
    """
    st.session_state.gallery_workflows = workflows
    previous = st.session_state.get('workflow_df')
    selected = set(previous.loc[previous['Select'], 'id']) if keep_selection and previous is not None and not previous.empty else set()
    df = pd.DataFrame(workflows or [])
    if not df.empty:
        df['Select'] = df['id'].isin(selected)
    st.session_state.workflow_df = df

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
        def apply_saved_connection():
            # Fill the fields only when the selection changes, so typing over them (e.g. a secret
            # that wasn't saved) isn't undone on the next rerun
            conn_details = saved_connections.get(st.session_state.get('saved_conn_selector'))
            if conn_details:
                st.session_state.server_url = conn_details['url']
                st.session_state.client_id = conn_details['client_id']
                st.session_state.client_secret = conn_details['client_secret']
        st.selectbox("Load Saved Connection", options=[""] + list(saved_connections.keys()),
                     key='saved_conn_selector', on_change=apply_saved_connection)

    st.text_input("Server URL", key="server_url")
    st.text_input("Client ID", key="client_id")
    st.text_input("Client Secret", type="password", key="client_secret")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Connect to Server"):
            with st.spinner("Authenticating..."):
                target_url = st.session_state.server_url
                s_key = server_client.get_session_key(target_url, st.session_state.client_id, st.session_state.client_secret)
                if s_key:
                    st.session_state.session_key = s_key
                    # Everything after connecting uses this URL, not the text box, which can be edited
                    # (or changed by loading a saved connection) while this session key is still active
                    st.session_state.connected_server_url = target_url
                    set_gallery(server_client.get_workflows(target_url, s_key), keep_selection=False)
                    st.success("Connection successful!")
                else:
                    st.session_state.session_key = None
                    st.session_state.connected_server_url = None
                    set_gallery([], keep_selection=False)
                    st.error("Authentication failed.")
    with col2:
        save_conn_name = st.text_input("Save connection as:")
        if st.button("Save Current Connection"):
            if save_conn_name and st.session_state.get('server_url') and st.session_state.get('client_id') and st.session_state.get('client_secret'):
                saved, secret_stored = db.save_connection(save_conn_name, st.session_state.server_url, st.session_state.client_id, st.session_state.client_secret)
                if not saved:
                    st.error(f"Could not save connection '{save_conn_name}'.")
                elif secret_stored:
                    st.success(f"Connection '{save_conn_name}' saved! The client secret is kept in your system's credential store.")
                else:
                    st.warning(f"Connection '{save_conn_name}' saved without its client secret (no system credential store "
                               "is available). You'll need to enter the secret each time.")
            else:
                st.warning("Please provide a name and fill in all credential fields to save.")

# Workspace Update Check: workflows downloaded earlier whose published version has changed since
connected_url = st.session_state.get('connected_server_url')
if st.session_state.get('session_key') and connected_url and not st.session_state.get('workflows_to_download'):
    st.markdown("---")
    st.subheader("Check Workspace for Server Updates")
    st.caption(f"Compared with {connected_url}")
    status_df = server_sync.compare_with_server(db.get_server_workflows_in_workspace(st.session_state.workspace),
                                                st.session_state.get('gallery_workflows'), connected_url)
    if st.session_state.get('gallery_workflows') is None:
        st.warning("Couldn't get the workflow list from the server. The session may have expired: reconnect, "
                   "or try Re-check Server.")
    elif status_df.empty:
        st.info("No workflows in this workspace have been downloaded from a server yet.")
    else:
        counts = status_df['Status'].value_counts()
        st.caption(" · ".join(f"{status}: {counts[status]}" for status in counts.index))
        st.dataframe(status_df.drop(columns=['server_workflow_id']), hide_index=True, use_container_width=True)
        if counts.get(server_sync.STATUS_MISSING):
            st.caption(f"'{server_sync.STATUS_MISSING}': deleted from the server, or not shared with this connection's account.")

        downloadable = status_df[status_df['Status'].isin(server_sync.DOWNLOADABLE)]
        id_by_name = dict(zip(downloadable['Workflow'], downloadable['server_workflow_id']))
        to_update = st.multiselect(
            "Workflows to update", options=list(id_by_name),
            default=list(downloadable.loc[downloadable['Status'] == server_sync.STATUS_UPDATE, 'Workflow']),
            help=f"'{server_sync.STATUS_UNKNOWN}' rows were downloaded before versions were recorded; re-download "
                 f"them to record the current version. '{server_sync.STATUS_LOCAL}' rows hold a file uploaded from "
                 "a computer, which may have unpublished edits: downloading replaces it with the server's version.")
        if st.button(f"Download Updates ({len(to_update)})", disabled=not to_update, type="primary"):
            st.session_state.workflows_to_download = [id_by_name[name] for name in to_update]
            st.session_state.cancel_download = False
            st.rerun()
    if st.button("Re-check Server"):
        # Picks up versions published since you connected
        set_gallery(server_client.get_workflows(connected_url, st.session_state.session_key))
        st.rerun()

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
    failed_files, renamed_files = [], []
    connected_url = st.session_state.get('connected_server_url')
    # The package endpoint serves the version published now, so the versions to record are read now too,
    # not from when you connected. The cached list is kept if the server can't be reached.
    latest = server_client.get_workflows(connected_url, st.session_state.session_key) if connected_url else None
    if latest:
        set_gallery(latest)
    # Read from the server's own list: in workflow_df, one missing version turns every version into a float ('3.0')
    gallery_by_id = {str(wf['id']): wf for wf in st.session_state.get('gallery_workflows') or []}
    try:
        for i, wf_id in enumerate(workflow_ids):
            if st.session_state.get('cancel_download'):
                st.warning("Operation cancelled by user.")
                break
            server_row = gallery_by_id.get(str(wf_id))
            if server_row is None or not connected_url:
                failed_files.append(str(wf_id) if server_row is None else server_row['name'])
                continue
            workflow_name = server_row['name']
            progress_bar.progress(i / total_files, text=f"({i+1}/{total_files}) Downloading: {workflow_name}...")
            yxmd_path = server_client.download_and_unpack_workflow(connected_url, st.session_state.session_key, wf_id, temp_dir)
            if yxmd_path:
                try:
                    progress_bar.progress((i + 0.5) / total_files, text=f"({i+1}/{total_files}) Parsing: {os.path.basename(yxmd_path)}...")
                    tools_list, connections_list, workflow_meta = parse_workflow(yxmd_path)
                    if tools_list is None:
                        failed_files.append(workflow_name)
                    else:
                        workflow_meta.update(server_sync.download_meta(server_row, connected_url))
                        file_name = os.path.basename(yxmd_path)
                        stored_name = db.log_workflow_details(st.session_state.workspace, file_name, tools_list, connections_list,
                                                              workflow_meta, server_workflow_id=wf_id)
                        if stored_name is None:
                            failed_files.append(workflow_name)
                        elif stored_name != file_name:
                            renamed_files.append(f"'{workflow_name}' saved as '{stored_name}'")
                    del tools_list, connections_list
                    gc.collect()
                finally:
                    # Each download is unpacked into its own folder
                    shutil.rmtree(os.path.dirname(yxmd_path), ignore_errors=True)
            else:
                failed_files.append(workflow_name)
        if failed_files:
            st.error(f"Could not download or parse {len(failed_files)} workflow(s): {', '.join(failed_files)}")
        if renamed_files:
            st.warning("Another workflow in this workspace already uses the same file name, so these were saved "
                       f"under a unique name: {'; '.join(renamed_files)}")
        if failed_files or renamed_files:
            time.sleep(5)
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
uploaded_files = st.file_uploader("Upload workflows (.yxmd) or analytic apps (.yxwz) from your computer", type=["yxmd", "yxwz"], accept_multiple_files=True, key='file_uploader_key')

if st.button("Process Local Files", disabled=not uploaded_files):
    total_files = len(uploaded_files)
    st.info("Processing local files...")
    progress_bar = st.progress(0)
    failed_files = []
    for i, uploaded_file in enumerate(uploaded_files):
        progress_text = f"Analyzing ({i+1}/{total_files}): {uploaded_file.name}"
        progress_bar.progress((i + 1) / total_files, text=progress_text)
        uploaded_file.seek(0)
        tools_list, connections_list, workflow_meta = parse_workflow(uploaded_file)
        if tools_list is None:
            # Don't overwrite previously parsed data with an empty result
            failed_files.append(uploaded_file.name)
            continue
        workflow_meta['source'] = 'Local Upload'
        # If this replaces a server download, its contents no longer match any server version.
        # The server ID is kept, so the update check can still offer the server's version.
        workflow_meta.update({'server_version': None, 'source_ref': None})
        if db.log_workflow_details(st.session_state.workspace, uploaded_file.name, tools_list, connections_list, workflow_meta) is None:
            failed_files.append(uploaded_file.name)
        del tools_list, connections_list
        gc.collect()

    st.cache_data.clear()
    if failed_files:
        st.error(f"Could not parse {len(failed_files)} file(s): {', '.join(failed_files)}. They were skipped.")
    else:
        st.success("Local file processing complete!")
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


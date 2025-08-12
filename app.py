import streamlit as st
import pandas as pd
from parser import parse_workflow
import database_manager as db
import reports
import tracer
import server_client
import os

db.create_tables()

# Initialize session state variables
if 'files_to_process' not in st.session_state: st.session_state.files_to_process = []
if 'active_tab' not in st.session_state: st.session_state.active_tab = "💥 Impact Analysis"
if 'access_token' not in st.session_state: st.session_state.access_token = None
if 'gallery_workflows' not in st.session_state: st.session_state.gallery_workflows = []
if 'workflow_df' not in st.session_state: st.session_state.workflow_df = pd.DataFrame()

def queue_files_for_processing():
    st.session_state.files_to_process = st.session_state.file_uploader_key

st.set_page_config(layout="wide", page_title="Alteryx Lineage Tool")

# --- Sidebar ---
# ... (Sidebar logic is unchanged)
st.sidebar.title("Workspace")
workspace_list = db.get_all_workspaces()
if 'workspace' not in st.session_state: st.session_state.workspace = None
selected_workspace = st.sidebar.selectbox("Choose an existing workspace", options=[""] + workspace_list, key='workspace_selector')
if selected_workspace: st.session_state.workspace = selected_workspace
st.sidebar.write("---")
new_workspace_name = st.sidebar.text_input("Or, Create a New Workspace:")
if st.sidebar.button("Create Workspace"):
    conn = db.create_connection()
    if conn and new_workspace_name and new_workspace_name not in workspace_list:
        db._add_workspace(conn, new_workspace_name)
        conn.close()
        db.get_all_workspaces.clear()
        st.session_state.workspace = new_workspace_name
        st.sidebar.success(f"Workspace '{new_workspace_name}' created and selected.")
        st.rerun()
    elif not (new_workspace_name and new_workspace_name not in workspace_list): st.sidebar.error("Name cannot be empty or already exist.")
    else: st.sidebar.error("Could not connect to database.")

st.sidebar.title("Alteryx Server")
with st.sidebar.expander("Connect to Server", expanded=True):
    if not st.session_state.workspace:
        st.info("Please select or create a workspace first.")
    else:
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
        if st.button("Connect"):
            with st.spinner("Authenticating..."):
                token = server_client.get_access_token(st.session_state.server_url, st.session_state.client_id, st.session_state.client_secret)
                if token:
                    st.session_state.access_token = token
                    st.session_state.gallery_workflows = server_client.get_workflows(st.session_state.server_url, token)
                    if st.session_state.gallery_workflows:
                        df = pd.DataFrame(st.session_state.gallery_workflows)
                        df['Select'] = False
                        st.session_state.workflow_df = df
                    st.success("Connection successful!")
                else:
                    st.session_state.access_token = None
                    st.session_state.gallery_workflows = []
                    st.error("Authentication failed.")
        st.write("---")
        st.subheader("Save Current Connection")
        save_conn_name = st.text_input("Save connection as:")
        if st.button("Save"):
            if save_conn_name and st.session_state.server_url and st.session_state.client_id and st.session_state.client_secret:
                db.save_connection(save_conn_name, st.session_state.server_url, st.session_state.client_id, st.session_state.client_secret)
                st.success(f"Connection '{save_conn_name}' saved!")
            else:
                st.warning("Please provide a name and fill in all credential fields to save.")


# --- Main App Logic ---
st.title("Alteryx Workflow Lineage Tool 🔗")

if not st.session_state.workspace:
    st.info("⬅️ Please select or create a workspace in the sidebar to begin.")
else:
    st.header(f"📍 Current Workspace: **{st.session_state.workspace}**")
    
    if st.session_state.access_token and not st.session_state.workflow_df.empty:
        st.subheader("Download Workflows from Server")
        
        display_df = st.session_state.workflow_df.copy()

        owners = sorted(display_df['ownerId'].unique())
        selected_owners = st.multiselect("Filter by Owner:", options=owners)
        
        if selected_owners:
            display_df = display_df[display_df['ownerId'].isin(selected_owners)]

        edited_df = st.data_editor(
            display_df,
            key='data_editor',
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False),
                "id": None, "name": "Workflow Name", "ownerId": "Owner ID",
                "dateCreated": "Date Created", "publishedVersionNumber": "Version"
            },
            disabled=["name", "ownerId", "dateCreated", "publishedVersionNumber"],
            hide_index=True, use_container_width=True
        )
        
        if st.session_state['data_editor']['edited_rows']:
            selections = st.session_state['data_editor']['edited_rows']
            for index, value in selections.items():
                st.session_state.workflow_df.loc[index, 'Select'] = value['Select']

        col1, col2, col3, _ = st.columns([1, 1, 1, 4])
        
        with col1:
            if st.button("Select All Visible"):
                visible_ids = display_df['id'].tolist()
                st.session_state.workflow_df.loc[st.session_state.workflow_df['id'].isin(visible_ids), 'Select'] = True
                st.rerun()
        with col2:
            if st.button("Deselect All Visible"):
                visible_ids = display_df['id'].tolist()
                st.session_state.workflow_df.loc[st.session_state.workflow_df['id'].isin(visible_ids), 'Select'] = False
                st.rerun()
        with col3:
            selected_rows = st.session_state.workflow_df[st.session_state.workflow_df.Select]
            if st.button("Download & Process", disabled=selected_rows.empty):
                st.session_state.workflows_to_download = selected_rows['id'].tolist()
                st.rerun()

    st.markdown("---")
    st.subheader("Upload Local Workflows")
    st.file_uploader("Upload local workflow files (.yxmd)", type="yxmd", accept_multiple_files=True, key='file_uploader_key', on_change=queue_files_for_processing)

    if st.session_state.get('files_to_process') or st.session_state.get('workflows_to_download'):
        # Processing logic for local files
        if st.session_state.get('files_to_process'):
            # ... (omitting unchanged logic for brevity)
            st.session_state.files_to_process = []
        
        # Processing logic for server workflows
        if st.session_state.get('workflows_to_download'):
            workflow_ids = st.session_state.workflows_to_download
            total_files = len(workflow_ids)
            progress_bar = st.progress(0, text="Downloading and processing from server...")
            temp_dir = "temp_downloads"

            for i, wf_id in enumerate(workflow_ids):
                progress_text = f"Processing workflow {i+1}/{total_files}"
                progress_bar.progress((i + 1) / total_files, text=progress_text)
                
                yxmd_path = server_client.download_and_unpack_workflow(
                    st.session_state.server_url, st.session_state.access_token, wf_id, temp_dir
                )
                
                if yxmd_path:
                    workflow_name = os.path.basename(yxmd_path)
                    tools_list, connections_list, _ = parse_workflow(yxmd_path)
                    db.log_workflow_details(st.session_state.workspace, workflow_name, tools_list, connections_list)
                    # --- FIX: Comment out this line to keep the file for debugging ---
                    # os.remove(yxmd_path)
            
            st.session_state.workflows_to_download = []
            st.session_state.workflow_df = pd.DataFrame()

        st.success("Processing complete!")
        st.rerun()

    st.radio("Select a view:", options=["💥 Impact Analysis", "🗺️ Field Lineage Explorer", "🗂️ Raw I/O Log", "🕵️‍♂️ DB Inspector"], key='active_tab', horizontal=True)
    st.markdown("---")
    
    # ... (all tab logic is unchanged)

    # ... (all tab logic is unchanged)
    if st.session_state.active_tab == "💥 Impact Analysis":
        raw_io = reports.get_raw_io_list(st.session_state.workspace)
        st.subheader("Blast Radius Report")
        view_by = st.radio("Group report by:", ("Data Source", "Workflow"), horizontal=True, label_visibility="collapsed")
        if view_by == 'Data Source': st.write("This report shows which **data sources** have the biggest downstream impact.")
        else: st.write("This report shows which **workflows** have the highest total number of downstream consumers.")
        impact_df = reports.generate_impact_report(raw_io, view_by)
        if impact_df.empty: st.info("No data sources found. Upload workflows to see the report.")
        else: st.dataframe(impact_df, use_container_width=True)
        
    if st.session_state.active_tab == "🗺️ Field Lineage Explorer":
        conn = db.create_connection()
        try:
            st.subheader("Trace a Field's Lineage")
            workflows_in_db = pd.read_sql_query("SELECT w.id, w.workflow_name FROM workflows w JOIN workspaces ws ON w.workspace_id = ws.id WHERE ws.name = ?", conn, params=(st.session_state.workspace,))
            if workflows_in_db.empty:
                st.info("No workflows have been analyzed in this workspace yet.")
            else:
                workflow_options = pd.Series(workflows_in_db.id.values, index=workflows_in_db.workflow_name).to_dict()
                selected_workflow_name = st.selectbox("1. Select a workflow to explore:", options=list(workflow_options.keys()))
                if selected_workflow_name:
                    workflow_db_id = workflow_options[selected_workflow_name]
                    tools_in_workflow = pd.read_sql_query("SELECT id, tool_id_xml, plugin FROM tools WHERE workflow_id = ?", conn, params=(workflow_db_id,))
                    tool_options = pd.Series(tools_in_workflow.id.values, index=tools_in_workflow.tool_id_xml + " (" + tools_in_workflow.plugin.str.split('.').str[-1] + ")").to_dict()
                    selected_tool_display = st.selectbox("2. Select a starting tool:", options=[""] + list(tool_options.keys()))
                    if selected_tool_display:
                        tool_db_id = tool_options[selected_tool_display]
                        fields_in_tool = pd.read_sql_query("SELECT field_name FROM tool_fields WHERE tool_id = ? ORDER BY field_name", conn, params=(tool_db_id,))
                        if fields_in_tool.empty:
                            st.warning("This tool has no output fields to trace.")
                        else:
                            selected_field = st.selectbox("3. Select a field to trace:", options=[""] + list(fields_in_tool['field_name']))
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button("Trace Field to Origin (Upstream)"):
                                    if selected_field:
                                        with st.spinner("Tracing field backward..."):
                                            lineage_df = tracer.trace_field_lineage(workflow_db_id, tool_db_id, selected_field)
                                            st.write(f"Upstream lineage for **{selected_field}**:")
                                            st.dataframe(lineage_df, use_container_width=True)
                                    else: st.warning("Please select a field first.")
                            with col2:
                                if st.button("Find Downstream Impact"):
                                    if selected_field:
                                        with st.spinner("Tracing field forward..."):
                                            endpoints_df, log_df = tracer.trace_field_downstream(workflow_db_id, tool_db_id, selected_field)
                                            st.write(f"Downstream impact for **{selected_field}**:")
                                            if endpoints_df.empty: st.info("This field is not used in any final outputs.")
                                            else: st.dataframe(endpoints_df, use_container_width=True)
                                            with st.expander("Show Trace Log"):
                                                st.dataframe(log_df, use_container_width=True)
                                    else: st.warning("Please select a field first.")
        finally:
            if conn: conn.close()
            
    if st.session_state.active_tab == "🗂️ Raw I/O Log":
        raw_io_log = reports.get_raw_io_list(st.session_state.workspace)
        st.subheader("Full Data Log")
        st.write(f"This is the complete, unfiltered list of all inputs and outputs parsed from the workflows in the **'{st.session_state.workspace}'** workspace.")
        if not raw_io_log: st.info("No workflows have been analyzed in this workspace yet.")
        else: st.dataframe(pd.DataFrame(raw_io_log), use_container_width=True)
        
    if st.session_state.active_tab == "🕵️‍♂️ DB Inspector":
        conn = db.create_connection()
        try:
            st.subheader("Database Inspector")
            table_to_inspect = st.selectbox("Select a table to inspect:", ["workspaces", "workflows", "tools", "tool_fields", "connections"])
            if table_to_inspect:
                query = f"SELECT * FROM {table_to_inspect}"
                df = pd.read_sql_query(query, conn)
                st.write(f"Contents of `{table_to_inspect}` table:")
                st.dataframe(df, use_container_width=True)
        finally:
            if conn: conn.close()
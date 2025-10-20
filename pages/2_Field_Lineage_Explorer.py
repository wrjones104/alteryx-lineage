import streamlit as st
import pandas as pd
import os
import database_manager as db
import tracer
from parser import extract_io_tools
import shared  # Import the shared components

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Field Lineage")

# --- Session State Initialization ---
if 'workspace' not in st.session_state:
    st.session_state.workspace = None

# --- Sidebar ---
shared.create_sidebar()

# --- Main Page Content ---
st.header("🗺️ Field Lineage Explorer")

if not st.session_state.get('workspace'):
    st.warning("⬅️ Please select a workspace in the sidebar to begin.")
    st.stop()

st.write(f"Exploring lineage for workspace: **{st.session_state.workspace}**")
io_map = tracer.build_io_map(st.session_state.workspace)

# Use st.radio to create state-aware tabs
tab_options = ["Trace from a Source File", "Trace from a Tool", "Trace from an Output File", "Search by Field Name"]
active_tab = st.radio(
    "Lineage Explorer Mode",
    options=tab_options,
    horizontal=True,
    label_visibility="collapsed"
)

if active_tab == "Trace from a Source File":
    st.subheader("Trace a field forward from a source file")
    source_files = sorted([
        os.path.basename(k) for k, v in io_map.items()
        if v.get('consumers') and not v.get('producer')
        and not k.lower().strip().startswith('select')
        and '|||' not in k
        and not k.lower().strip().startswith('dynamic from template:')
    ])

    if not source_files:
        st.info("No source files found in this workspace to trace from.")
    else:
        selected_file_basename = st.selectbox("1. Select a source file:", options=[""] + source_files, key="source_file_selector")
        if selected_file_basename:
            selected_full_path = ""
            consumers = []
            for k, v in io_map.items():
                if os.path.basename(k) == selected_file_basename:
                    selected_full_path = k
                    consumers = v.get('consumers', [])
                    break
            if consumers:
                consumer_options = {
                    f"{c['workflow_name']} (Tool DB ID: {c['tool_db_id']})": i
                    for i, c in enumerate(consumers)
                }
                if len(consumers) > 1:
                    st.write("This file is used as an input in multiple places.")
                    selected_consumer_label = st.selectbox("2. Select the specific workflow/tool to start tracing from:", options=[""] + list(consumer_options.keys()))
                else:
                    selected_consumer_label = list(consumer_options.keys())[0]

                if selected_consumer_label:
                    consumer_index = consumer_options[selected_consumer_label]
                    selected_consumer = consumers[consumer_index]
                    workflow_db_id = selected_consumer['workflow_db_id']
                    tool_db_id = selected_consumer['tool_db_id']
                    conn = db.create_connection()
                    try:
                        fields_df = pd.read_sql_query("SELECT field_name FROM tool_fields WHERE tool_id = ? ORDER BY field_name", conn, params=(tool_db_id,))
                        if fields_df.empty:
                            st.warning("Could not retrieve the field schema for this input tool.")
                        else:
                            step_number = "3." if len(consumers) > 1 else "2."
                            selected_fields = st.multiselect(f"{step_number} Select field(s) to find their downstream impact:", options=fields_df['field_name'].tolist())
                            if st.button("Find Downstream Impact", key="trace_from_source"):
                                if not selected_fields:
                                    st.warning("Please select at least one field to trace.")
                                else:
                                    for field in selected_fields:
                                        with st.spinner(f"Tracing '{field}' forward..."):
                                            st.markdown(f"--- \n#### Downstream Impact for: **{field}**")
                                            endpoints_df = tracer.trace_downstream_end_to_end(st.session_state.workspace, workflow_db_id, tool_db_id, field, io_map)
                                            if endpoints_df.empty:
                                                st.info(f"The field '{field}' is not used in any final outputs from this starting point.")
                                            else:
                                                st.dataframe(endpoints_df, use_container_width=True)
                    finally:
                        if conn: conn.close()

elif active_tab == "Trace from a Tool":
    conn = db.create_connection()
    try:
        st.subheader("Trace a field starting from a specific tool")
        workflows_in_db = pd.read_sql_query("SELECT w.id, w.workflow_name FROM workflows w JOIN workspaces ws ON w.workspace_id = ws.id WHERE ws.name = ?", conn, params=(st.session_state.workspace,))
        if workflows_in_db.empty:
            st.info("No workflows have been analyzed in this workspace yet.")
        else:
            workflow_options = pd.Series(workflows_in_db.id.values, index=workflows_in_db.workflow_name).to_dict()
            selected_workflow_name = st.selectbox("1. Select a workflow to explore:", options=[""] + list(workflow_options.keys()))
            if selected_workflow_name:
                workflow_db_id = workflow_options[selected_workflow_name]
                tools_query = """
                    SELECT DISTINCT t.id as db_id, t.tool_id_xml, t.plugin, t.macro, t.config_xml, t.annotation 
                    FROM tools t 
                    WHERE t.workflow_id = ? AND t.plugin NOT LIKE '%Container%'
                    ORDER BY CAST(t.tool_id_xml AS INTEGER)
                """
                tools_df = pd.read_sql_query(tools_query, conn, params=(workflow_db_id,))
                tool_options = {}
                if not tools_df.empty:
                    parser_df = tools_df.rename(columns={'db_id': 'tool_db_id'})
                    parser_df['workflow_db_id'] = workflow_db_id
                    tools_list_for_parser = parser_df.to_dict('records')
                    inputs, outputs = extract_io_tools(tools_list_for_parser)
                    io_details = {}
                    db_id_to_xml_id = tools_df.set_index('db_id')['tool_id_xml'].to_dict()
                    for item in inputs + outputs:
                        xml_id = db_id_to_xml_id.get(item['tool_db_id'])
                        if xml_id: io_details[xml_id] = item['source_detail']
                    def create_label(row):
                        plugin_short = row['plugin'].split('.')[-1]
                        source_detail = io_details.get(row['tool_id_xml'])
                        if source_detail and source_detail != "Not Found":
                            filename = os.path.basename(source_detail)
                            return f"Tool {row['tool_id_xml']} ({plugin_short}) - {filename}"
                        return f"Tool {row['tool_id_xml']} ({plugin_short})"
                    tools_df['display_label'] = tools_df.apply(create_label, axis=1)
                    tool_options = pd.Series(tools_df.db_id.values, index=tools_df.display_label).to_dict()
                selected_tool_display = st.selectbox("2. Select a starting tool:", options=[""] + list(tool_options.keys()))
                if selected_tool_display:
                    tool_db_id = tool_options[selected_tool_display]
                    fields_in_tool = pd.read_sql_query("SELECT field_name FROM tool_fields WHERE tool_id = ? ORDER BY field_name", conn, params=(tool_db_id,))
                    if not fields_in_tool.empty:
                        selected_field = st.selectbox("3. Select a field to trace:", options=[""] + list(fields_in_tool['field_name']))
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Trace Field to Origin (Upstream)"):
                                if selected_field:
                                    with st.spinner("Tracing field backward..."):
                                        lineage_df = tracer.trace_upstream_end_to_end(st.session_state.workspace, workflow_db_id, tool_db_id, selected_field, io_map)
                                        st.write(f"Upstream lineage for **{selected_field}**:")
                                        st.dataframe(lineage_df, use_container_width=True)
                                else: st.warning("Please select a field first.")
                        with col2:
                            if st.button("Find Downstream Impact"):
                                if selected_field:
                                    with st.spinner("Tracing field forward..."):
                                        endpoints_df = tracer.trace_downstream_end_to_end(st.session_state.workspace, workflow_db_id, tool_db_id, selected_field, io_map)
                                        st.write(f"Downstream impact for **{selected_field}**:")
                                        if endpoints_df.empty: st.info("This field is not used in any final outputs.")
                                        else: st.dataframe(endpoints_df, use_container_width=True)
                                else: st.warning("Please select a field first.")
                    else:
                        st.info("This tool has no output fields to trace.")
    finally:
        if conn: conn.close()

elif active_tab == "Trace from an Output File":
    st.subheader("Trace all fields from a final output file")
    output_files = sorted([os.path.basename(k) for k, v in io_map.items() if v.get('producer')])
    if not output_files:
        st.info("No output files found in this workspace to trace from.")
    else:
        selected_file_basename = st.selectbox("1. Select an output file:", options=[""] + output_files, key="output_file_selector")
        if selected_file_basename:
            selected_full_path = ""
            for k in io_map:
                if os.path.basename(k) == selected_file_basename:
                    selected_full_path = k
                    break
            if selected_full_path and io_map[selected_full_path].get('producer'):
                producer_info = io_map[selected_full_path]['producer']
                producer_workflow_id = producer_info['workflow_db_id']
                producer_tool_id = producer_info['tool_db_id']
                conn = db.create_connection()
                try:
                    fields_df = db.find_upstream_fields_for_tool(conn, producer_workflow_id, producer_tool_id)
                    if fields_df.empty:
                        st.warning("Could not retrieve fields for this output file.")
                    else:
                        st.write("2. Select fields to trace:")
                        selected_fields = st.multiselect("Fields:", options=fields_df['field_name'].tolist())
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Trace to Origin (Upstream)", key="trace_upstream_from_output"):
                                if selected_fields:
                                    for field in selected_fields:
                                        with st.spinner(f"Tracing '{field}' upstream..."):
                                            st.markdown(f"--- \n#### Upstream Lineage for: **{field}**")
                                            lineage_df = tracer.trace_upstream_end_to_end(st.session_state.workspace, producer_workflow_id, producer_tool_id, field, io_map)
                                            st.dataframe(lineage_df, use_container_width=True)
                                else:
                                    st.warning("Please select at least one field to trace.")
                        with col2:
                            if st.button("Find Downstream Impact", key="trace_downstream_from_output"):
                                if selected_fields:
                                    for field in selected_fields:
                                        with st.spinner(f"Tracing '{field}' downstream..."):
                                            st.markdown(f"--- \n#### Downstream Impact for: **{field}**")
                                            endpoints_df = tracer.trace_downstream_end_to_end(st.session_state.workspace, producer_workflow_id, producer_tool_id, field, io_map)
                                            if endpoints_df.empty:
                                                st.info(f"The field '{field}' is not used in any other final outputs.")
                                            else:
                                                st.dataframe(endpoints_df, use_container_width=True)
                                else:
                                    st.warning("Please select at least one field to trace.")
                finally:
                    if conn: conn.close()

elif active_tab == "Search by Field Name":
    st.subheader("Search for a field by name")

    # Use a simple variable to track the single active expander
    if 'active_expander' not in st.session_state:
        st.session_state.active_expander = None
    
    with st.form(key="field_search_form"):
        search_term = st.text_input("Enter a partial or full field name (case-insensitive):", key="field_search_input")
        submitted = st.form_submit_button("Search Fields")

    if submitted and search_term:
        # Reset active expander on a new search
        st.session_state.active_expander = None
        conn = db.create_connection()
        try:
            query = """
                SELECT
                    tf.field_name,
                    t.tool_id_xml,
                    t.plugin,
                    w.workflow_name,
                    w.id as workflow_db_id,
                    t.id as tool_db_id
                FROM tool_fields tf
                JOIN tools t ON tf.tool_id = t.id
                JOIN workflows w ON t.workflow_id = w.id
                JOIN workspaces ws ON w.workspace_id = ws.id
                WHERE ws.name = ? AND LOWER(tf.field_name) LIKE ? AND t.plugin NOT LIKE '%Container%'
                ORDER BY w.workflow_name, CAST(t.tool_id_xml AS INTEGER)
            """
            search_pattern = f"%{search_term.lower()}%"
            results_df = pd.read_sql_query(query, conn, params=(st.session_state.workspace, search_pattern))
            st.session_state.field_search_results = results_df
            st.session_state.last_field_search = search_term
            # Clear previous trace results
            for key in list(st.session_state.keys()):
                if key.startswith('trace_up_') or key.startswith('trace_down_'):
                    del st.session_state[key]
        finally:
            if conn: conn.close()

    if 'field_search_results' in st.session_state:
        results_df = st.session_state.field_search_results
        if results_df.empty:
            st.info(f"No fields found in this workspace matching '{st.session_state.get('last_field_search', '')}'.")
        else:
            st.write(f"Found **{len(results_df)}** total occurrences in **{results_df['workflow_name'].nunique()}** workflow(s):")
            grouped_results = results_df.groupby('workflow_name')
            
            for workflow_name, group in grouped_results:
                expander_label = f"Workflow: `{workflow_name}` ({len(group)} tool(s))"
                # Check if this is the active expander
                is_expanded = (st.session_state.active_expander == workflow_name)

                with st.expander(expander_label, expanded=is_expanded):
                    for index, row in group.iterrows():
                        st.markdown(f"--- \n**Tool ID:** `{row['tool_id_xml']}` | **Plugin:** `{row['plugin'].split('.')[-1]}` | **Exact Field Name:** `{row['field_name']}`")
                        
                        b_col1, b_col2 = st.columns(2)
                        up_key = f"trace_up_{index}"
                        down_key = f"trace_down_{index}"

                        with b_col1:
                            if st.button("Trace to Origin (Upstream)", key=f"up_btn_{index}"):
                                # Set this expander to be the active one
                                st.session_state.active_expander = workflow_name
                                with st.spinner("Tracing field backward..."):
                                    lineage_df = tracer.trace_upstream_end_to_end(st.session_state.workspace, row['workflow_db_id'], row['tool_db_id'], row['field_name'], io_map)
                                    st.session_state[up_key] = lineage_df
                        with b_col2:
                            if st.button("Find Downstream Impact", key=f"down_btn_{index}"):
                                # Set this expander to be the active one
                                st.session_state.active_expander = workflow_name
                                with st.spinner("Tracing field forward..."):
                                    endpoints_df = tracer.trace_downstream_end_to_end(st.session_state.workspace, row['workflow_db_id'], row['tool_db_id'], row['field_name'], io_map)
                                    st.session_state[down_key] = endpoints_df
                        
                        if up_key in st.session_state:
                            st.markdown("#### Upstream Lineage")
                            st.dataframe(st.session_state[up_key], use_container_width=True)

                        if down_key in st.session_state:
                            st.markdown("#### Downstream Impact")
                            df = st.session_state[down_key]
                            if df.empty:
                                st.info("This field is not used in any final outputs.")
                            else:
                                st.dataframe(df, use_container_width=True)

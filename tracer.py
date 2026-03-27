import pandas as pd
from lxml import etree as ET
from collections import deque
from parser import extract_io_tools
from database_manager import create_connection
import os
import streamlit as st

# Use a secure parser by default to prevent XXE attacks
SECURE_PARSER = ET.XMLParser(resolve_entities=False, no_network=True)

def find_origin_field_name(config_xml, plugin, target_field_name):
    """
    Parses a tool's config to find the original name of a field before this tool transformed it.
    
    Returns:
        - A string (the original field name) if it was renamed or passed through.
        - A dictionary {'status': 'CREATED', 'expression': '...'} if the field was created here.
    """
    if not any(p in plugin for p in ['AlteryxSelect', 'Join', 'Formula', 'DynamicRename']) or not config_xml:
        return target_field_name

    try:
        config_root = ET.fromstring(config_xml.encode('utf-8') if isinstance(config_xml, str) else config_xml, parser=SECURE_PARSER)
        
        if 'Formula' in plugin:
            for field in config_root.findall(".//FormulaField"):
                if field.get('field') == target_field_name:
                    return {
                        'status': 'CREATED',
                        'expression': field.get('expression', 'N/A')
                    }
            return target_field_name

        if 'DynamicRename' in plugin:
            rename_mode_node = config_root.find('.//RenameMode')
            if rename_mode_node is not None and rename_mode_node.text == 'Add':
                type_node = config_root.find('.//AddPrefixSuffix/Type')
                text_node = config_root.find('.//AddPrefixSuffix/Text')
                if type_node is not None and text_node is not None and text_node.text is not None:
                    if type_node.text == 'Prefix' and target_field_name.startswith(text_node.text):
                        return target_field_name[len(text_node.text):]
                    if type_node.text == 'Suffix' and target_field_name.endswith(text_node.text):
                        return target_field_name[:-len(text_node.text)]
            return target_field_name

        if any(p in plugin for p in ['AlteryxSelect', 'Join']):
            for field in config_root.findall(".//SelectField"):
                if field.get('selected') != 'False':
                    if field.get('rename') == target_field_name:
                        return field.get('field')
            return target_field_name
            
    except (ET.ParseError, TypeError):
        return target_field_name

    return target_field_name


def find_destination_field_name(config_xml, plugin, origin_field_name):
    """
    Parses a tool's config to find the new name of a field after this tool transforms it.
    """
    if not any(p in plugin for p in ['AlteryxSelect', 'Join', 'DynamicRename']) or not config_xml:
        return origin_field_name
        
    try:
        config_root = ET.fromstring(config_xml.encode('utf-8') if isinstance(config_xml, str) else config_xml, parser=SECURE_PARSER)

        if 'DynamicRename' in plugin:
            rename_mode_node = config_root.find('.//RenameMode')
            if rename_mode_node is not None and rename_mode_node.text == 'Add':
                type_node = config_root.find('.//AddPrefixSuffix/Type')
                text_node = config_root.find('.//AddPrefixSuffix/Text')
                if type_node is not None and text_node is not None and text_node.text is not None:
                    if type_node.text == 'Prefix':
                        return text_node.text + origin_field_name
                    if type_node.text == 'Suffix':
                        return origin_field_name + text_node.text
            return origin_field_name

        if any(p in plugin for p in ['AlteryxSelect', 'Join']):
            for field in config_root.findall(".//SelectField"):
                if field.get('selected') != 'False':
                    prefixed_origin_name = f"Right_{origin_field_name}"
                    if field.get('field') == origin_field_name or field.get('field') == prefixed_origin_name:
                        return field.get('rename') or origin_field_name
                        
    except (ET.ParseError, TypeError):
        return origin_field_name
    return origin_field_name

def build_io_map(workspace_name):
    """Builds a map of all data sources, detailing which tools produce and consume them."""
    conn = create_connection()
    if conn is None: return {}
    
    io_map = {}
    try:
        query = """
            SELECT 
                w.id as workflow_db_id, 
                w.workflow_name, 
                t.id as tool_db_id, 
                t.tool_id_xml, 
                t.plugin, 
                t.macro, 
                t.config_xml, 
                t.annotation
            FROM tools t
            JOIN workflows w ON t.workflow_id = w.id
            JOIN workspaces ws ON w.workspace_id = ws.id
            WHERE ws.name = ?
        """
        all_tools_df = pd.read_sql_query(query, conn, params=(workspace_name,))
        if all_tools_df.empty: return {}

        tools_list_for_parser = all_tools_df.to_dict('records')
        inputs, outputs = extract_io_tools(tools_list_for_parser)

        def normalize_path(p):
            if not p or not isinstance(p, str): return ""
            return os.path.normcase(os.path.normpath(p))

        for item in outputs:
            path = normalize_path(item['source_detail'])
            if not path: continue
            if path not in io_map: io_map[path] = {'producer': None, 'consumers': []}
            io_map[path]['producer'] = {
                'workflow_db_id': item['workflow_db_id'],
                'workflow_name': item['workflow_name'],
                'tool_db_id': item['tool_db_id']
            }

        for item in inputs:
            path = normalize_path(item['source_detail'])
            if not path: continue
            if path not in io_map: io_map[path] = {'producer': None, 'consumers': []}
            io_map[path]['consumers'].append({
                'workflow_db_id': item['workflow_db_id'],
                'workflow_name': item['workflow_name'],
                'tool_db_id': item['tool_db_id']
            })
    finally:
        if conn: conn.close()
    return io_map


def trace_upstream_end_to_end(workspace_name, start_workflow_id, start_tool_id, start_field_name, io_map):
    """Traces a field's lineage backward, jumping across workflows."""
    conn = create_connection()
    if not conn: return pd.DataFrame()

    full_lineage = []
    
    queue = deque([(start_workflow_id, start_tool_id, start_field_name)])
    visited = set()

    try:
        while queue:
            workflow_id, tool_id, field_name = queue.popleft()
            
            if (workflow_id, tool_id, field_name) in visited: continue
            visited.add((workflow_id, tool_id, field_name))

            wf_df = pd.read_sql_query("SELECT workflow_name FROM workflows WHERE id = ?", conn, params=(workflow_id,))
            if wf_df.empty:
                print(f"Error: Could not find workflow with ID {workflow_id}. The I/O map may be stale. Skipping this branch of the trace.")
                continue
            workflow_name = wf_df.iloc[0]['workflow_name']
            
            path, origin = _trace_within_workflow(conn, workflow_id, tool_id, field_name)
            
            for step in reversed(path):
                step['Workflow'] = workflow_name
                full_lineage.insert(0, step)

            if origin and origin['type'] == 'INPUT' and origin.get('source_detail'):
                normalized_path = os.path.normcase(os.path.normpath(origin['source_detail']))
                if normalized_path in io_map and io_map[normalized_path].get('producer'):
                    producer = io_map[normalized_path]['producer']
                    full_lineage.insert(0, {
                        'Workflow': f"--- Link via {os.path.basename(origin['source_detail'])} ---",
                        'Tool ID': '-->', 'Plugin': '-->', 'Field Name': '-->', 'Transformation': '-->'
                    })
                    queue.append((producer['workflow_db_id'], producer['tool_db_id'], origin['field_name']))
                elif full_lineage:
                    full_lineage[0]['Transformation'] = 'Origin Field'
            elif full_lineage:
                 full_lineage[0]['Transformation'] = 'Origin Field' if not origin or origin['type'] != 'CREATED' else full_lineage[0]['Transformation']
    finally:
        if conn: conn.close()
        
    return pd.DataFrame(full_lineage)

def trace_downstream_end_to_end(workspace_name, start_workflow_id, start_tool_id, start_field_name, io_map):
    """Traces a field's usage forward across all workflows to all its final destinations."""
    conn = create_connection()
    if not conn: return pd.DataFrame()

    final_endpoints = []
    
    queue = deque([(start_workflow_id, start_tool_id, start_field_name)])
    visited = set()

    try:
        while queue:
            wf_id, tool_db_id, field_name = queue.popleft()
            
            if (wf_id, tool_db_id, field_name) in visited: continue
            visited.add((wf_id, tool_db_id, field_name))

            wf_df = pd.read_sql_query("SELECT workflow_name FROM workflows WHERE id = ?", conn, params=(wf_id,))
            if wf_df.empty:
                print(f"Error: Could not find workflow with ID {wf_id}. The I/O map may be stale. Skipping this branch of the trace.")
                continue
            workflow_name = wf_df.iloc[0]['workflow_name']
            
            endpoints, continuations = _trace_within_workflow_downstream(conn, wf_id, tool_db_id, field_name)
            
            for ep in endpoints:
                ep['Workflow'] = workflow_name
                final_endpoints.append(ep)

            for cont in continuations:
                if not cont.get('source_detail'): continue
                normalized_path = os.path.normcase(os.path.normpath(cont['source_detail']))
                if normalized_path in io_map and io_map[normalized_path].get('consumers'):
                    for consumer in io_map[normalized_path]['consumers']:
                        queue.append((consumer['workflow_db_id'], consumer['tool_db_id'], cont['field_name']))
    finally:
        if conn: conn.close()
        
    return pd.DataFrame(final_endpoints)


def _trace_within_workflow(conn, workflow_id, start_tool_id, start_field_name):
    """(Helper) Traces a field backward within a single workflow. Returns the path and origin details."""
    path, origin_details = [], None
    current_field_name, current_tool_db_id = start_field_name, start_tool_id

    for _ in range(50):
        tool_details_df = pd.read_sql_query("SELECT tool_id_xml, plugin, config_xml, macro, annotation FROM tools WHERE id = ?", conn, params=(current_tool_db_id,))
        if tool_details_df.empty: break
        tool_info = tool_details_df.iloc[0]
        plugin = tool_info['plugin']
        
        path.append({'Tool ID': tool_info['tool_id_xml'], 'Plugin': plugin.split('.')[-1], 'Field Name': current_field_name, 'Transformation': 'Passthrough'})
        
        if any(ptype in plugin for ptype in ['DbFileInput', 'DynamicInput']) or 'Macro' in plugin:
            tool_dict = {
                'tool_db_id': current_tool_db_id,
                'id': tool_info['tool_id_xml'], 
                'plugin': plugin, 
                'config_xml': tool_info['config_xml'], 
                'macro': tool_info['macro'], 
                'annotation': tool_info['annotation']
            }
            inputs, _ = extract_io_tools([tool_dict])
            origin_details = {'type': 'INPUT', 'source_detail': inputs[0]['source_detail'] if inputs else 'Unknown', 'field_name': current_field_name}
            break

        origin_df = pd.read_sql_query("SELECT origin_tool_id_xml FROM connections WHERE workflow_id = ? AND destination_tool_id_xml = ?", conn, params=(workflow_id, tool_info['tool_id_xml']))
        if origin_df.empty:
            origin_details = {'type': 'NO_INPUT_CONNECTION'}
            break

        previous_tool_xml_id = origin_df.iloc[0]['origin_tool_id_xml']
        next_step = find_origin_field_name(tool_info['config_xml'], plugin, current_field_name)
        
        if isinstance(next_step, str):
            if next_step != current_field_name: path[-1]['Transformation'] = f"Renamed from '{next_step}'"
            current_field_name = next_step
        elif isinstance(next_step, dict) and next_step.get('status') == 'CREATED':
            path[-1]['Transformation'] = f"Created with expression: {next_step['expression']}"
            origin_details = {'type': 'CREATED'}
            break
        else: break

        prev_tool_df = pd.read_sql_query("SELECT id FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(workflow_id, previous_tool_xml_id))
        if prev_tool_df.empty: break
        current_tool_db_id = prev_tool_df.iloc[0]['id']
        
    return path, origin_details

def _trace_within_workflow_downstream(conn, wf_id, start_tool_db_id, start_field_name):
    """(Helper) Traces a field forward within a single workflow. Returns final outputs and continuation points."""
    endpoints, continuations = [], []
    start_tool_info = pd.read_sql_query("SELECT tool_id_xml FROM tools WHERE id = ?", conn, params=(start_tool_db_id,)).iloc[0]
    
    queue = deque([(start_tool_info['tool_id_xml'], start_field_name)])
    visited = set()

    while queue:
        tool_xml_id, field_name = queue.popleft()
        if (tool_xml_id, field_name) in visited: continue
        visited.add((tool_xml_id, field_name))

        tool_info_df = pd.read_sql_query("SELECT id, plugin, config_xml FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(wf_id, tool_xml_id))
        if tool_info_df.empty: continue
        tool_info = tool_info_df.iloc[0]

        if any(ptype in tool_info['plugin'] for ptype in ['DbFileOutput', 'SalesforceOutput', 'Upload']):
            tool_dict = {
                'tool_db_id': tool_info['id'],
                'id': tool_xml_id,
                'plugin': tool_info['plugin'],
                'config_xml': tool_info['config_xml']
            }
            _, outputs = extract_io_tools([tool_dict])
            if outputs:
                output_details = {'Final Field Name': field_name, 'Output Tool ID': tool_xml_id, 'Destination': outputs[0]['source_detail']}
                endpoints.append(output_details)
                continuations.append({'source_detail': outputs[0]['source_detail'], 'field_name': field_name})
            continue

        destination_df = pd.read_sql_query("SELECT destination_tool_id_xml FROM connections WHERE workflow_id = ? AND origin_tool_id_xml = ?", conn, params=(wf_id, tool_xml_id))
        for _, row in destination_df.iterrows():
            next_tool_xml_id = row['destination_tool_id_xml']
            next_tool_details_df = pd.read_sql_query("SELECT plugin, config_xml FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(wf_id, next_tool_xml_id))
            if next_tool_details_df.empty: continue
            next_tool_info = next_tool_details_df.iloc[0]
            next_field_name = find_destination_field_name(next_tool_info['config_xml'], next_tool_info['plugin'], field_name)
            queue.append((next_tool_xml_id, next_field_name))

    return endpoints, continuations
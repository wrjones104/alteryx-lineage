import pandas as pd
from lxml import etree as ET
from collections import deque
from parser import extract_io_tools
from database_manager import create_connection

def find_origin_field_name(config_xml, plugin, target_field_name):
    """
    Parses a tool's config to find the original name of a field before this tool transformed it.
    
    Returns:
        - A string (the original field name) if it was renamed or passed through.
        - A dictionary {'status': 'CREATED', 'expression': '...'} if the field was created here.
    """
    if not any(p in plugin for p in ['AlteryxSelect', 'Join', 'Formula']) or not config_xml:
        return target_field_name

    try:
        config_root = ET.fromstring(config_xml)
        
        if 'Formula' in plugin:
            for field in config_root.findall(".//FormulaField"):
                if field.get('field') == target_field_name:
                    return {
                        'status': 'CREATED',
                        'expression': field.get('expression', 'N/A')
                    }
            return target_field_name
            
        if any(p in plugin for p in ['AlteryxSelect', 'Join']):
            for field in config_root.findall(".//SelectField"):
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
    if not any(p in plugin for p in ['AlteryxSelect', 'Join']) or not config_xml:
        return origin_field_name
    try:
        config_root = ET.fromstring(config_xml)
        for field in config_root.findall(".//SelectField"):
            prefixed_origin_name = f"Right_{origin_field_name}"
            if field.get('field') == origin_field_name or field.get('field') == prefixed_origin_name:
                return field.get('rename') or origin_field_name
    except (ET.ParseError, TypeError):
        return origin_field_name
    return origin_field_name


def trace_field_lineage(workflow_db_id, start_tool_db_id, start_field_name):
    """
    Traces a field's lineage backward from a starting tool.
    """
    conn = create_connection()
    if not conn: return pd.DataFrame()
    
    path = []
    current_field_name = start_field_name
    current_tool_db_id = start_tool_db_id
    
    try:
        for _ in range(50): # Safety break
            tool_details_df = pd.read_sql_query("SELECT tool_id_xml, plugin, config_xml FROM tools WHERE id = ?", conn, params=(current_tool_db_id,))
            if tool_details_df.empty: break
            
            tool_info = tool_details_df.iloc[0]
            plugin = tool_info['plugin']
            
            path.append({
                'Tool ID': tool_info['tool_id_xml'],
                'Plugin': plugin.split('.')[-1],
                'Field Name': current_field_name,
                'Transformation': 'Passthrough'
            })

            if any(ptype in plugin for ptype in ['DbFileInput', 'DynamicInput']) or 'Macro' in plugin:
                path[-1]['Transformation'] = 'Origin Field'
                break
                
            origin_df = pd.read_sql_query("SELECT origin_tool_id_xml FROM connections WHERE workflow_id = ? AND destination_tool_id_xml = ?", conn, params=(workflow_db_id, tool_info['tool_id_xml']))
            if origin_df.empty:
                path[-1]['Transformation'] = 'Origin (Start of Path)'
                break

            previous_tool_xml_id = origin_df.iloc[0]['origin_tool_id_xml']
            
            next_step = find_origin_field_name(tool_info['config_xml'], plugin, current_field_name)
            
            if isinstance(next_step, str):
                if next_step != current_field_name:
                    path[-1]['Transformation'] = f"Renamed from '{next_step}'"
                current_field_name = next_step
            elif isinstance(next_step, dict) and next_step.get('status') == 'CREATED':
                path[-1]['Transformation'] = f"Created with expression: {next_step['expression']}"
                break
            else:
                break

            prev_tool_df = pd.read_sql_query("SELECT id FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(workflow_db_id, previous_tool_xml_id))
            if prev_tool_df.empty: break
            current_tool_db_id = prev_tool_df.iloc[0]['id']
    finally:
        if conn: conn.close()

    return pd.DataFrame(path)

def trace_field_downstream(workflow_db_id, start_tool_db_id, start_field_name):
    """
    Traces a field's usage forward to all its final output destinations.
    """
    conn = create_connection()
    if not conn: return pd.DataFrame(), pd.DataFrame()

    endpoints = []
    log = []
    
    start_tool_info = pd.read_sql_query("SELECT tool_id_xml FROM tools WHERE id = ?", conn, params=(start_tool_db_id,)).iloc[0]
    start_tool_xml_id = start_tool_info['tool_id_xml']

    queue = deque([(start_tool_xml_id, start_field_name)])
    visited = set()

    try:
        while queue:
            current_tool_xml_id, current_field_name = queue.popleft()
            if (current_tool_xml_id, current_field_name) in visited: continue
            visited.add((current_tool_xml_id, current_field_name))
            
            tool_info_df = pd.read_sql_query("SELECT id, plugin, config_xml FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(workflow_db_id, current_tool_xml_id))
            if tool_info_df.empty: continue
            tool_info = tool_info_df.iloc[0]
            plugin = tool_info['plugin']
            
            log_entry = {'Current Tool ID': current_tool_xml_id, 'Current Field Name': current_field_name}
            
            next_tool_ids = []
            next_field_names = []

            if any(ptype in plugin for ptype in ['DbFileOutput']):
                _, outputs = extract_io_tools([{'id': current_tool_xml_id, 'plugin': plugin, 'config_xml': tool_info['config_xml']}])
                if outputs: endpoints.append({'Final Field Name': current_field_name, 'Output Tool ID': current_tool_xml_id, 'Destination': outputs[0]['source_detail']})
                next_tool_ids.append("ENDPOINT")
            else:
                destination_df = pd.read_sql_query("SELECT destination_tool_id_xml FROM connections WHERE workflow_id = ? AND origin_tool_id_xml = ?", conn, params=(workflow_db_id, current_tool_xml_id))
                if destination_df.empty:
                    next_tool_ids.append("END OF PATH")
                else:
                    for _, row in destination_df.iterrows():
                        next_tool_xml_id = row['destination_tool_id_xml']
                        next_tool_details_df = pd.read_sql_query("SELECT plugin, config_xml FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(workflow_db_id, next_tool_xml_id))
                        if next_tool_details_df.empty: continue
                        next_tool_info = next_tool_details_df.iloc[0]
                        next_field_name = find_destination_field_name(next_tool_info['config_xml'], next_tool_info['plugin'], current_field_name)
                        next_tool_ids.append(next_tool_xml_id)
                        next_field_names.append(next_field_name)
                        queue.append((next_tool_xml_id, next_field_name))
            
            # --- FIX: Convert list of IDs/names to a comma-separated string for stable display ---
            log_entry['Next Tool ID'] = ', '.join(next_tool_ids)
            log_entry['Next Field Name'] = ', '.join(next_field_names)
            log.append(log_entry)
    finally:
        if conn: conn.close()
    
    return pd.DataFrame(endpoints), pd.DataFrame(log)
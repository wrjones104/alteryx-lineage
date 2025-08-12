from lxml import etree as ET
import yaml
import re

def parse_workflow(workflow_path):
    """
    Parses an Alteryx .yxmd file to extract all tools, connections, and the output
    field schema for each tool. Handles both file paths and file-like objects.
    """
    try:
        # lxml's ET.parse handles both file paths and in-memory file objects correctly.
        tree = ET.parse(workflow_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return [], [], []

    tools = []
    connections = []
    
    for node in root.findall('.//Node'):
        tool_id = node.get('ToolID')
        
        gui_settings = node.find('GuiSettings')
        plugin = 'Unknown'
        if gui_settings is not None:
            plugin = gui_settings.get('Plugin') or 'Unknown'
        
        config_node = node.find('./Properties/Configuration')
        config_xml_string = ET.tostring(config_node, encoding='unicode') if config_node is not None else ''

        annotation_text = ''
        ann_node = node.find('./Properties/Annotation/AnnotationText')
        def_ann_node = node.find('./Properties/Annotation/DefaultAnnotationText')
        if ann_node is not None and ann_node.text:
            annotation_text = ann_node.text
        elif def_ann_node is not None and def_ann_node.text:
            annotation_text = def_ann_node.text

        engine_settings = node.find('EngineSettings')
        macro_path = None
        if engine_settings is not None:
            macro_path = engine_settings.get('Macro')

        output_fields = []
        # This universal search finds all RecordInfo tags, which is the most robust method.
        for record_info_node in node.findall('.//RecordInfo'):
            for field_node in record_info_node.findall('Field'):
                output_fields.append({
                    'name': field_node.get('name'),
                    'type': field_node.get('type'),
                    'size': field_node.get('size'),
                    'source': field_node.get('source'),
                    'description': field_node.get('description'),
                })

        tools.append({
            'id': tool_id,
            'plugin': plugin,
            'macro': macro_path,
            'config_xml': config_xml_string,
            'annotation': annotation_text,
            'output_fields': output_fields
        })

    for conn in root.findall('.//Connection'):
        origin_node = conn.find('.//Origin')
        dest_node = conn.find('.//Destination')
        if origin_node is not None and dest_node is not None:
            origin_tool_id = origin_node.get('ToolID')
            dest_tool_id = dest_node.get('ToolID')
            connections.append({
                'origin_id': origin_tool_id,
                'destination_id': dest_tool_id
            })

    return tools, connections, []


def parse_annotation(annotation_text):
    if not annotation_text: return None
    match = re.search(r'--- lineage ---(.*?)---', annotation_text, re.DOTALL)
    if not match: return None
    yaml_content = match.group(1)
    try:
        return yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML from annotation: {e}")
        return None

def extract_io_tools(tools_list):
    inputs, outputs = [], []
    INPUT_PLUGINS = ['DbFileInput', 'Download', 'SalesforceInput', 'Directory', 'DynamicInput']
    OUTPUT_PLUGINS = ['DbFileOutput', 'Upload', 'SalesforceOutput']
    for tool in tools_list:
        macro = tool.get('macro') or ''
        if 'Input Data Selector.yxmc' in macro:
            try:
                config_root = ET.fromstring(tool['config_xml'])
                value_node = config_root.find(".//Value[@name='Drop Down (5)']")
                if value_node is not None and value_node.text:
                    inputs.append({'tool_id': tool['id'], 'plugin': 'Input Data Selector (Macro)', 'source_detail': value_node.text})
                continue
            except (ET.ParseError, TypeError): continue
        manual_lineage = parse_annotation(tool.get('annotation', ''))
        if manual_lineage:
            if 'inputs' in manual_lineage and manual_lineage['inputs']:
                for item in manual_lineage['inputs']:
                    inputs.append({'tool_id': tool['id'], 'plugin': 'Manual Annotation', 'source_detail': f"{item.get('type', 'N/A')}: {item.get('path', 'N/A')}"})
            if 'outputs' in manual_lineage and manual_lineage['outputs']:
                for item in manual_lineage['outputs']:
                     outputs.append({'tool_id': tool['id'], 'plugin': 'Manual Annotation', 'source_detail': f"{item.get('type', 'N/A')}: {item.get('path', 'N/A')}"})
            continue
        is_input = any(plugin in tool['plugin'] for plugin in INPUT_PLUGINS)
        is_output = any(plugin in tool['plugin'] for plugin in OUTPUT_PLUGINS)
        if (is_input or is_output) and tool['config_xml']:
            try:
                config_root = ET.fromstring(tool['config_xml'])
                source_detail = "Not Found"
                file_node = config_root.find('.//File')
                if file_node is not None: source_detail = file_node.get('value') or file_node.text
                query_node = config_root.find('.//Query')
                if query_node is not None and query_node.text: source_detail = ' '.join(query_node.text.strip().split())
                if 'Directory' in tool['plugin']:
                    dir_node = config_root.find('.//Directory')
                    spec_node = config_root.find('.//FileSpec')
                    if dir_node is not None and spec_node is not None: source_detail = f"{dir_node.text}\\{spec_node.text}"
                if 'DynamicInput' in tool['plugin']:
                     template_node = config_root.find('.//InputConfiguration/Configuration/File')
                     if template_node is not None: source_detail = f"Dynamic from template: {template_node.get('value')}"
                io_item = {'tool_id': tool['id'], 'plugin': tool['plugin'].split('.')[-1], 'source_detail': source_detail}
                if is_input: inputs.append(io_item)
                else: outputs.append(io_item)
            except (ET.ParseError, TypeError): continue
    return inputs, outputs
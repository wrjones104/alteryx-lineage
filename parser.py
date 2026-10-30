from lxml import etree as ET
import ntpath
import yaml
import re

# Plugin name fragment -> (direction, friendly tool name). Matched as a substring of the GuiSettings Plugin.
IO_PLUGINS = {
    'DbFileInput': ('input', 'Input Data'),
    'DynamicInput': ('input', 'Dynamic Input'),
    'Directory': ('input', 'Directory'),
    'Download': ('input', 'Download'),
    'SalesforceInput': ('input', 'Salesforce Input'),
    'BlobInput': ('input', 'Blob Input'),
    'LockInInput': ('input', 'Connect In-DB'),
    'DbFileOutput': ('output', 'Output Data'),
    'Upload': ('output', 'Upload'),
    'SalesforceOutput': ('output', 'Salesforce Output'),
    'BlobOutput': ('output', 'Blob Output'),
    'LockInOutput': ('output', 'Write Data In-DB'),
}

# Flags attached to I/O items. REVIEW_FLAGS are the ones a person needs to look at.
FLAG_NOT_FOUND = 'Path not found in tool configuration'
FLAG_DYNAMIC = 'Dynamic: files come from an incoming field at runtime (template shown)'
FLAG_MULTI_FILE = 'Output file name may be set per record from a field'
FLAG_WILDCARD = 'Wildcard path (matches multiple files)'
FLAG_RELATIVE = "Relative path (resolved against the workflow's folder at runtime)"
FLAG_CONSTANT = 'Uses a workflow constant (%...%)'
FLAG_LOCAL_DRIVE = 'Local C: drive (may only exist on the machine that ran it)'
FLAG_LISTING = 'Directory listing only (reads file names, not contents)'
REVIEW_FLAGS = {FLAG_NOT_FOUND, FLAG_DYNAMIC, FLAG_MULTI_FILE, FLAG_RELATIVE, FLAG_CONSTANT, FLAG_LOCAL_DRIVE}

PLACEHOLDER_SOURCES = {'', 'not found', 'unknown', 'n/a', 'none'}
_DB_PREFIXES = ('aka:', 'odbc:', 'oledb:', 'oci:', 'sqlncli', 'snowbl:', 'simba', 'dsn=')
_SQL_PREFIX = re.compile(r'^\s*(select|with|exec|execute|call)\b', re.IGNORECASE)
_PATH_PATTERN = re.compile(r'(?:[A-Za-z]:\\|\\\\[\w.$-]+\\)[^"\'<>|\r\n\t]*')
# Values of 'type' in a lineage annotation -> data store type
ANNOTATION_TYPES = {
    'file': 'File',
    'database': 'Database', 'db': 'Database', 'table': 'Database', 'sql': 'Database',
    'api': 'Web/API', 'web': 'Web/API', 'url': 'Web/API',
    'salesforce': 'Salesforce',
}


def _safe_xml_parser():
    # Workflow files are uploaded by users: never expand entities or fetch anything over the network.
    # A new parser per call, because lxml parsers must not be shared between threads (Streamlit sessions).
    return ET.XMLParser(resolve_entities=False, no_network=True, load_dtd=False)


def safe_fromstring(xml_text):
    return ET.fromstring(xml_text, parser=_safe_xml_parser())


def is_join(plugin):
    """True for the Join tool only (not Join Multiple, whose inputs are #1, #2, ...)."""
    return isinstance(plugin, str) and plugin.split('.')[-1] == 'Join'


def parse_workflow(workflow_path):
    """
    Parses an Alteryx .yxmd/.yxwz file to extract all tools, connections, the output
    field schema for each tool, and workflow-level metadata. Handles both file paths
    and file-like objects. Returns (None, None, None) if the file cannot be parsed.
    """
    try:
        tree = ET.parse(workflow_path, parser=_safe_xml_parser())
        root = tree.getroot()
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return None, None, None

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

        # One schema per output anchor: a Join has Join/Left/Right, a Filter True/False
        output_fields = []
        for meta_info_node in node.findall('./Properties/MetaInfo'):
            anchor = meta_info_node.get('connection')
            seen = set()
            for field_node in meta_info_node.findall('./RecordInfo/Field'):
                name = field_node.get('name')
                if not name or name in seen: continue
                seen.add(name)
                output_fields.append({
                    'name': name,
                    'type': field_node.get('type'),
                    'size': field_node.get('size'),
                    'source': field_node.get('source'),
                    'description': field_node.get('description'),
                    'anchor': anchor,
                })

        tools.append({
            'id': tool_id, # This remains the XML ToolID
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
                'destination_id': dest_tool_id,
                # Anchor names, e.g. a Join's 'Left'/'Right' inputs and 'Join'/'Left'/'Right' outputs
                'origin_anchor': origin_node.get('Connection'),
                'destination_anchor': dest_node.get('Connection'),
            })

    workflow_meta = {
        'alteryx_version': root.get('yxmdVer'),
        'description': _node_value(root.find('./Properties/MetaInfo/Description')),
        'author': _node_value(root.find('./Properties/MetaInfo/Author')),
    }

    return tools, connections, workflow_meta


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


def strip_lineage_annotation(annotation_text):
    """Returns the human-written part of a tool annotation, without any lineage YAML block."""
    if not annotation_text: return ''
    return re.sub(r'--- lineage ---.*?---', '', annotation_text, flags=re.DOTALL).strip()


def _node_value(node):
    if node is None: return None
    value = node.get('value') or (node.text or '').strip()
    return value or None


def is_placeholder(path):
    return not isinstance(path, str) or path.strip().lower() in PLACEHOLDER_SOURCES


def split_source(path):
    """Splits an Alteryx source string into (location, object). Excel sheets, Access tables and
    database tables/queries follow a '|||' separator, e.g. 'C:\\book.xlsx|||`Sheet1$`'."""
    if not isinstance(path, str): return '', None
    location, sep, obj = path.partition('|||')
    obj = obj.strip() if sep else None
    if obj and not obj.lower().startswith('select'):
        # Excel inputs are written as `Sheet1$` (or `'Sheet 1$'` with spaces), outputs as Sheet1
        obj = obj.strip('`').strip("'").rstrip('$') or None
    return location.strip(), obj or None


def classify_source(path):
    """Returns the data store type for a configured path: File, Database, Web/API, Other or Unknown."""
    if is_placeholder(path): return 'Unknown'
    location = split_source(path)[0]
    low = location.lower()
    if re.match(r'^(https?|s?ftp)://', low): return 'Web/API'
    if low.startswith(_DB_PREFIXES) or re.search(r'\b(dsn|driver|server|data source)\s*=', low) or _SQL_PREFIX.match(low):
        return 'Database'
    if low.startswith(('\\\\', '//')) or re.match(r'^[a-z]:', low) or '%' in low or ntpath.splitext(low)[1]:
        return 'File'
    return 'Other'


def location_type(location):
    """Classifies a file location as UNC, drive letter, workflow constant or relative."""
    if location.startswith(('\\\\', '//')): return 'UNC Path'
    if re.match(r'^[A-Za-z]:', location): return f"Drive {location[0].upper()}:"
    if '%' in location: return 'Workflow Constant'
    return 'Relative Path'


def _norm_prefix(prefix):
    prefix = ntpath.normcase(ntpath.normpath(prefix.strip()))
    return prefix if prefix.endswith('\\') else prefix + '\\'


def resolve_path(location, aliases=()):
    """Normalizes a Windows path and applies drive mappings, e.g. ('S:', '\\\\fileserver\\share')."""
    if not location: return ''
    path = ntpath.normpath(location.strip())
    lowered = ntpath.normcase(path)
    # Most specific mapping first, compared after normalizing (so 'S:\\\\' doesn't beat 'S:\\AP')
    usable = [(_norm_prefix(p), t) for p, t in aliases or () if p and p.strip() and t and t.strip()]
    for norm_prefix, target in sorted(usable, key=lambda a: len(a[0]), reverse=True):
        if lowered.startswith(norm_prefix) or lowered + '\\' == norm_prefix:
            path = ntpath.normpath(ntpath.join(target.strip(), path[len(norm_prefix):]))
            break
    return path


def data_store_key(path, aliases=()):
    """Key used to link an output of one workflow to an input of another. Files are linked
    at the file level (Excel sheets are ignored); other sources by their full text."""
    if is_placeholder(path): return None
    if classify_source(path) == 'File':
        return ntpath.normcase(resolve_path(split_source(path)[0], aliases))
    return ' '.join(path.split()).lower()


def lineage_key(io_item, aliases=()):
    """data_store_key for an item returned by extract_io_tools; None if it cannot be linked."""
    if not io_item or io_item.get('is_dynamic'): return None
    return data_store_key(io_item.get('path'), aliases)


def wildcard_match(pattern_key, key):
    """Matches an Alteryx wildcard path (* and ?) against a normalized path, within one folder."""
    regex = re.escape(pattern_key).replace(r'\*', r'[^\\]*').replace(r'\?', r'[^\\]')
    return re.fullmatch(regex, key) is not None


def find_paths_in_config(config_xml):
    """Finds Windows file paths anywhere in a tool's configuration (text or attribute values)."""
    if not config_xml: return []
    try:
        config_root = safe_fromstring(config_xml)
    except (ET.ParseError, ValueError):
        return []
    found = []
    for element in config_root.iter():
        values = [element.text or ''] + list(element.attrib.values())
        for value in values:
            for match in _PATH_PATTERN.findall(value):
                match = match.rstrip(' .,;)')
                if match not in found: found.append(match)
    return found


def _path_flags(path, data_store_type):
    flags = []
    if is_placeholder(path):
        return [FLAG_NOT_FOUND]
    if data_store_type != 'File':
        return flags
    location = split_source(path)[0]
    if '*' in location or '?' in location: flags.append(FLAG_WILDCARD)
    loc_type = location_type(location)
    if loc_type == 'Relative Path': flags.append(FLAG_RELATIVE)
    if loc_type == 'Workflow Constant' or '%' in location: flags.append(FLAG_CONSTANT)
    if loc_type == 'Drive C:': flags.append(FLAG_LOCAL_DRIVE)
    return flags


def _io_item(io_base, plugin, tool_type, source_detail, path, data_store_type=None,
             is_dynamic=False, extra_flags=(), write_mode=None):
    data_store_type = data_store_type or classify_source(path)
    flags = _path_flags(path, data_store_type) + [f for f in extra_flags if f]
    return {
        **io_base,
        'plugin': plugin,
        'source_detail': source_detail,
        'tool_type': tool_type,
        'path': None if is_placeholder(path) else path,
        'data_store_type': data_store_type,
        'is_dynamic': is_dynamic,
        'write_mode': write_mode,
        'flags': flags,
    }


def extract_io_tools(tools_list):
    """
    Finds every input and output in a list of tools. Each item keeps the legacy 'source_detail'
    string (used by the impact report) and adds structured fields: tool_type, path,
    data_store_type, is_dynamic, write_mode and flags.
    """
    inputs, outputs = [], []
    for tool in tools_list:
        # Use the unique database ID (tool_db_id) as the primary identifier
        io_base = {
            'tool_db_id': tool['tool_db_id'],
            'workflow_db_id': tool.get('workflow_db_id'),
            'workflow_name': tool.get('workflow_name')
        }
        macro = tool.get('macro') or ''
        if 'Input Data Selector.yxmc' in macro:
            try:
                config_root = safe_fromstring(tool['config_xml'])
                value_node = config_root.find(".//Value[@name='Drop Down (5)']")
                if value_node is not None and value_node.text:
                    name = 'Input Data Selector (Macro)'
                    inputs.append(_io_item(io_base, name, name, value_node.text, value_node.text))
                continue
            except (ET.ParseError, TypeError, ValueError): continue

        manual_lineage = parse_annotation(tool.get('annotation', ''))
        if isinstance(manual_lineage, dict):
            for key, target in (('inputs', inputs), ('outputs', outputs)):
                for entry in manual_lineage.get(key) or []:
                    if not isinstance(entry, dict): continue
                    detail = f"{entry.get('type', 'N/A')}: {entry.get('path', 'N/A')}"
                    store_type = ANNOTATION_TYPES.get(str(entry.get('type') or '').strip().lower())
                    target.append(_io_item(io_base, 'Manual Annotation', 'Manual Annotation', detail, entry.get('path'),
                                           data_store_type=store_type))
            continue

        plugin = tool.get('plugin') or ''
        match = next((v for k, v in IO_PLUGINS.items() if k in plugin), None)
        if not match or not tool.get('config_xml'):
            continue
        direction, tool_type = match

        try:
            config_root = safe_fromstring(tool['config_xml'])
        except (ET.ParseError, TypeError, ValueError):
            continue

        source_detail = "Not Found"
        file_node = config_root.find('.//File')
        if file_node is not None: source_detail = file_node.get('value') or file_node.text
        query_node = config_root.find('.//Query')
        if query_node is not None and query_node.text: source_detail = ' '.join(query_node.text.strip().split())
        path = source_detail
        data_store_type, is_dynamic, extra_flags = None, False, []

        if 'Directory' in plugin:
            directory = _node_value(config_root.find('.//Directory'))
            spec = _node_value(config_root.find('.//FileSpec')) or '*.*'
            source_detail = path = f"{directory}\\{spec}" if directory else "Not Found"
            extra_flags.append(FLAG_LISTING)
        elif 'DynamicInput' in plugin:
            template = _node_value(config_root.find('.//InputConfiguration/Configuration/File'))
            if template is not None:
                source_detail = f"Dynamic from template: {template}"
                path = template
            is_dynamic = True
            extra_flags.append(FLAG_DYNAMIC)
        elif 'LockIn' in plugin:
            connection = _node_value(config_root.find('.//Connection'))
            table = _node_value(config_root.find('.//Table'))
            obj = source_detail if query_node is not None and query_node.text else table
            if connection:
                path = f"{connection}|||{obj}" if obj else connection
            data_store_type = 'Database'
        elif 'Salesforce' in plugin:
            data_store_type = 'Salesforce'
        elif 'Blob' in plugin and is_placeholder(path):
            found = find_paths_in_config(tool['config_xml'])
            path = found[0] if found else path

        if direction == 'output' and config_root.find(".//MultiFile[@value='True']") is not None:
            extra_flags.append(FLAG_MULTI_FILE)
        write_mode = _node_value(config_root.find('.//OutputOption')) or _node_value(config_root.find('.//OutputMode'))

        io_item = _io_item(io_base, plugin.split('.')[-1], tool_type, source_detail, path,
                           data_store_type=data_store_type, is_dynamic=is_dynamic,
                           extra_flags=extra_flags, write_mode=write_mode)
        if direction == 'input': inputs.append(io_item)
        else: outputs.append(io_item)
    return inputs, outputs

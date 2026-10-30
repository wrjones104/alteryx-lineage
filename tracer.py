import re
import pandas as pd
from lxml import etree as ET
from collections import deque
from parser import extract_io_tools, lineage_key, wildcard_match, split_source, safe_fromstring, is_join
from database_manager import create_connection, get_path_aliases, find_upstream_fields_for_tool
import ntpath
import streamlit as st

# Tools that read data themselves; an upstream trace always stops at them.
ORIGIN_PLUGINS = ('DbFileInput', 'DynamicInput')
MAX_BRANCHES = 25
MAX_STEPS = 200

def find_origin_field_name(config_xml, plugin, target_field_name):
    """
    Parses a tool's config to find the original name of a field before this tool transformed it.
    
    Returns:
        - A string (the original field name) if it was renamed or passed through.
        - A dictionary {'status': 'CREATED', 'expression': '...'} if the field was created here, or
          {'status': 'UPDATED', ...} if an expression rewrote it from its own value (e.g. [Amount]*2).
    """
    if not any(p in plugin for p in ['AlteryxSelect', 'Join', 'Formula', 'DynamicRename']) or not config_xml:
        return target_field_name

    try:
        config_root = safe_fromstring(config_xml)

        if 'Formula' in plugin:
            for field in config_root.findall(".//FormulaField"):
                if field.get('field') == target_field_name:
                    expression = field.get('expression', 'N/A')
                    self_reference = re.search(rf"\[{re.escape(target_field_name)}\]", expression, re.IGNORECASE)
                    return {
                        'status': 'UPDATED' if self_reference else 'CREATED',
                        'expression': expression
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


def find_destination_field_name(config_xml, plugin, origin_field_name, in_anchor=None):
    """
    Parses a tool's config to find the new name of a field after this tool transforms it.
    in_anchor is the input the field arrives on (a Join's 'Left' or 'Right'). Returns None if the
    tool drops the field (deselected in a Select or Join).
    """
    if not any(p in plugin for p in ['AlteryxSelect', 'Join', 'DynamicRename']) or not config_xml:
        return origin_field_name

    try:
        config_root = safe_fromstring(config_xml)

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
            select_fields = {field.get('field'): field for field in config_root.findall(".//SelectField")}
            # Join select lists name fields by input, e.g. 'Left_Account' / 'Right_Account'
            if is_join(plugin):
                sides = [in_anchor] if in_anchor in ('Left', 'Right') else ['Left', 'Right']
                names = [f"{side}_{origin_field_name}" for side in sides] + [origin_field_name]
            else:
                names = [origin_field_name]
            field = next((select_fields[n] for n in names if n in select_fields), None)
            if field is not None:
                if field.get('selected') == 'False': return None
                if field.get('rename'): return field.get('rename')
            else:
                # Fields not listed follow the '*Unknown' entry, which can drop them
                unknown = select_fields.get('*Unknown')
                if unknown is not None and unknown.get('selected') == 'False': return None
            if not is_join(plugin):
                return origin_field_name
            left_twin = select_fields.get(f"Left_{origin_field_name}")
            if in_anchor == 'Right' and left_twin is not None and left_twin.get('selected') != 'False':
                # Both inputs have this name and the Left one keeps it, so Alteryx prefixes the Right one
                return f"Right_{origin_field_name}"
            return origin_field_name

    except (ET.ParseError, TypeError):
        return origin_field_name
    return origin_field_name

@st.cache_data(show_spinner=False)
def build_io_map(workspace_name):
    """
    Builds a map of all data sources, detailing which tools produce and consume them. Each entry has:
      'producer'  - the first tool that writes exactly this key (None for external sources)
      'producers' - every tool whose output feeds this key, including outputs a wildcard input matches
      'consumers' - every tool that reads it, including wildcard inputs matching this key
    """
    aliases = get_path_aliases(workspace_name)
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

        def entry(path):
            return io_map.setdefault(path, {'producer': None, 'producers': [], 'consumers': []})

        def ref(item):
            return {
                'workflow_db_id': item['workflow_db_id'],
                'workflow_name': item['workflow_name'],
                'tool_db_id': item['tool_db_id'],
                # Excel files link at the file level; the sheet picks the right producer when there are several
                'sheet': (split_source(item.get('path'))[1] or '').lower() or None,
            }

        for item in outputs:
            path = lineage_key(item, aliases)
            if not path: continue
            producer = ref(item)
            entry(path)['producers'].append(producer)
            if entry(path)['producer'] is None: entry(path)['producer'] = producer

        for item in inputs:
            path = lineage_key(item, aliases)
            if not path: continue
            entry(path)['consumers'].append(ref(item))

        # Wildcard inputs (e.g. gl_*.csv) read every output they match
        written = [k for k, v in io_map.items() if v['producer']]
        for pattern, pattern_entry in list(io_map.items()):
            if not pattern_entry['consumers'] or not ('*' in pattern or '?' in pattern): continue
            for key in written:
                if key == pattern or not wildcard_match(pattern, key): continue
                pattern_entry['producers'].extend(p for p in io_map[key]['producers'] if p not in pattern_entry['producers'])
                io_map[key]['consumers'].extend(c for c in pattern_entry['consumers'] if c not in io_map[key]['consumers'])
    finally:
        if conn: conn.close()
    return io_map


def _producers_for(io_map, key, io_item):
    """The producers an input reads from; for an Excel input, those writing its sheet when any do."""
    producers = io_map.get(key, {}).get('producers') or []
    sheet = (split_source((io_item or {}).get('path'))[1] or '').lower() or None
    same_sheet = [p for p in producers if p.get('sheet') == sheet]
    return same_sheet or producers


def trace_upstream_end_to_end(workspace_name, start_workflow_id, start_tool_id, start_field_name, io_map):
    """
    Traces a field's lineage backward, jumping across workflows. Returns one row per step, origin first.
    When the field comes from more than one place (e.g. both inputs of a Union), each route is
    numbered in a 'Branch' column.
    """
    conn = create_connection()
    if not conn: return pd.DataFrame()

    try:
        chains = _trace_upstream_chains(conn, start_workflow_id, start_tool_id, start_field_name,
                                        io_map, get_path_aliases(workspace_name), frozenset(), {})
    finally:
        if conn: conn.close()

    # Routes that differ only by an anchor (e.g. a Filter's True and False outputs) look the same; show them once
    chains = list({tuple(tuple(step.items()) for step in chain): chain for chain in chains}.values())
    if len(chains) <= 1:
        return pd.DataFrame(chains[0] if chains else [])
    return pd.DataFrame([{'Branch': i, **step} for i, chain in enumerate(chains, start=1) for step in chain])


def _trace_upstream_chains(conn, workflow_id, tool_id, field_name, io_map, aliases, ancestors, memo):
    """
    (Helper) Returns the lineage chains (origin first) that end at this tool, following producing workflows.
    ancestors holds the tools already on this route, to stop at a workflow that reads back its own output;
    memo reuses the result when two routes reach the same producer.
    """
    key = (workflow_id, tool_id, field_name)
    if key in ancestors: return []
    if key in memo: return memo[key]
    ancestors = ancestors | {key}

    wf_df = pd.read_sql_query("SELECT workflow_name FROM workflows WHERE id = ?", conn, params=(workflow_id,))
    if wf_df.empty:
        print(f"Error: Could not find workflow with ID {workflow_id}. The I/O map may be stale. Skipping this branch of the trace.")
        return []
    workflow_name = wf_df.iloc[0]['workflow_name']

    chains = []
    for path, origin in _trace_within_workflow(conn, workflow_id, tool_id, field_name):
        steps = [{**step, 'Workflow': workflow_name} for step in reversed(path)]
        upstream = []
        if origin and origin['type'] == 'INPUT' and origin.get('source_detail'):
            producers = _producers_for(io_map, lineage_key(origin.get('io_item'), aliases), origin.get('io_item'))
            # A file this route already wrote (a workflow re-reading its own output) is treated as the origin
            producers = [p for p in producers if (p['workflow_db_id'], p['tool_db_id'], origin['field_name']) not in ancestors]
            if producers:
                link = {
                    'Workflow': f"--- Link via {ntpath.basename(origin['source_detail'])} ---",
                    'Tool ID': '-->', 'Plugin': '-->', 'Field Name': '-->', 'Transformation': '-->'
                }
                for producer in producers:
                    producer_chains = _trace_upstream_chains(conn, producer['workflow_db_id'], producer['tool_db_id'],
                                                             origin['field_name'], io_map, aliases, ancestors, memo)
                    upstream.extend(chain + [link] for chain in producer_chains)
                upstream = upstream or [[link]]
            elif steps:
                steps[0]['Transformation'] = 'Origin Field'
        elif steps and (not origin or origin['type'] != 'CREATED'):
            steps[0]['Transformation'] = 'Origin Field'
        chains.extend([chain + steps for chain in upstream] or [steps])
    memo[key] = chains[:MAX_BRANCHES]
    return memo[key]

def trace_downstream_end_to_end(workspace_name, start_workflow_id, start_tool_id, start_field_name, io_map):
    """Traces a field's usage forward across all workflows to all its final destinations."""
    conn = create_connection()
    if not conn: return pd.DataFrame()

    final_endpoints = []
    aliases = get_path_aliases(workspace_name)

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
                normalized_path = lineage_key(cont.get('io_item'), aliases)
                if normalized_path in io_map and io_map[normalized_path].get('consumers'):
                    for consumer in io_map[normalized_path]['consumers']:
                        queue.append((consumer['workflow_db_id'], consumer['tool_db_id'], cont['field_name']))
    finally:
        if conn: conn.close()

    return pd.DataFrame(final_endpoints)


def _get_tool(conn, tool_db_id):
    tool_df = pd.read_sql_query("SELECT id, tool_id_xml, plugin, config_xml, macro, annotation FROM tools WHERE id = ?", conn, params=(int(tool_db_id),))
    return None if tool_df.empty else tool_df.iloc[0]


def _upstream_branches(conn, workflow_id, plugin, config_xml, upstream_rows, field_name, out_field_name, via_anchor):
    """
    (Helper) Picks which inputs of a tool a field comes from. field_name is the field's name before this
    tool's renames, out_field_name its name in the tool's output. Returns (branches, confirmed): a list of
    (tool_db_id, field name in that input, origin anchor), and whether the choice was confirmed against
    the inputs' schemas rather than guessed.
    """
    join = is_join(plugin)
    # On a Join's J output, an input only qualifies if the Join's select settings map it to this field
    check_select = join and via_anchor not in ('Left', 'Right') and bool(config_xml)
    candidates = []
    for row in upstream_rows.itertuples():
        # A Join's L and R outputs carry only the unjoined records (and fields) of that input
        if join and via_anchor in ('Left', 'Right') and row.destination_anchor and row.destination_anchor != via_anchor:
            continue
        prev_df = pd.read_sql_query("SELECT id FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(workflow_id, row.origin_tool_id_xml))
        if prev_df.empty: continue
        names = [field_name]
        # Join select lists prefix fields with their input, e.g. 'Right_Amount'
        if row.destination_anchor and field_name.startswith(f"{row.destination_anchor}_"):
            names.insert(0, field_name[len(row.destination_anchor) + 1:])
        if check_select and row.destination_anchor in ('Left', 'Right'):
            names = [n for n in names if find_destination_field_name(config_xml, plugin, n, row.destination_anchor) == out_field_name]
            if not names: continue
        candidates.append((int(prev_df.iloc[0]['id']), names, row.origin_anchor, row.destination_anchor))

    matches = []
    for prev_id, names, origin_anchor, dest_anchor in candidates:
        schema_df = find_upstream_fields_for_tool(conn, workflow_id, prev_id, origin_anchor)
        schema = set(schema_df['field_name']) if not schema_df.empty else set()
        name = next((n for n in names if n in schema), None)
        if name: matches.append((prev_id, name, origin_anchor, dest_anchor))
    if matches:
        if join and len(matches) > 1:
            # A name on both sides of a Join keeps its plain name only on the Left; the Right copy becomes 'Right_...'
            matches = [m for m in matches if m[3] == 'Left'] or matches
        return [(prev_id, name, origin_anchor) for prev_id, name, origin_anchor, _ in matches], True

    # No schema to check against (e.g. data parsed before anchors were stored): follow the naming convention
    if join:
        side = 'Right' if field_name.startswith('Right_') else 'Left'
        candidates = [c for c in candidates if c[3] in (side, None)] or candidates
    return [(prev_id, names[0], origin_anchor) for prev_id, names, origin_anchor, _ in candidates], False


def _trace_within_workflow(conn, workflow_id, start_tool_id, start_field_name):
    """
    (Helper) Traces a field backward within a single workflow. Returns a list of (path, origin details),
    one per input branch the field comes from (a Union, for example, can have several).
    """
    results = []
    # Each entry carries the tools already on its own route, so two routes that meet again
    # (e.g. a Union of two Selects on the same input) are both kept, while a loop still stops
    stack = [(start_tool_id, start_field_name, None, [], frozenset())]

    while stack and len(results) < MAX_BRANCHES:
        tool_db_id, field_name, via_anchor, path, ancestors = stack.pop()
        if (tool_db_id, field_name, via_anchor) in ancestors: continue
        ancestors = ancestors | {(tool_db_id, field_name, via_anchor)}

        tool_info = _get_tool(conn, tool_db_id)
        if tool_info is None or len(path) >= MAX_STEPS:
            results.append((path, None))
            continue
        plugin = tool_info['plugin']
        plugin_label = f"Macro: {ntpath.basename(tool_info['macro'])}" if tool_info['macro'] else plugin.split('.')[-1]
        step = {'Tool ID': tool_info['tool_id_xml'], 'Plugin': plugin_label, 'Field Name': field_name, 'Transformation': 'Passthrough'}
        path = path + [step]

        tool_dict = {
            'tool_db_id': tool_db_id,
            'id': tool_info['tool_id_xml'],
            'plugin': plugin,
            'config_xml': tool_info['config_xml'],
            'macro': tool_info['macro'],
            'annotation': tool_info['annotation']
        }
        inputs, _ = extract_io_tools([tool_dict])
        input_origin = {'type': 'INPUT', 'source_detail': inputs[0]['source_detail'] if inputs else 'Unknown', 'field_name': field_name, 'io_item': inputs[0] if inputs else None}
        if any(ptype in plugin for ptype in ORIGIN_PLUGINS):
            results.append((path, input_origin))
            continue

        upstream_rows = pd.read_sql_query(
            "SELECT origin_tool_id_xml, origin_anchor, destination_anchor FROM connections WHERE workflow_id = ? AND destination_tool_id_xml = ?",
            conn, params=(workflow_id, tool_info['tool_id_xml']))
        if upstream_rows.empty:
            results.append((path, input_origin if inputs else {'type': 'NO_INPUT_CONNECTION'}))
            continue

        if is_join(plugin) and via_anchor in ('Left', 'Right'):
            next_step = field_name  # reached through an unjoined output, which the Join's select settings don't touch
        else:
            next_step = find_origin_field_name(tool_info['config_xml'], plugin, field_name)
        if isinstance(next_step, dict) and next_step.get('status') == 'CREATED':
            step['Transformation'] = f"Created with expression: {next_step['expression']}"
            results.append((path, {'type': 'CREATED'}))
            continue
        if isinstance(next_step, dict) and next_step.get('status') == 'UPDATED':
            # The expression reads the field's own earlier value, so keep tracing it upstream
            step['Transformation'] = f"Updated with expression: {next_step['expression']}"
            next_step = field_name
        if not isinstance(next_step, str):
            results.append((path, None))
            continue
        if next_step != field_name: step['Transformation'] = f"Renamed from '{next_step}'"

        branches, confirmed = _upstream_branches(conn, workflow_id, plugin, tool_info['config_xml'], upstream_rows,
                                                 next_step, field_name, via_anchor)
        # Tools that read data themselves (macros, annotated tools, Download) are the origin
        # unless one of their inputs is known to carry the field
        if inputs and not confirmed:
            results.append((path, input_origin))
            continue
        if not branches:
            results.append((path, None))
            continue
        for prev_id, prev_field, prev_anchor in reversed(branches):
            stack.append((prev_id, prev_field, prev_anchor, path, ancestors))

    return results

def _trace_within_workflow_downstream(conn, wf_id, start_tool_db_id, start_field_name):
    """(Helper) Traces a field forward within a single workflow. Returns final outputs and continuation points."""
    endpoints, continuations = [], []
    start_tool = _get_tool(conn, start_tool_db_id)
    if start_tool is None:
        return endpoints, continuations  # the workflow was re-parsed since the trace started

    # Entries: (tool, field name, input anchor it arrived on, whether it is the starting tool). A field queued
    # for a Join still has its input name; the Join's select settings are applied as it leaves. The starting
    # tool's field is already in its output form.
    queue = deque([(start_tool['tool_id_xml'], start_field_name, None, True)])
    visited = set()

    while queue:
        tool_xml_id, field_name, in_anchor, is_start = queue.popleft()
        if (tool_xml_id, field_name, in_anchor, is_start) in visited: continue
        visited.add((tool_xml_id, field_name, in_anchor, is_start))

        tool_info_df = pd.read_sql_query("SELECT id, plugin, config_xml, macro, annotation FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(wf_id, tool_xml_id))
        if tool_info_df.empty: continue
        tool_info = tool_info_df.iloc[0]

        tool_dict = {
            'tool_db_id': tool_info['id'],
            'id': tool_xml_id,
            'plugin': tool_info['plugin'],
            'config_xml': tool_info['config_xml'],
            'macro': tool_info['macro'],
            'annotation': tool_info['annotation'],
        }
        # Any tool that writes data: Output Data, In-DB, Blob, Upload, or a tool with a lineage annotation
        _, outputs = extract_io_tools([tool_dict])
        for output in outputs:
            endpoints.append({'Final Field Name': field_name, 'Output Tool ID': tool_xml_id, 'Destination': output['source_detail']})
            continuations.append({'source_detail': output['source_detail'], 'field_name': field_name, 'io_item': output})

        joining = is_join(tool_info['plugin'])
        destination_df = pd.read_sql_query("SELECT destination_tool_id_xml, origin_anchor, destination_anchor FROM connections WHERE workflow_id = ? AND origin_tool_id_xml = ?", conn, params=(wf_id, tool_xml_id))
        for _, row in destination_df.iterrows():
            # A field entering a Join on one side never leaves through the other side's unjoined output
            if joining and in_anchor in ('Left', 'Right') and row['origin_anchor'] in ('Left', 'Right') and row['origin_anchor'] != in_anchor:
                continue
            out_field_name = field_name
            # A Join's select settings apply to its J output only; the unjoined L and R outputs keep the original fields
            if joining and not is_start and row['origin_anchor'] in ('Join', None):
                out_field_name = find_destination_field_name(tool_info['config_xml'], tool_info['plugin'], field_name, in_anchor)
                if out_field_name is None: continue  # deselected in the Join
            next_tool_xml_id = row['destination_tool_id_xml']
            next_tool_details_df = pd.read_sql_query("SELECT plugin, config_xml FROM tools WHERE workflow_id = ? AND tool_id_xml = ?", conn, params=(wf_id, next_tool_xml_id))
            if next_tool_details_df.empty: continue
            next_tool_info = next_tool_details_df.iloc[0]
            next_field_name = out_field_name
            if not is_join(next_tool_info['plugin']):
                next_field_name = find_destination_field_name(next_tool_info['config_xml'], next_tool_info['plugin'], out_field_name, row['destination_anchor'])
                if next_field_name is None: continue  # the field is deselected there
            queue.append((next_tool_xml_id, next_field_name, row['destination_anchor'], False))

    return endpoints, continuations

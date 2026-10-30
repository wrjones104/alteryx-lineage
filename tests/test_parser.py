import os
from conftest import FIXTURES
from parser import (parse_workflow, extract_io_tools, split_source, classify_source, data_store_key,
                    resolve_path, wildcard_match, find_paths_in_config,
                    FLAG_DYNAMIC, FLAG_RELATIVE, FLAG_MULTI_FILE, FLAG_WILDCARD)


def _io_for(name):
    tools, _, _ = parse_workflow(os.path.join(FIXTURES, name))
    records = [{**t, 'tool_db_id': t['id'], 'workflow_name': name} for t in tools]
    inputs, outputs = extract_io_tools(records)
    return {i['tool_db_id']: i for i in inputs}, {o['tool_db_id']: o for o in outputs}


def test_parse_workflow_returns_metadata():
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, 'Extract_GL.yxmd'))
    assert len(tools) == 6 and len(connections) == 4
    assert meta == {'alteryx_version': '2023.1', 'description': 'Loads and cleans the daily GL extract.',
                    'author': 'Finance Analytics'}


def test_parse_failure_returns_none():
    assert parse_workflow(os.path.join(FIXTURES, 'Broken.yxmd')) == (None, None, None)


def test_split_source_normalizes_excel_sheets():
    assert split_source(r'\\srv\share\book.xlsx|||`Summary$`') == (r'\\srv\share\book.xlsx', 'Summary')
    assert split_source(r'\\srv\share\book.xlsx|||Summary') == (r'\\srv\share\book.xlsx', 'Summary')
    assert split_source('aka:Warehouse|||SELECT * FROM t') == ('aka:Warehouse', 'SELECT * FROM t')
    assert split_source(r'C:\data.csv') == (r'C:\data.csv', None)


def test_classify_source():
    assert classify_source(r'\\srv\share\a.csv') == 'File'
    assert classify_source(r'S:\a.csv') == 'File'
    assert classify_source(r'output\final.csv') == 'File'
    assert classify_source('aka:Warehouse|||SELECT 1') == 'Database'
    assert classify_source('odbc:DSN=Prod;UID=x|||dbo.t') == 'Database'
    assert classify_source('https://example.com/api') == 'Web/API'
    assert classify_source('Not Found') == 'Unknown'


def test_keys_ignore_case_sheet_and_mapped_drive():
    aliases = (('S:', r'\\fileserver\finance'),)
    written = data_store_key(r'\\fileserver\finance\reports\GL Summary.xlsx|||Summary', aliases)
    read = data_store_key(r'\\FileServer\Finance\reports\GL Summary.xlsx|||`Summary$`', aliases)
    assert written == read
    assert data_store_key(r'S:\staging\GL_Clean.yxdb', aliases) == data_store_key(r'\\fileserver\finance\staging\gl_clean.yxdb')
    assert resolve_path(r'S:\staging\GL_Clean.yxdb', aliases) == r'\\fileserver\finance\staging\GL_Clean.yxdb'
    assert data_store_key('Not Found') is None


def test_wildcard_match_stays_in_folder():
    pattern = data_store_key(r'\\srv\share\raw\gl_*.csv')
    assert wildcard_match(pattern, data_store_key(r'\\srv\share\raw\gl_2026.csv'))
    assert not wildcard_match(pattern, data_store_key(r'\\srv\share\raw\sub\gl_2026.csv'))


def test_extract_io_tools_structured_fields():
    inputs, outputs = _io_for('Report_GL.yxmd')
    assert set(inputs) == {'1', '2', '3', '4', '6'}
    assert set(outputs) == {'5', '9'}

    assert inputs['3']['is_dynamic'] and FLAG_DYNAMIC in inputs['3']['flags']
    assert inputs['3']['source_detail'] == r'Dynamic from template: \\fileserver\finance\templates\budget_template.csv'
    assert inputs['4']['data_store_type'] == 'Database'
    assert inputs['6']['tool_type'] == 'Input Data Selector (Macro)'
    assert {FLAG_RELATIVE, FLAG_MULTI_FILE} <= set(outputs['5']['flags'])
    # Manual annotations now carry the real path so they can link to other workflows
    assert outputs['9']['path'] == r'\\fileserver\finance\exports\treasury.csv'

    inputs, outputs = _io_for('Extract_GL.yxmd')
    assert FLAG_WILDCARD in inputs['1']['flags']
    assert outputs['4']['write_mode'] == 'Overwrite'


def test_find_paths_in_config():
    _, outputs = _io_for('Extract_GL.yxmd')
    tools, _, _ = parse_workflow(os.path.join(FIXTURES, 'Extract_GL.yxmd'))
    formula = next(t for t in tools if t['id'] == '2')
    assert find_paths_in_config(formula['config_xml']) == ['\\\\fileserver\\finance\\archive\\']


def _io_item(plugin, config_xml='', annotation=''):
    tool = {'tool_db_id': 1, 'id': '1', 'plugin': plugin, 'config_xml': config_xml, 'annotation': annotation}
    inputs, outputs = extract_io_tools([tool])
    return (inputs + outputs)[0] if inputs or outputs else None


def test_sql_and_annotated_databases_are_not_files():
    assert classify_source("SELECT * FROM dbo.Accounts WHERE name LIKE '%x%'") == 'Database'
    annotation = '--- lineage ---\noutputs:\n  - type: database\n    path: PROD.FINANCE.GL\n---'
    item = _io_item('JupyterCode', annotation=annotation)
    assert item['data_store_type'] == 'Database' and item['flags'] == []


def test_unconfigured_directory_tool_is_not_found():
    item = _io_item('AlteryxBasePluginsGui.Directory.Directory',
                    '<Configuration><Directory /><FileSpec>*.csv</FileSpec></Configuration>')
    assert item['path'] is None and item['source_detail'] == 'Not Found'


def test_quoted_sheet_names_and_alias_order():
    assert split_source(r"\\srv\book.xlsx|||`'GL Summary$'`") == (r'\\srv\book.xlsx', 'GL Summary')
    # The more specific mapping wins even when the shorter one is written with extra slashes
    aliases = ((r'S:\\\\', r'\\srv\root'), (r'S:\AP', r'\\srv\payables'))
    assert resolve_path(r'S:\AP\inv.csv', aliases) == r'\\srv\payables\inv.csv'


def test_fields_are_stored_per_output_anchor():
    tools, _, _ = parse_workflow(os.path.join(FIXTURES, 'Join_Union.yxmd'))
    join = next(t for t in tools if t['id'] == '3')
    by_anchor = {}
    for field in join['output_fields']:
        by_anchor.setdefault(field['anchor'], []).append(field['name'])
    assert by_anchor['Right'] == ['CustomerID', 'Amount', 'OrderDate']
    assert 'OrderDate' not in by_anchor['Join']

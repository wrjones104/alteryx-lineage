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

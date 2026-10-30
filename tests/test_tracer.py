import os
import pytest
import database_manager as db
import tracer
from conftest import FIXTURES
from parser import parse_workflow


@pytest.fixture
def join_workspace(workspace):
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, 'Join_Union.yxmd'))
    db.log_workflow_details('Joins', 'Join_Union.yxmd', tools, connections, meta)
    conn = db.create_connection()
    ids = dict(conn.execute("""SELECT t.tool_id_xml, t.id FROM tools t JOIN workflows w ON t.workflow_id = w.id
                               WHERE w.workflow_name = 'Join_Union.yxmd'""").fetchall())
    workflow_id = conn.execute("SELECT id FROM workflows WHERE workflow_name = 'Join_Union.yxmd'").fetchone()[0]
    conn.close()
    return workflow_id, ids


def _upstream(join_workspace, tool, field):
    workflow_id, ids = join_workspace
    return tracer.trace_upstream_end_to_end('Joins', workflow_id, ids[tool], field, tracer.build_io_map('Joins'))


def _downstream(join_workspace, tool, field):
    workflow_id, ids = join_workspace
    endpoints = tracer.trace_downstream_end_to_end('Joins', workflow_id, ids[tool], field, tracer.build_io_map('Joins'))
    return sorted(zip(endpoints['Output Tool ID'], endpoints['Final Field Name'])) if not endpoints.empty else []


def test_field_from_right_side_of_join_is_traced_to_right_input(join_workspace):
    lineage = _upstream(join_workspace, '6', 'Order_CustomerID')
    assert 'Branch' not in lineage.columns
    assert list(lineage['Tool ID']) == ['2', '3', '4', '6']
    assert list(lineage['Field Name']) == ['CustomerID', 'Order_CustomerID', 'Order_CustomerID', 'Order_CustomerID']
    assert lineage.iloc[0]['Transformation'] == 'Origin Field'
    assert lineage.iloc[1]['Transformation'] == "Renamed from 'Right_CustomerID'"


def test_field_on_both_sides_of_join_comes_from_left(join_workspace):
    lineage = _upstream(join_workspace, '3', 'CustomerID')
    assert list(lineage['Tool ID']) == ['1', '3']


def test_union_returns_one_branch_per_input(join_workspace):
    lineage = _upstream(join_workspace, '6', 'Amount')
    assert set(lineage['Branch']) == {1, 2}
    origins = {branch: rows.iloc[0]['Tool ID'] for branch, rows in lineage.groupby('Branch')}
    # Amount comes from orders.csv through the Join, and from the Input Data Selector macro
    assert sorted(origins.values()) == ['2', '5']
    macro_branch = lineage[lineage['Branch'] == next(b for b, t in origins.items() if t == '5')]
    assert macro_branch.iloc[0]['Transformation'] == 'Origin Field'


def test_field_only_in_one_union_input_has_one_branch(join_workspace):
    lineage = _upstream(join_workspace, '6', 'Name')
    assert 'Branch' not in lineage.columns
    assert list(lineage['Tool ID']) == ['1', '3', '4', '6']


def test_downstream_respects_join_outputs_and_selection(join_workspace):
    # Deselected in the Join, but still present in the unjoined Right output
    assert _downstream(join_workspace, '2', 'OrderDate') == [('7', 'OrderDate')]
    # Renamed in the joined output only
    assert _downstream(join_workspace, '2', 'CustomerID') == [('6', 'Order_CustomerID'), ('7', 'CustomerID')]
    # Left-side fields never leave through the Right output
    assert _downstream(join_workspace, '1', 'Name') == [('6', 'Name')]


def test_find_destination_field_name_for_right_duplicate():
    config = ('<Configuration><SelectConfiguration><Configuration><SelectFields>'
              '<SelectField field="Left_ID" selected="True" /><SelectField field="*Unknown" selected="True" />'
              '</SelectFields></Configuration></SelectConfiguration></Configuration>')
    plugin = 'AlteryxBasePluginsGui.Join.Join'
    assert tracer.find_destination_field_name(config, plugin, 'ID', 'Right') == 'Right_ID'
    assert tracer.find_destination_field_name(config, plugin, 'ID', 'Left') == 'ID'


def test_join_right_output_traces_to_right_input(join_workspace):
    # OrderDate is dropped from the J output but still leaves through R
    lineage = _upstream(join_workspace, '7', 'OrderDate')
    assert list(lineage['Tool ID']) == ['2', '3', '7']


def test_downstream_from_a_join_starts_with_output_names(join_workspace):
    # The Join's own fields are already renamed; they are not renamed again on the way out
    assert ('6', 'CustomerID') in _downstream(join_workspace, '3', 'CustomerID')
    assert ('6', 'Order_CustomerID') in _downstream(join_workspace, '3', 'Order_CustomerID')


def test_select_unknown_deselected_drops_field():
    config = ('<Configuration><SelectFields><SelectField field="Keep" selected="True" />'
              '<SelectField field="*Unknown" selected="False" /></SelectFields></Configuration>')
    plugin = 'AlteryxBasePluginsGui.AlteryxSelect.AlteryxSelect'
    assert tracer.find_destination_field_name(config, plugin, 'Keep') == 'Keep'
    assert tracer.find_destination_field_name(config, plugin, 'Other') is None


def test_join_multiple_is_not_a_join():
    from parser import is_join
    assert is_join('AlteryxBasePluginsGui.Join.Join')
    assert not is_join('AlteryxBasePluginsGui.JoinMultiple.JoinMultiple')


@pytest.fixture
def diamond_workspace(workspace):
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, 'Diamond.yxmd'))
    db.log_workflow_details('Ops', 'Diamond.yxmd', tools, connections, meta)
    conn = db.create_connection()
    ids = dict(conn.execute("""SELECT t.tool_id_xml, t.id FROM tools t JOIN workflows w ON t.workflow_id = w.id
                               WHERE w.workflow_name = 'Diamond.yxmd'""").fetchall())
    workflow_id = conn.execute("SELECT id FROM workflows WHERE workflow_name = 'Diamond.yxmd'").fetchone()[0]
    conn.close()
    return workflow_id, ids


def test_routes_that_meet_again_are_all_kept(diamond_workspace):
    workflow_id, ids = diamond_workspace
    lineage = tracer.trace_upstream_end_to_end('Ops', workflow_id, ids['6'], 'B', tracer.build_io_map('Ops'))
    routes = {tuple(rows['Tool ID']) for _, rows in lineage.groupby('Branch')}
    assert routes == {('1', '2', '3', '5', '6'), ('1', '2', '4', '5', '6')}
    # The Formula rewrites B from its own value, so the trace continues past it
    formula = lineage[lineage['Tool ID'] == '3'].iloc[0]
    assert formula['Transformation'] == 'Updated with expression: [B] * 2'
    # history.yxdb is written by this same workflow: it is the origin, not a link back to itself
    assert '-->' not in set(lineage['Tool ID']) and set(lineage['Workflow']) == {'Diamond.yxmd'}
    assert set(lineage[lineage['Tool ID'] == '1']['Transformation']) == {'Origin Field'}


def test_downstream_handles_annotated_outputs_and_stale_ids(workspace):
    conn = db.create_connection()
    workflow_id, tool_id = conn.execute("""SELECT w.id, t.id FROM tools t JOIN workflows w ON t.workflow_id = w.id
                                           WHERE w.workflow_name = 'Report_GL.yxmd' AND t.tool_id_xml = '10'""").fetchone()
    conn.close()
    io_map = tracer.build_io_map(workspace)
    endpoints = tracer.trace_downstream_end_to_end(workspace, workflow_id, tool_id, 'Account', io_map)
    # Tool 9 is a Python tool whose output is declared in a lineage annotation
    assert r'file: \\fileserver\finance\exports\treasury.csv' in set(endpoints['Destination'])
    assert tracer.trace_downstream_end_to_end(workspace, workflow_id, 999999, 'Account', io_map).empty

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

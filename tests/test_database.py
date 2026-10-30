import os
import sqlite3
import database_manager as db
from conftest import FIXTURES
from parser import parse_workflow


def _rows(workspace):
    conn = db.create_connection()
    rows = conn.execute("""SELECT w.workflow_name, w.server_workflow_id, COUNT(t.id) FROM workflows w
                           JOIN workspaces ws ON w.workspace_id = ws.id LEFT JOIN tools t ON t.workflow_id = w.id
                           WHERE ws.name = ? GROUP BY w.id ORDER BY w.id""", (workspace,)).fetchall()
    conn.close()
    return rows


def _log(name, fixture, server_id=None):
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, fixture))
    return db.log_workflow_details('Server', name, tools, connections, meta, server_workflow_id=server_id)


def test_server_workflows_with_same_file_name_are_kept_apart(workspace):
    assert _log('Workflow.yxmd', 'Extract_GL.yxmd', 'id-1') == 'Workflow.yxmd'
    assert _log('Workflow.yxmd', 'Daily_Feed.yxmd', 'id-2') == 'Workflow.yxmd [id-2]'
    assert _rows('Server') == [('Workflow.yxmd', 'id-1', 6), ('Workflow.yxmd [id-2]', 'id-2', 2)]

    # Re-downloading either one updates its own row
    assert _log('Workflow.yxmd', 'Daily_Feed.yxmd', 'id-2') == 'Workflow.yxmd [id-2]'
    assert _log('Workflow.yxmd', 'Extract_GL.yxmd', 'id-1') == 'Workflow.yxmd'
    assert _rows('Server') == [('Workflow.yxmd', 'id-1', 6), ('Workflow.yxmd [id-2]', 'id-2', 2)]


def test_rows_saved_before_server_ids_are_adopted(workspace):
    assert _log('Legacy.yxmd', 'Daily_Feed.yxmd') == 'Legacy.yxmd'
    assert _log('Legacy.yxmd', 'Extract_GL.yxmd', 'id-9') == 'Legacy.yxmd'
    assert _rows('Server') == [('Legacy.yxmd', 'id-9', 6)]
    # A local re-upload of the same file name still replaces it
    assert _log('Legacy.yxmd', 'Daily_Feed.yxmd') == 'Legacy.yxmd'
    assert _rows('Server') == [('Legacy.yxmd', 'id-9', 2)]


def test_connection_anchors_are_stored(workspace):
    conn = db.create_connection()
    anchors = conn.execute("""SELECT c.origin_anchor, c.destination_anchor FROM connections c
                              JOIN workflows w ON c.workflow_id = w.id
                              WHERE w.workflow_name = 'Report_GL.yxmd' AND c.destination_tool_id_xml = '10'
                              ORDER BY c.destination_anchor""").fetchall()
    conn.close()
    assert anchors == [('Output', 'Left'), ('Output', 'Right')]


def test_old_database_is_migrated(tmp_path, monkeypatch):
    monkeypatch.setattr(db, 'DB_FILE', str(tmp_path / 'old.db'))
    conn = sqlite3.connect(db.DB_FILE)
    conn.execute("CREATE TABLE workflows (id INTEGER PRIMARY KEY, workspace_id INTEGER NOT NULL, workflow_name TEXT NOT NULL, last_parsed_at TEXT NOT NULL, UNIQUE(workspace_id, workflow_name))")
    conn.execute("CREATE TABLE connections (id INTEGER PRIMARY KEY, workflow_id INTEGER NOT NULL, origin_tool_id_xml TEXT NOT NULL, destination_tool_id_xml TEXT NOT NULL)")
    conn.commit()
    conn.close()
    db.create_tables()
    db.create_tables()
    conn = sqlite3.connect(db.DB_FILE)
    workflow_columns = {row[1] for row in conn.execute("PRAGMA table_info(workflows)")}
    connection_columns = {row[1] for row in conn.execute("PRAGMA table_info(connections)")}
    conn.close()
    assert {'server_workflow_id', 'owner', 'author'} <= workflow_columns
    assert {'origin_anchor', 'destination_anchor'} <= connection_columns

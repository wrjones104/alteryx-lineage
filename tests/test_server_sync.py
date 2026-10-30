import os
import numpy as np
import pandas as pd
import requests
import database_manager as db
import server_client
import server_sync
from conftest import FIXTURES
from parser import parse_workflow

SERVER = 'https://gallery.example.com/'


def _download(name, server_id, version, url=SERVER, fixture='Daily_Feed.yxmd'):
    """Saves a workflow the way Home.py does after a server download."""
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, fixture))
    meta.update(server_sync.download_meta(_published(server_id, version, name=name), url))
    return db.log_workflow_details('Server', name, tools, connections, meta, server_workflow_id=server_id)


def _statuses(server_workflows, server_url=SERVER):
    result = server_sync.compare_with_server(db.get_server_workflows_in_workspace('Server'), server_workflows, server_url)
    return dict(zip(result['Workflow'], result['Status']))


def _published(server_id, version, name='wf'):
    return {'id': server_id, 'name': name, 'ownerName': 'Pat', 'publishedVersionNumber': version}


def test_changed_versions_are_flagged(workspace):
    _download('A.yxmd', 'id-a', 3)
    _download('B.yxmd', 'id-b', 5)
    _download('C.yxmd', 'id-c', 2)
    statuses = _statuses([_published('id-a', 4), _published('id-b', 5), _published('id-c', 1)])
    # C was rolled back on the server: it no longer matches either
    assert statuses == {'A.yxmd': server_sync.STATUS_UPDATE, 'B.yxmd': server_sync.STATUS_CURRENT,
                        'C.yxmd': server_sync.STATUS_UPDATE}


def test_local_uploads_are_not_compared(workspace):
    # The workspace fixture's three workflows are local uploads with no server ID
    _download('A.yxmd', 'id-a', 3)
    assert set(_statuses([_published('id-a', 3)])) == {'A.yxmd'}


def test_missing_and_other_server_workflows(workspace):
    _download('Gone.yxmd', 'id-gone', 1)
    _download('Elsewhere.yxmd', 'id-x', 1, url='https://other.example.com')
    statuses = _statuses([], SERVER.upper())
    assert statuses == {'Gone.yxmd': server_sync.STATUS_MISSING, 'Elsewhere.yxmd': server_sync.STATUS_OTHER_SERVER}


def test_rows_from_before_versions_were_stored_use_the_source_reference(workspace):
    _download('Old.yxmd', 'id-old', 7)
    conn = db.create_connection()
    conn.execute("UPDATE workflows SET server_version = NULL, server_url = NULL")
    conn.commit()
    conn.close()
    assert _statuses([_published('id-old', 7)]) == {'Old.yxmd': server_sync.STATUS_CURRENT}
    assert _statuses([_published('id-old', 8)]) == {'Old.yxmd': server_sync.STATUS_UPDATE}


def test_unknown_versions_can_be_redownloaded(workspace):
    _download('NoVersion.yxmd', 'id-n', 'N/A')
    _download('Replaced.yxmd', 'id-r', 2)
    # A local upload of the same file name replaces the server download's contents (as Home.py saves it)
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, 'Extract_GL.yxmd'))
    db.log_workflow_details('Server', 'Replaced.yxmd', tools, connections,
                            {**meta, 'source': 'Local Upload', 'server_version': None, 'source_ref': None})
    statuses = _statuses([_published('id-n', 3), _published('id-r', 2)])
    assert statuses == {'NoVersion.yxmd': server_sync.STATUS_UNKNOWN, 'Replaced.yxmd': server_sync.STATUS_LOCAL}
    assert set(statuses.values()) <= set(server_sync.DOWNLOADABLE)


def test_versions_from_a_float_column_match(workspace):
    # One workflow without a version makes pandas store the others as floats (3 -> 3.0)
    gallery = pd.DataFrame([_published('id-a', 3), _published('id-b', None)])
    assert gallery['publishedVersionNumber'].dtype == 'float64'
    assert server_sync.version_text(gallery.iloc[0]['publishedVersionNumber']) == '3'
    assert server_sync.version_text(np.float64('nan')) is None
    _download('A.yxmd', 'id-a', gallery.iloc[0]['publishedVersionNumber'])
    # A source reference written from a float column before this fix
    _download('Old.yxmd', 'id-old', 5)
    conn = db.create_connection()
    conn.execute("UPDATE workflows SET server_version = NULL, server_url = NULL, "
                 "source_ref = 'Old (id id-old, v5.0) on https://gallery.example.com' WHERE server_workflow_id = 'id-old'")
    conn.commit()
    conn.close()
    assert _statuses([_published('id-a', 3), _published('id-old', 5)]) == {
        'A.yxmd': server_sync.STATUS_CURRENT, 'Old.yxmd': server_sync.STATUS_CURRENT}


def test_same_id_on_another_server_is_not_matched(workspace):
    # A Gallery copied from Dev to Prod keeps its workflow IDs
    _download('DevOnly.yxmd', 'shared-id', 2, url='https://dev.example.com')
    result = server_sync.compare_with_server(db.get_server_workflows_in_workspace('Server'),
                                             [_published('shared-id', 5, name='Prod Copy')], SERVER)
    row = result.iloc[0]
    assert (row['Status'], row['Server Name'], row['Server Version']) == (server_sync.STATUS_OTHER_SERVER, '', '')
    assert row['Status'] not in server_sync.DOWNLOADABLE


def test_nan_for_missing_values_does_not_crash(workspace):
    _download('Modern.yxmd', 'id-mod', 3)
    _download('Legacy.yxmd', 'id-leg', 7)
    conn = db.create_connection()
    conn.execute("UPDATE workflows SET server_version = NULL, server_url = NULL, owner = NULL WHERE server_workflow_id = 'id-leg'")
    conn.commit()
    conn.close()
    # pandas 3 reads SQL NULLs in text columns as NaN, not None
    with pd.option_context('future.infer_string', True):
        workspace_df = db.get_server_workflows_in_workspace('Server')
    legacy = workspace_df[workspace_df['server_workflow_id'] == 'id-leg'].iloc[0]
    assert isinstance(legacy['server_url'], float)
    result = server_sync.compare_with_server(workspace_df, [_published('id-mod', 4.0), _published('id-leg', 7)], SERVER)
    assert dict(zip(result['Workflow'], result['Status'])) == {
        'Modern.yxmd': server_sync.STATUS_UPDATE, 'Legacy.yxmd': server_sync.STATUS_CURRENT}


def test_workflow_list_errors_are_told_apart_from_an_empty_list(monkeypatch):
    def fail(*args, **kwargs):
        raise requests.exceptions.ConnectionError('expired')
    monkeypatch.setattr(server_client, 'get_user_map', lambda base_url: {})
    monkeypatch.setattr(server_client.requests, 'get', fail)
    assert server_client.get_workflows('https://gallery.example.com', 'key') is None

    class EmptyList:
        def raise_for_status(self): pass
        def json(self): return []
    monkeypatch.setattr(server_client.requests, 'get', lambda *args, **kwargs: EmptyList())
    assert server_client.get_workflows('https://gallery.example.com', 'key') == []


def test_redownload_records_the_new_version(workspace):
    _download('A.yxmd', 'id-a', 3)
    assert _download('A.yxmd', 'id-a', 4, fixture='Extract_GL.yxmd') == 'A.yxmd'
    assert _statuses([_published('id-a', 4)]) == {'A.yxmd': server_sync.STATUS_CURRENT}
    # Still one row, now holding the new version's tools
    conn = db.create_connection()
    tools = conn.execute("""SELECT COUNT(*) FROM tools t JOIN workflows w ON t.workflow_id = w.id
                            WHERE w.server_workflow_id = 'id-a'""").fetchone()[0]
    conn.close()
    assert tools == 6

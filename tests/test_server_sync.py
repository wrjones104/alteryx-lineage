import os
import database_manager as db
import server_sync
from conftest import FIXTURES
from parser import parse_workflow

SERVER = 'https://gallery.example.com/'


def _download(name, server_id, version, url=SERVER, fixture='Daily_Feed.yxmd'):
    """Saves a workflow the way Home.py does after a server download."""
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, fixture))
    meta.update({'source': 'Alteryx Server', 'source_ref': f"{name} (id {server_id}, v{version}) on {url}",
                 'server_version': server_sync.version_text(version), 'server_url': server_sync.normalize_url(url)})
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
    # A local upload of the same file name replaces the server download's contents
    tools, connections, meta = parse_workflow(os.path.join(FIXTURES, 'Extract_GL.yxmd'))
    db.log_workflow_details('Server', 'Replaced.yxmd', tools, connections,
                            {**meta, 'source': 'Local Upload', 'server_version': None})
    statuses = _statuses([_published('id-n', 3), _published('id-r', 2)])
    assert statuses == {'NoVersion.yxmd': server_sync.STATUS_UNKNOWN, 'Replaced.yxmd': server_sync.STATUS_UNKNOWN}
    assert set(statuses.values()) <= set(server_sync.DOWNLOADABLE)


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

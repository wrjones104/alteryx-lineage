"""Compares the server workflows in a workspace with the versions currently published on the server."""
import re
import pandas as pd

STATUS_UPDATE = 'Update available'
STATUS_CURRENT = 'Up to date'
STATUS_UNKNOWN = 'Version unknown'
STATUS_MISSING = 'Not found on server'
STATUS_OTHER_SERVER = 'From another server'
# Statuses that a re-download can resolve
DOWNLOADABLE = (STATUS_UPDATE, STATUS_UNKNOWN)

# Rows downloaded before the version had its own column: "<name> (id <id>, v<version>) on <url>"
_SOURCE_REF = re.compile(r'\(id [^,()]*, v(?P<version>[^)]*)\) on (?P<url>.*)$')

COLUMNS = ['Workflow', 'Server Name', 'Owner', 'Downloaded Version', 'Server Version', 'Last Parsed', 'Status',
           'server_workflow_id']


def normalize_url(url):
    return (url or '').strip().rstrip('/').lower()


def version_text(version):
    """A published version number as text, or None when the server didn't give one."""
    text = '' if version is None or pd.isna(version) else str(version).strip()
    return text if text and text.upper() != 'N/A' else None


def _downloaded_from(row):
    """(version, server url) the workspace row was downloaded at; either can be None."""
    version, url = version_text(row.get('server_version')), normalize_url(row.get('server_url')) or None
    match = _SOURCE_REF.search(row.get('source_ref') or '')
    if match:
        version = version or version_text(match['version'])
        url = url or normalize_url(match['url']) or None
    # A local upload that replaced the download no longer matches any server version
    if row.get('source') == 'Local Upload' and not version_text(row.get('server_version')):
        version = None
    return version, url


def compare_with_server(workspace_df, server_workflows, server_url):
    """
    One row per server workflow in the workspace (rows from db.get_server_workflows_in_workspace),
    checked against the server's workflow list (server_client.get_workflows) for the connected server_url.
    """
    if workspace_df is None or workspace_df.empty:
        return pd.DataFrame(columns=COLUMNS)
    on_server = {str(wf['id']): wf for wf in server_workflows or []}
    connected = normalize_url(server_url)
    rows = []
    for row in workspace_df.to_dict('records'):
        server_id = str(row['server_workflow_id'])
        downloaded, url = _downloaded_from(row)
        server_wf = on_server.get(server_id)
        published = version_text(server_wf.get('publishedVersionNumber')) if server_wf else None
        if server_wf is None:
            status = STATUS_OTHER_SERVER if url and url != connected else STATUS_MISSING
        elif not downloaded or not published:
            status = STATUS_UNKNOWN
        else:
            # Any difference counts, since a server admin can also roll the published version back
            status = STATUS_CURRENT if downloaded == published else STATUS_UPDATE
        rows.append({
            'Workflow': row['workflow_name'],
            'Server Name': server_wf.get('name') if server_wf else '',
            'Owner': (server_wf.get('ownerName') if server_wf else None) or row.get('owner') or '',
            'Downloaded Version': downloaded or '',
            'Server Version': published or '',
            'Last Parsed': row.get('last_parsed_at') or '',
            'Status': status,
            'server_workflow_id': server_id,
        })
    order = {STATUS_UPDATE: 0, STATUS_UNKNOWN: 1, STATUS_MISSING: 2, STATUS_OTHER_SERVER: 3, STATUS_CURRENT: 4}
    result = pd.DataFrame(rows, columns=COLUMNS)
    return result.sort_values(['Status', 'Workflow'], key=lambda s: s.map(order) if s.name == 'Status' else s.str.lower(),
                              ignore_index=True)

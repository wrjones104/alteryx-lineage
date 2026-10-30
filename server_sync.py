"""Compares the server workflows in a workspace with the versions currently published on the server."""
import re
import pandas as pd

STATUS_UPDATE = 'Update available'
STATUS_CURRENT = 'Up to date'
STATUS_UNKNOWN = 'Version unknown'
STATUS_LOCAL = 'Replaced by local upload'
STATUS_MISSING = 'Not found on server'
STATUS_OTHER_SERVER = 'From another server'
# Statuses that a re-download can resolve. Only STATUS_UPDATE is preselected: a local upload may hold
# unpublished edits, so overwriting it is the user's choice.
DOWNLOADABLE = (STATUS_UPDATE, STATUS_UNKNOWN, STATUS_LOCAL)

# Rows downloaded before the version had its own column: "<name> (id <id>, v<version>) on <url>"
_SOURCE_REF = re.compile(r'\(id [^,()]*, v(?P<version>[^)]*)\) on (?P<url>.*)$')
# '3.0' from a version column that pandas turned into floats because one workflow had no version
_INTEGRAL = re.compile(r'(\d+)\.0+')

COLUMNS = ['Workflow', 'Server Name', 'Owner', 'Downloaded Version', 'Server Version', 'Last Parsed', 'Status',
           'server_workflow_id']


def _clean_text(value):
    """Text with SQL NULLs as ''. pandas can hand them over as None or as NaN, which is truthy."""
    return '' if value is None or (not isinstance(value, str) and pd.isna(value)) else str(value).strip()


def normalize_url(url):
    return _clean_text(url).rstrip('/').lower()


def version_text(version):
    """A published version number as text ('3', never '3.0'), or None when the server didn't give one."""
    if isinstance(version, float) and version.is_integer():  # np.float64 too
        version = int(version)
    text = _clean_text(version)
    if not text or text.upper() == 'N/A':
        return None
    integral = _INTEGRAL.fullmatch(text)
    return integral[1] if integral else text


def download_meta(server_wf, server_url):
    """Workflow metadata to save with a server download; server_wf is one entry of server_client.get_workflows."""
    version = version_text(server_wf.get('publishedVersionNumber'))
    return {
        'source': 'Alteryx Server',
        'source_ref': f"{server_wf.get('name')} (id {server_wf.get('id')}, v{version or 'N/A'}) on {server_url}",
        'owner': server_wf.get('ownerName'),
        # Kept so the workspace can be checked for newer published versions later
        'server_version': version,
        'server_url': normalize_url(server_url),
    }


def _downloaded_from(row):
    """(version, server url) the workspace row was downloaded at; either can be None."""
    version, url = version_text(row.get('server_version')), normalize_url(row.get('server_url')) or None
    match = _SOURCE_REF.search(_clean_text(row.get('source_ref')))
    if match:
        version = version or version_text(match['version'])
        url = url or normalize_url(match['url']) or None
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
        server_id = _clean_text(row.get('server_workflow_id'))
        downloaded, url = _downloaded_from(row)
        # A recorded URL is checked before the ID: a Gallery copied to another server (e.g. Dev from Prod)
        # keeps its workflow IDs, and a match there must not replace this server's download. A server
        # reached under another host name shows here too; connect with the URL it was downloaded from.
        other_server = bool(url and url != connected)
        server_wf = None if other_server else on_server.get(server_id)
        published = version_text(server_wf.get('publishedVersionNumber')) if server_wf else None
        if other_server:
            status = STATUS_OTHER_SERVER
        elif server_wf is None:
            status = STATUS_MISSING
        elif _clean_text(row.get('source')) == 'Local Upload':
            status = STATUS_LOCAL
        elif not downloaded or not published:
            status = STATUS_UNKNOWN
        else:
            # Any difference counts, since a server admin can also roll the published version back
            status = STATUS_CURRENT if downloaded == published else STATUS_UPDATE
        rows.append({
            'Workflow': _clean_text(row.get('workflow_name')),
            'Server Name': _clean_text(server_wf.get('name')) if server_wf else '',
            'Owner': (_clean_text(server_wf.get('ownerName')) if server_wf else '') or _clean_text(row.get('owner')),
            'Downloaded Version': downloaded or '',
            'Server Version': published or '',
            'Last Parsed': _clean_text(row.get('last_parsed_at')),
            'Status': status,
            'server_workflow_id': server_id,
        })
    order = {STATUS_UPDATE: 0, STATUS_UNKNOWN: 1, STATUS_LOCAL: 2, STATUS_MISSING: 3, STATUS_OTHER_SERVER: 4, STATUS_CURRENT: 5}
    result = pd.DataFrame(rows, columns=COLUMNS)
    return result.sort_values(['Status', 'Workflow'], key=lambda s: s.map(order) if s.name == 'Status' else s.str.lower(),
                              ignore_index=True)

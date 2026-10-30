import sqlite3
from sqlite3 import Error
import os
import streamlit as st
import json
import keyring
from keyring.errors import KeyringError
import pandas as pd
from collections import deque

DB_FILE = os.path.join("data", "lineage.db")
CONNECTIONS_FILE = os.path.join("data", "connections.json")
KEYRING_SERVICE = "alteryx-lineage"
WORKFLOW_META_COLUMNS = ('source', 'source_ref', 'owner', 'description', 'author', 'alteryx_version')

def _store_secret(conn_name, client_secret):
    """Stores a client secret in the OS credential store (Windows Credential Manager). Returns success."""
    try:
        keyring.set_password(KEYRING_SERVICE, conn_name, client_secret)
        return True
    except KeyringError as e:
        print(f"Could not store secret for '{conn_name}' in the credential store: {e}")
        return False

def _read_secret(conn_name):
    try:
        return keyring.get_password(KEYRING_SERVICE, conn_name) or ''
    except KeyringError as e:
        print(f"Could not read secret for '{conn_name}' from the credential store: {e}")
        return ''

def _read_connections_file():
    if not os.path.exists(CONNECTIONS_FILE):
        return {}
    try:
        with open(CONNECTIONS_FILE, 'r') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError):
        return {}

def _write_connections_file(connections):
    try:
        os.makedirs(os.path.dirname(CONNECTIONS_FILE), exist_ok=True)
        with open(CONNECTIONS_FILE, 'w') as f:
            json.dump(connections, f, indent=4)
        return True
    except IOError:
        return False

def load_connections():
    """
    Loads saved server connections. The JSON file holds only the URL and client ID; secrets live in
    the OS credential store. Secrets saved in plain text by older versions are moved there on load.
    """
    connections = _read_connections_file()
    migrated = False
    for conn_name, details in connections.items():
        if details.get('client_secret') and _store_secret(conn_name, details['client_secret']):
            del details['client_secret']
            migrated = True
    if migrated:
        _write_connections_file(connections)
    return {name: {**details, 'client_secret': details.get('client_secret') or _read_secret(name)}
            for name, details in connections.items()}

def save_connection(conn_name, url, client_id, client_secret):
    """
    Saves a new or updated server connection. Returns (saved, secret_stored): if the OS credential
    store is unavailable, the URL and client ID are still saved but the secret is not.
    """
    connections = _read_connections_file()
    connections[conn_name] = {'url': url, 'client_id': client_id}
    secret_stored = _store_secret(conn_name, client_secret)
    return _write_connections_file(connections), secret_stored

def create_connection():
    conn = None
    try:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        conn = sqlite3.connect(DB_FILE)
        return conn
    except Error as e:
        print(e)
    return conn

def create_tables():
    conn = create_connection()
    if conn is None: return
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS workspaces (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                id INTEGER PRIMARY KEY, workspace_id INTEGER NOT NULL, workflow_name TEXT NOT NULL,
                last_parsed_at TEXT NOT NULL, FOREIGN KEY (workspace_id) REFERENCES workspaces (id),
                UNIQUE(workspace_id, workflow_name)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tools (
                id INTEGER PRIMARY KEY, workflow_id INTEGER NOT NULL, tool_id_xml TEXT NOT NULL,
                plugin TEXT NOT NULL, annotation TEXT, config_xml TEXT, macro TEXT,
                FOREIGN KEY (workflow_id) REFERENCES workflows (id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tool_fields (
                id INTEGER PRIMARY KEY, tool_id INTEGER NOT NULL, field_name TEXT NOT NULL,
                field_type TEXT, field_size TEXT, field_source TEXT, field_description TEXT,
                FOREIGN KEY (tool_id) REFERENCES tools (id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS connections (
                id INTEGER PRIMARY KEY,
                workflow_id INTEGER NOT NULL,
                origin_tool_id_xml TEXT NOT NULL,
                destination_tool_id_xml TEXT NOT NULL,
                FOREIGN KEY (workflow_id) REFERENCES workflows (id)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS path_aliases (
                id INTEGER PRIMARY KEY, workspace_id INTEGER NOT NULL, prefix TEXT NOT NULL, target TEXT NOT NULL,
                FOREIGN KEY (workspace_id) REFERENCES workspaces (id)
            );
        """)
        # Columns added after the first release; existing databases are migrated in place.
        added_columns = {
            'workflows': WORKFLOW_META_COLUMNS + ('server_workflow_id',),
            'connections': ('origin_anchor', 'destination_anchor'),
        }
        for table, columns in added_columns.items():
            existing_columns = {row[1] for row in cursor.execute(f"PRAGMA table_info({table})")}
            for column in columns:
                if column not in existing_columns:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_workflows_server_id ON workflows (workspace_id, server_workflow_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tools_workflow ON tools (workflow_id, tool_id_xml)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tool_fields_tool ON tool_fields (tool_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_connections_origin ON connections (workflow_id, origin_tool_id_xml)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_connections_destination ON connections (workflow_id, destination_tool_id_xml)")
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn: conn.close()

@st.cache_data(ttl=5)
def get_all_workspaces():
    conn = create_connection()
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM workspaces ORDER BY name")
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Error as e:
            print(e)
        finally:
            if conn: conn.close()
    return []

def _add_workspace(conn, workspace_name):
    cursor = conn.cursor()
    cursor.execute("INSERT INTO workspaces (name) VALUES (?)", (workspace_name,))
    conn.commit()

def _get_or_create_workspace_id(conn, workspace_name):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM workspaces WHERE name = ?", (workspace_name,))
    row = cursor.fetchone()
    if row: return row[0]
    else:
        cursor.execute("INSERT INTO workspaces (name) VALUES (?)", (workspace_name,))
        conn.commit()
        return cursor.lastrowid

def get_upstream_tool_ids(conn, workflow_db_id, tool_db_id):
    """Finds the database IDs of tools connected to the input of a given tool."""
    tool_xml_id_df = pd.read_sql_query("SELECT tool_id_xml FROM tools WHERE id = ?", conn, params=(tool_db_id,))
    if tool_xml_id_df.empty:
        return []
    tool_xml_id = tool_xml_id_df.iloc[0]['tool_id_xml']
    
    upstream_xml_ids_df = pd.read_sql_query(
        "SELECT origin_tool_id_xml FROM connections WHERE workflow_id = ? AND destination_tool_id_xml = ?",
        conn,
        params=(workflow_db_id, tool_xml_id)
    )
    if upstream_xml_ids_df.empty:
        return []
    
    upstream_xml_ids = upstream_xml_ids_df['origin_tool_id_xml'].tolist()
    if not upstream_xml_ids:
        return []

    placeholders = ','.join('?' for _ in upstream_xml_ids)
    query = f"SELECT id FROM tools WHERE workflow_id = ? AND tool_id_xml IN ({placeholders})"
    
    params = [workflow_db_id]
    params.extend(upstream_xml_ids)
    
    upstream_db_ids_df = pd.read_sql_query(query, conn, params=tuple(params))
    
    return upstream_db_ids_df['id'].tolist()

def find_upstream_fields_for_tool(conn, workflow_db_id, start_tool_db_id):
    """Iteratively looks upstream from a tool to find the first ancestor(s) with a field schema."""
    queue = deque([start_tool_db_id])
    visited = set()
    
    while queue:
        current_tool_id = queue.popleft()
        if current_tool_id in visited:
            continue
        visited.add(current_tool_id)
        
        fields_df = pd.read_sql_query("SELECT field_name FROM tool_fields WHERE tool_id = ? ORDER BY field_name", conn, params=(current_tool_id,))
        if not fields_df.empty:
            return fields_df

        upstream_ids = get_upstream_tool_ids(conn, workflow_db_id, current_tool_id)
        for up_id in upstream_ids:
            if up_id not in visited:
                queue.append(up_id)
                
    return pd.DataFrame()


def get_path_aliases(workspace_name):
    """Returns the workspace's drive mappings as (prefix, target) tuples, longest prefix first."""
    conn = create_connection()
    if conn is None: return ()
    try:
        rows = conn.execute("""
            SELECT pa.prefix, pa.target FROM path_aliases pa
            JOIN workspaces ws ON pa.workspace_id = ws.id
            WHERE ws.name = ? ORDER BY LENGTH(pa.prefix) DESC
        """, (workspace_name,)).fetchall()
        return tuple((prefix, target) for prefix, target in rows)
    except Error as e:
        print(f"Database error in get_path_aliases: {e}")
        return ()
    finally:
        conn.close()

def save_path_aliases(workspace_name, aliases):
    """Replaces the workspace's drive mappings with the given (prefix, target) pairs."""
    conn = create_connection()
    if conn is None: return False
    try:
        workspace_id = _get_or_create_workspace_id(conn, workspace_name)
        conn.execute("DELETE FROM path_aliases WHERE workspace_id = ?", (workspace_id,))
        conn.executemany(
            "INSERT INTO path_aliases (workspace_id, prefix, target) VALUES (?, ?, ?)",
            [(workspace_id, prefix.strip(), target.strip()) for prefix, target in aliases if prefix and target]
        )
        conn.commit()
        return True
    except Error as e:
        print(f"Database error in save_path_aliases: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def _upsert_workflow(cursor, workspace_id, workflow_name, server_workflow_id):
    """
    Finds or creates the workflow row and returns (workflow_db_id, stored_name).
    Server workflows are matched by their server ID, so two different server workflows that share a
    file name (e.g. 'Workflow.yxmd') are kept apart; the second gets its ID appended to its name.
    Local uploads have no ID and are matched by file name, as before.
    """
    workflow_db_id = None
    if server_workflow_id:
        row = cursor.execute("SELECT id FROM workflows WHERE workspace_id = ? AND server_workflow_id = ?",
                             (workspace_id, server_workflow_id)).fetchone()
        workflow_db_id = row[0] if row else None
    same_name = cursor.execute("SELECT id, server_workflow_id FROM workflows WHERE workspace_id = ? AND workflow_name = ?",
                               (workspace_id, workflow_name)).fetchone()
    # A same-named row with no server ID is this workflow parsed before IDs were tracked (or a local upload)
    if workflow_db_id is None and same_name and (not server_workflow_id or same_name[1] in (None, server_workflow_id)):
        workflow_db_id = same_name[0]
    stored_name = workflow_name
    if same_name and same_name[0] != workflow_db_id:
        stored_name = f"{workflow_name} [{server_workflow_id}]"

    if workflow_db_id is None:
        cursor.execute(
            "INSERT INTO workflows (workspace_id, workflow_name, last_parsed_at, server_workflow_id) VALUES (?, ?, datetime('now'), ?)",
            (workspace_id, stored_name, server_workflow_id)
        )
        return cursor.lastrowid, stored_name
    cursor.execute(
        "UPDATE workflows SET workflow_name = ?, last_parsed_at = datetime('now'), server_workflow_id = COALESCE(?, server_workflow_id) WHERE id = ?",
        (stored_name, server_workflow_id, workflow_db_id)
    )
    return workflow_db_id, stored_name

def log_workflow_details(workspace_name, workflow_name, tools_list, connections_list, workflow_meta=None, server_workflow_id=None):
    """Saves a parsed workflow, replacing any earlier parse of it. Returns the name it was stored under, or None on error."""
    conn = create_connection()
    if conn is None: return None
    try:
        cursor = conn.cursor()
        workspace_id = _get_or_create_workspace_id(conn, workspace_name)
        workflow_db_id, stored_name = _upsert_workflow(cursor, workspace_id, workflow_name, server_workflow_id)
        # Only overwrite metadata we were given, so a local re-upload keeps the server owner, etc.
        meta = {k: v for k, v in (workflow_meta or {}).items() if k in WORKFLOW_META_COLUMNS and v is not None}
        if meta:
            assignments = ', '.join(f"{column} = ?" for column in meta)
            cursor.execute(f"UPDATE workflows SET {assignments} WHERE id = ?", (*meta.values(), workflow_db_id))
        existing_tools_cursor = cursor.execute("SELECT id FROM tools WHERE workflow_id = ?", (workflow_db_id,))
        existing_tool_ids = [row[0] for row in existing_tools_cursor.fetchall()]
        if existing_tool_ids:
            cursor.execute(f"DELETE FROM tool_fields WHERE tool_id IN ({','.join('?' for _ in existing_tool_ids)})", existing_tool_ids)
            cursor.execute("DELETE FROM connections WHERE workflow_id = ?", (workflow_db_id,))
            cursor.execute("DELETE FROM tools WHERE workflow_id = ?", (workflow_db_id,))
        for tool in tools_list:
            cursor.execute(
                "INSERT INTO tools (workflow_id, tool_id_xml, plugin, annotation, config_xml, macro) VALUES (?, ?, ?, ?, ?, ?)",
                (workflow_db_id, tool['id'], tool['plugin'], tool['annotation'], tool['config_xml'], tool.get('macro'))
            )
            tool_db_id = cursor.lastrowid
            if tool.get('output_fields'):
                for field in tool['output_fields']:
                    cursor.execute(
                        "INSERT INTO tool_fields (tool_id, field_name, field_type, field_size, field_source, field_description) VALUES (?, ?, ?, ?, ?, ?)",
                        (tool_db_id, field['name'], field['type'], field['size'], field['source'], field['description'])
                    )
        for conn_data in connections_list:
            cursor.execute(
                "INSERT INTO connections (workflow_id, origin_tool_id_xml, destination_tool_id_xml, origin_anchor, destination_anchor) VALUES (?, ?, ?, ?, ?)",
                (workflow_db_id, conn_data['origin_id'], conn_data['destination_id'], conn_data.get('origin_anchor'), conn_data.get('destination_anchor'))
            )
        conn.commit()
        return stored_name
    except Error as e:
        print(f"Database error in log_workflow_details: {e}")
        conn.rollback()
        return None
    finally:
        if conn: conn.close()

def get_workflows_in_workspace(workspace_name):
    """Retrieves all workflows for a given workspace."""
    conn = create_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        query = """
            SELECT w.id, w.workflow_name, w.last_parsed_at
            FROM workflows w
            JOIN workspaces ws ON w.workspace_id = ws.id
            WHERE ws.name = ?
            ORDER BY w.workflow_name;
        """
        df = pd.read_sql_query(query, conn, params=(workspace_name,))
        return df
    except Error as e:
        print(f"Database error in get_workflows_in_workspace: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def delete_workflow(workflow_id):
    """Deletes a workflow and all its associated tools, fields, and connections."""
    conn = create_connection()
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM tools WHERE workflow_id = ?", (workflow_id,))
        tool_ids = [row[0] for row in cursor.fetchall()]
        
        if tool_ids:
            placeholders = ','.join('?' for _ in tool_ids)
            cursor.execute(f"DELETE FROM tool_fields WHERE tool_id IN ({placeholders})", tool_ids)
        
        cursor.execute("DELETE FROM connections WHERE workflow_id = ?", (workflow_id,))
        
        cursor.execute("DELETE FROM tools WHERE workflow_id = ?", (workflow_id,))
        
        cursor.execute("DELETE FROM workflows WHERE id = ?", (workflow_id,))
        
        conn.commit()
        return True
    except Error as e:
        print(f"Database error in delete_workflow: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


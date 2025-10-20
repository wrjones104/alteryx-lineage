import sqlite3
from sqlite3 import Error
import os
import streamlit as st
import json
import pandas as pd
from collections import deque

DB_FILE = os.path.join("data", "lineage.db")
CONNECTIONS_FILE = os.path.join("data", "connections.json")

def load_connections():
    """Loads saved server connection details from a JSON file."""
    if not os.path.exists(CONNECTIONS_FILE):
        return {}
    try:
        with open(CONNECTIONS_FILE, 'r') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError):
        return {}

def save_connection(conn_name, url, client_id, client_secret):
    """Saves a new or updated server connection to the JSON file."""
    connections = load_connections()
    connections[conn_name] = {
        'url': url,
        'client_id': client_id,
        'client_secret': client_secret
    }
    try:
        os.makedirs(os.path.dirname(CONNECTIONS_FILE), exist_ok=True)
        with open(CONNECTIONS_FILE, 'w') as f:
            json.dump(connections, f, indent=4)
        return True
    except IOError:
        return False

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


def log_workflow_details(workspace_name, workflow_name, tools_list, connections_list):
    conn = create_connection()
    if conn is None: return
    try:
        cursor = conn.cursor()
        workspace_id = _get_or_create_workspace_id(conn, workspace_name)
        cursor.execute(
            "INSERT OR IGNORE INTO workflows (workspace_id, workflow_name, last_parsed_at) VALUES (?, ?, datetime('now'))",
            (workspace_id, workflow_name)
        )
        cursor.execute(
            "UPDATE workflows SET last_parsed_at = datetime('now') WHERE workspace_id = ? AND workflow_name = ?",
            (workspace_id, workflow_name)
        )
        workflow_db_id = cursor.execute("SELECT id FROM workflows WHERE workspace_id = ? AND workflow_name = ?", (workspace_id, workflow_name)).fetchone()[0]
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
                "INSERT INTO connections (workflow_id, origin_tool_id_xml, destination_tool_id_xml) VALUES (?, ?, ?)",
                (workflow_db_id, conn_data['origin_id'], conn_data['destination_id'])
            )
        conn.commit()
    except Error as e:
        print(f"Database error in log_workflow_details: {e}")
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


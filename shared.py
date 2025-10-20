import streamlit as st
import database_manager as db

def on_workspace_select():
    """Callback to handle workspace selection changes."""
    selected = st.session_state.get('workspace_selector')
    st.session_state.workspace = selected if selected else None
    # These are necessary to ensure data is reloaded when the workspace changes.
    st.cache_data.clear()
    st.cache_resource.clear()

def create_sidebar():
    """Creates the shared sidebar for all pages."""
    st.sidebar.title("Workspace")
    workspace_list = db.get_all_workspaces()
    options = [""] + workspace_list

    try:
        current_index = options.index(st.session_state.get('workspace', ''))
    except ValueError:
        current_index = 0

    st.sidebar.selectbox(
        "Select a workspace",
        options=options,
        key='workspace_selector',
        index=current_index,
        on_change=on_workspace_select,
        help="Select a workspace to analyze, or create a new one below."
    )

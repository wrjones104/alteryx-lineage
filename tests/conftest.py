import os
import sys
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(PROJECT_ROOT, 'tests', 'fixtures')
sys.path.insert(0, PROJECT_ROOT)

import streamlit as st
import database_manager as db
from parser import parse_workflow


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    """A fresh database with the three fixture workflows loaded into workspace 'Finance'."""
    monkeypatch.setattr(db, 'DB_FILE', str(tmp_path / 'lineage.db'))
    st.cache_data.clear()
    db.create_tables()
    for name in ('Extract_GL.yxmd', 'Report_GL.yxmd', 'Daily_Feed.yxmd'):
        tools, connections, meta = parse_workflow(os.path.join(FIXTURES, name))
        db.log_workflow_details('Finance', name, tools, connections, {**meta, 'source': 'Local Upload'})
    yield 'Finance'
    st.cache_data.clear()

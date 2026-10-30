import streamlit as st
import pandas as pd
from datetime import date
import database_manager as db
import io_catalog
import shared  # Import the shared components

# --- Page Config and DB Setup ---
st.set_page_config(layout="wide", page_title="I/O Catalog")
db.create_tables()

# --- Session State Initialization ---
if 'workspace' not in st.session_state:
    st.session_state.workspace = None

# --- Sidebar ---
shared.create_sidebar()

# --- Main Page Content ---
st.header("📒 I/O Catalog")

if not st.session_state.workspace:
    st.warning("⬅️ Please select a workspace in the sidebar to begin.")
    st.stop()

workspace = st.session_state.workspace
st.write(f"Master list of every input and output for workspace: **{workspace}**")

# --- Drive Mappings ---
aliases = db.get_path_aliases(workspace)
with st.expander(f"🗺️ Drive mappings ({len(aliases)})"):
    st.caption(
        "Map drive letters to their UNC paths so `S:\\Finance\\gl.csv` and `\\\\fileserver\\finance\\gl.csv` "
        "are treated as the same file. Mappings apply to this catalog and to cross-workflow lineage."
    )
    # A form stops every cell edit from rerunning the page (which collapses this expander)
    with st.form("alias_form", border=False):
        edited_aliases = st.data_editor(
            pd.DataFrame(aliases, columns=['Drive or Prefix', 'UNC Path']),
            num_rows="dynamic", key="alias_editor", use_container_width=True,
            column_config={
                "Drive or Prefix": st.column_config.TextColumn(help="e.g. S: or S:\\Finance"),
                "UNC Path": st.column_config.TextColumn(help="e.g. \\\\fileserver\\finance"),
            },
        )
        save_aliases = st.form_submit_button("Save Drive Mappings")
    if save_aliases:
        pairs = [(row['Drive or Prefix'], row['UNC Path']) for _, row in edited_aliases.iterrows()
                 if isinstance(row['Drive or Prefix'], str) and isinstance(row['UNC Path'], str)]
        if db.save_path_aliases(workspace, pairs):
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Could not save drive mappings.")

# --- Build Catalog ---
catalog = io_catalog.build_io_catalog(workspace, aliases)
if not catalog:
    st.info("No workflows have been processed in this workspace yet. Add data on the 'Home' page.")
    st.stop()

detail, stores = catalog['I/O Detail'], catalog['Data Stores']
workflows, review = catalog['Workflows'], catalog['Needs Review']

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Workflows", len(workflows))
m2.metric("Inputs / Outputs", f"{(detail['Direction'] == 'Input').sum() if not detail.empty else 0} / "
                              f"{(detail['Direction'] == 'Output').sum() if not detail.empty else 0}")
m3.metric("Unique Data Stores", len(stores))
m4.metric("Cross-Workflow Links", int((stores['Role'] == io_catalog.ROLE_INTERMEDIATE).sum()) if not stores.empty else 0,
          help="Data stores written by one workflow and read by another")
m5.metric("Needs Review", len(review), help="Tools whose inputs/outputs could not be fully determined")

# --- Downloads ---
file_stem = f"io_catalog_{workspace}_{date.today():%Y%m%d}".replace(' ', '_')
d1, d2, _ = st.columns([1.2, 1.2, 4])
with d1:
    st.download_button("⬇️ Download Excel (all sheets)", data=io_catalog.to_excel_bytes(catalog),
                       file_name=f"{file_stem}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
with d2:
    st.download_button("⬇️ Download I/O Detail (CSV)", data=detail.to_csv(index=False).encode('utf-8-sig'),
                       file_name=f"{file_stem}.csv", mime="text/csv")

st.markdown("---")

# --- Views ---
tab_detail, tab_stores, tab_workflows, tab_review = st.tabs(
    ["I/O Detail", "Data Stores", "Workflows", f"Needs Review ({len(review)})"])

with tab_detail:
    if detail.empty:
        st.info("No inputs or outputs were found in this workspace.")
    else:
        f1, f2, f3 = st.columns([1, 1, 2])
        directions = f1.multiselect("Direction", ["Input", "Output"], default=["Input", "Output"])
        store_types = f2.multiselect("Data Store Type", sorted(detail['Data Store Type'].unique()))
        search = f3.text_input("Search (workflow, path, field...)", placeholder="Filter rows...")
        view = detail[detail['Direction'].isin(directions)]
        if store_types:
            view = view[view['Data Store Type'].isin(store_types)]
        if search:
            view = view[view.apply(lambda r: r.astype(str).str.contains(search, case=False, regex=False).any(), axis=1)]
        st.caption(f"Showing {len(view)} of {len(detail)} rows. Downloads always include every row.")
        st.dataframe(view, use_container_width=True, hide_index=True)

with tab_stores:
    st.caption(
        "Each file or table, who writes it and who reads it. **External Source**: read but not written by any "
        "workflow in this workspace. **Intermediate**: written by one workflow and read by another. "
        "**Final Output**: written but not read by any workflow in this workspace."
    )
    st.dataframe(stores, use_container_width=True, hide_index=True)

with tab_workflows:
    st.dataframe(workflows, use_container_width=True, hide_index=True)

with tab_review:
    st.caption(
        "Tools that may read or write data that could not be fully catalogued: dynamic or relative paths, "
        "Python/R/Run Command tools, custom macros, and file paths found in other tools' settings. "
        "To catalog a Python or R tool, add a lineage block to its annotation:"
    )
    st.code("--- lineage ---\ninputs:\n  - type: file\n    path: \\\\server\\share\\input.csv\n"
            "outputs:\n  - type: file\n    path: \\\\server\\share\\output.csv\n---", language="yaml")
    st.dataframe(review, use_container_width=True, hide_index=True)

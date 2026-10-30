# Alteryx Lineage & Impact Analysis Tool 🔗

A web application built with Python and Streamlit to parse Alteryx workflows, store their metadata, and provide powerful lineage analysis. This tool helps data teams understand dependencies between workflows, perform impact analysis for changes, and trace individual fields from source to destination.

---

## ✨ Features

* **Workspace Management:** Create separate, isolated workspaces to manage workflows by team (e.g., Marketing, Finance).
* **Multi-Source Ingestion:**
    * **Local Upload:** Process local `.yxmd` files via a drag-and-drop interface.
    * **Alteryx Server Connection:** Connect directly to an Alteryx Server (V3 API) to fetch, download, and process workflows.
    * **Connection Manager:** Save and load server connection details so you don't have to enter them every time.
* **Deep Parsing:**
    * Extracts every tool, its configuration, and all connections from a workflow.
    * Identifies and catalogs every field on the output of every tool.
    * Handles complex "black box" tools (like Python scripts) via manual YAML annotations.
    * Understands custom macros (like the "Input Data Selector").
* **I/O Catalog (📒):** A master list of every input and output in the workspace, built for data catalog work.
    * One row per Input/Output tool with workflow, tool, path (split into share, folder, file name, format), sheet/table, write mode and field list.
    * Links each file to the workflows that **produce** and **consume** it, so workflow-to-workflow dependencies show up directly. Each file is labelled *External Source*, *Intermediate* or *Final Output*.
    * **Drive mappings** treat `S:\...` and `\\fileserver\share\...` as the same file. Excel sheet names, path case and wildcard inputs (`gl_*.csv`) are matched too, here and in the Impact Analysis and Field Lineage pages.
    * A workflow that reads back a file it writes itself (for example a `.yxdb` history snapshot) is not counted as a cross-workflow link; its Data Stores row is noted "Read back by the same workflow".
    * A **Needs Review** list flags what the parser can't see for itself: dynamic or relative paths, Python/R/Run Command tools, custom macros, and file paths found in other tools' settings.
    * Download as a multi-sheet Excel workbook (I/O Detail, Data Stores, Workflows, Needs Review) or as CSV.
* **Impact Analysis Report (💥):**
    * View "blast radius" by **Data Source** to see which files/tables are the most critical dependencies.
    * Toggle to view by **Workflow** to see which workflows produce the most used outputs.
* **Field Lineage Explorer (🗺️):**
    * **Upstream Tracing:** Select any field from any tool and trace it backward to its origin, correctly tracking renames through `Select`, `Join`, and `Formula` tools. When a field comes from more than one place (a Union, or a Filter's True and False sides joined back together), each route is numbered in a **Branch** column.
    * **Downstream Tracing:** Select a field and find all the final output files it is a part of, tracking renames along the way.
* **Debugging & Validation:**
    * **Raw I/O Log:** An unfiltered view of every input and output parsed from all workflows.
    * **DB Inspector:** A simple UI to directly inspect the contents of the underlying SQLite database tables.

---

## 🛠️ Tech Stack & Requirements

* **Language:** Python 3.10–3.12 (the pinned packages in `requirements.txt` do not install on 3.14)
* **Framework:** Streamlit
* **Database:** SQLite
* **Core Libraries:**
    * `pandas`
    * `lxml` (for robust XML parsing)
    * `PyYAML` (for parsing manual annotations)
    * `requests` (for Alteryx Server API communication)
    * `openpyxl` (for the Excel export)

---

## 🚀 Setup & Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd alteryx_lineage_tool
    ```

2.  **Create and activate a Python virtual environment:**
    ```bash
    # Create the environment
    python -m venv venv

    # Activate on Windows
    .\venv\Scripts\activate

    # Activate on macOS/Linux
    source venv/bin/activate
    ```

3.  **Install the required packages:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the application:**
    ```bash
    streamlit run Home.py
    ```
    Your web browser should automatically open the application.

### Server connections and security

* **Certificates:** Alteryx Server certificates are checked against your computer's certificate store, so a server signed by an internal company CA works without extra setup. To trust a specific CA file instead, add `REQUESTS_CA_BUNDLE=C:\path\to\ca.pem` to a `.env` file. Only as a last resort (for example, an expired internal certificate), `ALTERYX_SSL_VERIFY=false` turns checking off.
* **Saved secrets:** "Save Current Connection" keeps the client secret in Windows Credential Manager (or the macOS Keychain), not in `data/connections.json`. Secrets saved in plain text by older versions are moved there automatically the next time the app loads them.

---

## 📖 Usage Guide

1.  **Select a Workspace:** Upon launching, use the sidebar to either create a new workspace or select an existing one.
2.  **Add Workflows:**
    * **From Server:** Expand the "Connect to Server" section, enter your credentials (or load a saved connection), and click "Connect". A table of accessible workflows will appear in the main window. Use the checkboxes to select workflows and then click "Download & Process Selected".
    * **From Local:** Use the "Upload Local Workflows" uploader to select one or more `.yxmd` or `.yxwz` files from your computer. They will be processed automatically.
    * Server workflows are tracked by their server ID, so two server workflows with the same file name stay separate (the second gets its ID added to its name). A server download never replaces a local upload of the same name; it is saved under its ID instead. Local uploads are matched by file name, so uploading a file with the same name replaces the earlier one.
    * **Update from Server:** Once connected, the **Check Workspace for Server Updates** table compares each server workflow in the workspace with the version now published on the server. Workflows marked *Update available* are preselected; click **Download Updates** to re-download and re-parse them in place. **Re-check Server** picks up versions published since you connected. *Version unknown* means the workflow was downloaded before versions were recorded, or was replaced by a local upload; re-download it once to record its version. *Not found on server* means it was deleted or isn't shared with the connected account. Local uploads aren't linked to a server workflow, so they aren't checked.
    * Re-process workflows that were loaded before this version. Join/Union field tracing needs their connection details, and field lists after a Join or Filter need the per-output schemas.
3.  **Analyze:**
    * Use the pages in the sidebar to switch between the **Impact Analysis**, **Field Lineage Explorer**, **I/O Catalog**, and other views to explore your workflow data.
4.  **Catalog:** On the **I/O Catalog** page, add your drive mappings first (for example `S:` → `\\fileserver\finance`). Then download the Excel workbook. Use the **Needs Review** sheet to see which tools need a manual check or a lineage annotation.

### Annotating tools the parser can't read

Python, R and other "black box" tools can declare their inputs and outputs in the tool annotation:

```yaml
--- lineage ---
inputs:
  - type: file
    path: \\fileserver\finance\input.csv
outputs:
  - type: file
    path: \\fileserver\finance\output.csv
---
```

---

## 🧪 Tests

```bash
pip install pytest
python -m pytest tests
```

The tests use the sample workflows in `tests/fixtures` and a temporary database.

---

## 🔮 Future Enhancements

* **SSO Integration:** Integrate with an SSO provider like Okta for user authentication and automatic workspace assignment.
* **Visual Lineage:** Enhance the field tracer to output a visual graph (e.g., using Graphviz) of the lineage path instead of a table.
* **Advanced Parsers:** Add more specific parsing logic for complex tools (e.g., handling `Left_` and `Right_` prefixes from Join tools in downstream traces).
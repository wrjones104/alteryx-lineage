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
* **Impact Analysis Report (💥):**
    * View "blast radius" by **Data Source** to see which files/tables are the most critical dependencies.
    * Toggle to view by **Workflow** to see which workflows produce the most used outputs.
* **Field Lineage Explorer (🗺️):**
    * **Upstream Tracing:** Select any field from any tool and trace it backward to its origin, correctly tracking renames through `Select`, `Join`, and `Formula` tools.
    * **Downstream Tracing:** Select a field and find all the final output files it is a part of, tracking renames along the way.
* **Debugging & Validation:**
    * **Raw I/O Log:** An unfiltered view of every input and output parsed from all workflows.
    * **DB Inspector:** A simple UI to directly inspect the contents of the underlying SQLite database tables.

---

## 🛠️ Tech Stack & Requirements

* **Language:** Python 3.10+
* **Framework:** Streamlit
* **Database:** SQLite
* **Core Libraries:**
    * `pandas`
    * `lxml` (for robust XML parsing)
    * `PyYAML` (for parsing manual annotations)
    * `requests` (for Alteryx Server API communication)

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

### Internal Certificates (Optional)
If your organization uses self-signed certificates or internal Certificate Authorities (CAs) and you encounter SSL errors when connecting to the Alteryx Server, you can set the `REQUESTS_CA_BUNDLE` environment variable to point to your custom CA bundle file. You can do this by adding it to a `.env` file in the root directory:
```
REQUESTS_CA_BUNDLE=/path/to/your/ca-bundle.crt
```

### Server Credentials Encryption
When connecting to the Alteryx Server, your `client_secret` is saved locally to allow easy reconnection in future sessions. To prevent saving credentials in plaintext, they are encrypted at rest using the `cryptography` library. For this to persist correctly, you should provide an encryption key via the `ENCRYPTION_KEY` environment variable in your `.env` file. You can generate a random secure key by running the following Python command:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
And add it to your `.env` file:
```bash
ENCRYPTION_KEY=your-generated-key
```

---

## 📖 Usage Guide

1.  **Select a Workspace:** Upon launching, use the sidebar to either create a new workspace or select an existing one.
2.  **Add Workflows:**
    * **From Server:** Expand the "Connect to Server" section, enter your credentials (or load a saved connection), and click "Connect". A table of accessible workflows will appear in the main window. Use the checkboxes to select workflows and then click "Download & Process Selected".
    * **From Local:** Use the "Upload Local Workflows" uploader to select one or more `.yxmd` files from your computer. They will be processed automatically.
3.  **Analyze:**
    * Use the tabs at the top of the main window to switch between the **Impact Analysis**, **Field Lineage Explorer**, and other views to explore your workflow data.

---

## 🔮 Future Enhancements

* **SSO Integration:** Integrate with an SSO provider like Okta for user authentication and automatic workspace assignment.
* **Visual Lineage:** Enhance the field tracer to output a visual graph (e.g., using Graphviz) of the lineage path instead of a table.
* **Advanced Parsers:** Add more specific parsing logic for complex tools (e.g., handling `Left_` and `Right_` prefixes from Join tools in downstream traces).
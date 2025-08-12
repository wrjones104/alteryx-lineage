import pandas as pd
from parser import extract_io_tools
from database_manager import create_connection

def get_raw_io_list(workspace_name):
    """Fetches and parses all I/O, managing its own DB connection."""
    conn = create_connection()
    if conn is None: return []
    
    try:
        query = """
            SELECT w.workflow_name, t.tool_id_xml as id, t.plugin, t.macro, t.config_xml, t.annotation
            FROM tools t
            JOIN workflows w ON t.workflow_id = w.id
            JOIN workspaces ws ON w.workspace_id = ws.id
            WHERE ws.name = ?
        """
        tools_df = pd.read_sql_query(query, conn, params=(workspace_name,))
    finally:
        if conn: conn.close()
        
    if tools_df.empty: return []

    io_list = []
    for workflow_name, group in tools_df.groupby('workflow_name'):
        tools_list = group.to_dict('records')
        inputs, outputs = extract_io_tools(tools_list)
        for i in inputs:
            i['workflow_name'] = workflow_name
            i['io_type'] = 'input'
            io_list.append(i)
        for o in outputs:
            o['workflow_name'] = workflow_name
            o['io_type'] = 'output'
            io_list.append(o)
    return io_list

def generate_impact_report(raw_io_list, view_by='Data Source'):
    """Uses a raw I/O list to generate the aggregated impact analysis report."""
    if not raw_io_list: return pd.DataFrame()

    df = pd.DataFrame(raw_io_list)
    df['source_detail'] = df['source_detail'].apply(lambda x: x.lower() if isinstance(x, str) else x)
    
    inputs = df[df['io_type'] == 'input'].rename(columns={'workflow_name': 'Consuming Workflow', 'source_detail': 'Data Source'})
    consumer_agg = inputs.groupby('Data Source').agg(
        NumberOfConsumers=('Consuming Workflow', 'nunique'),
        ConsumingWorkflows=('Consuming Workflow', lambda x: list(x.unique()))
    ).reset_index()

    outputs = df[df['io_type'] == 'output'].rename(columns={'workflow_name': 'Producing Workflow', 'source_detail': 'Data Source'})[['Data Source', 'Producing Workflow']].drop_duplicates()
    all_sources = pd.DataFrame({'Data Source': pd.concat([inputs['Data Source'], outputs['Data Source']]).unique()})

    report_df = pd.merge(all_sources, outputs, on='Data Source', how='left')
    report_df = pd.merge(report_df, consumer_agg, on='Data Source', how='left')
    
    report_df['Producing Workflow'] = report_df['Producing Workflow'].fillna('External Source')
    report_df['NumberOfConsumers'] = report_df['NumberOfConsumers'].fillna(0).astype(int)
    report_df['ConsumingWorkflows'] = report_df['ConsumingWorkflows'].apply(lambda d: d if isinstance(d, list) else [])

    if view_by == 'Workflow':
        workflow_view = report_df[report_df['Producing Workflow'] != 'External Source'].groupby('Producing Workflow').agg(
            UniqueOutputs=('Data Source', 'nunique'),
            TotalDownstreamConsumers=('NumberOfConsumers', 'sum')
        ).reset_index()
        workflow_view = workflow_view.sort_values(by='TotalDownstreamConsumers', ascending=False).reset_index(drop=True)
        return workflow_view.rename(columns={'Producing Workflow': 'Workflow'})
    
    else: # Default to 'Data Source' view
        datasource_view = report_df.sort_values(by=['NumberOfConsumers', 'Data Source'], ascending=[False, True]).reset_index(drop=True)
        return datasource_view[['Data Source', 'Producing Workflow', 'NumberOfConsumers', 'ConsumingWorkflows']]
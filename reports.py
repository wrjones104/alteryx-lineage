import pandas as pd
from parser import extract_io_tools
from database_manager import create_connection

def get_raw_io_list(workspace_name):
    """Fetches and parses all I/O, managing its own DB connection."""
    conn = create_connection()
    if conn is None: return []
    
    try:
        # Pass the tool's database primary key as tool_db_id
        query = """
            SELECT 
                w.id as workflow_db_id, 
                w.workflow_name, 
                t.id as tool_db_id, 
                t.tool_id_xml, 
                t.plugin, 
                t.macro, 
                t.config_xml, 
                t.annotation
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
    tools_list_for_parser = tools_df.to_dict('records')
    inputs, outputs = extract_io_tools(tools_list_for_parser)
    
    # Re-join with tool_id_xml for display purposes
    tool_id_map = tools_df.set_index('tool_db_id')['tool_id_xml'].to_dict()

    for i in inputs:
        i['io_type'] = 'input'
        i['tool_id'] = tool_id_map.get(i['tool_db_id'])
        io_list.append(i)
    for o in outputs:
        o['io_type'] = 'output'
        o['tool_id'] = tool_id_map.get(o['tool_db_id'])
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
        # Helper function to flatten lists of lists and get unique values
        def flatten_and_unique(series_of_lists):
            flat_list = [item for sublist in series_of_lists if isinstance(sublist, list) for item in sublist]
            return list(pd.Series(flat_list).unique())

        workflow_view = report_df[report_df['Producing Workflow'] != 'External Source'].groupby('Producing Workflow').agg(
            UniqueOutputs=('Data Source', 'nunique'),
            TotalDownstreamConsumers=('NumberOfConsumers', 'sum'),
            DownstreamConsumers=('ConsumingWorkflows', flatten_and_unique)
        ).reset_index()
        workflow_view = workflow_view.sort_values(by='TotalDownstreamConsumers', ascending=False).reset_index(drop=True)
        return workflow_view.rename(columns={'Producing Workflow': 'Workflow'})
    
    else: # Default to 'Data Source' view
        datasource_view = report_df.sort_values(by=['NumberOfConsumers', 'Data Source'], ascending=[False, True]).reset_index(drop=True)
        return datasource_view[['Data Source', 'Producing Workflow', 'NumberOfConsumers', 'ConsumingWorkflows']]

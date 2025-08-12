import os
import zipfile
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def get_access_token(base_url, client_id, client_secret):
    """Authenticates with the Alteryx Server API to get a bearer token."""
    token_url = f"{base_url}/webapi/oauth2/token"
    payload = {'grant_type': 'client_credentials', 'client_id': client_id, 'client_secret': client_secret}
    try:
        response = requests.post(token_url, data=payload, verify=False)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        print(f"Error getting access token: {e}")
        return None

def get_workflows(base_url, access_token):
    """Fetches all workflows accessible to the user, including extra metadata."""
    headers = {'Authorization': f'Bearer {access_token}'}
    workflows_url = f"{base_url}/webapi/v3/workflows"
    
    try:
        response = requests.get(workflows_url, headers=headers, verify=False)
        response.raise_for_status()
        workflows_list = response.json()
        if not isinstance(workflows_list, list):
            print(f"API Error: Expected a list of workflows but got a {type(workflows_list)}")
            return []
            
        detailed_workflows = []
        for wf in workflows_list:
            date_str = wf.get('dateCreated', 'N/A')
            friendly_date = date_str.split('T')[0] if 'T' in date_str else date_str
            detailed_workflows.append({
                'id': wf.get('id'),
                'name': wf.get('name'),
                'ownerId': wf.get('ownerId', 'N/A'),
                'dateCreated': friendly_date,
                'publishedVersionNumber': wf.get('publishedVersionNumber', 'N/A')
            })
            
        sorted_workflows = sorted(detailed_workflows, key=lambda w: w['name'].lower())
        return sorted_workflows
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve workflows: {e}")
        return []

def download_and_unpack_workflow(base_url, access_token, workflow_id, download_dir):
    """
    Downloads a packaged workflow (.yxzp), unpacks it, and returns the path to the .yxmd file.
    --- THIS VERSION KEEPS THE FINAL .yxmd FILE FOR DEBUGGING ---
    """
    download_url = f"{base_url}/webapi/v3/workflows/{workflow_id}/package"
    headers = {'Authorization': f'Bearer {access_token}'}
    yxzp_path = os.path.join(download_dir, f"{workflow_id}.yxzp")
    unpacked_path = None
    
    try:
        os.makedirs(download_dir, exist_ok=True)
        
        with requests.get(download_url, headers=headers, stream=True, verify=False) as r:
            r.raise_for_status()
            with open(yxzp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            
        with zipfile.ZipFile(yxzp_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.yxmd'):
                    zip_ref.extract(file_info, download_dir)
                    unpacked_path = os.path.join(download_dir, file_info.filename)
                    break
        
        # --- CHANGE: We no longer delete the unpacked .yxmd file ---
        if yxzp_path and os.path.exists(yxzp_path):
            os.remove(yxzp_path) # Still clean up the temporary zip file

        return unpacked_path

    except Exception as e:
        print(f"Failed to download or unpack workflow {workflow_id}: {e}")
        if os.path.exists(yxzp_path):
            os.remove(yxzp_path)
        return None
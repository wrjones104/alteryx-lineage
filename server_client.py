import os
import zipfile
import requests
from dotenv import load_dotenv

load_dotenv()

def get_verify_option():
    """Returns the custom CA bundle path if set, otherwise True."""
    ca_bundle = os.getenv("REQUESTS_CA_BUNDLE")
    if ca_bundle and os.path.exists(ca_bundle):
        return ca_bundle
    return True

def get_session_key(base_url, client_id, client_secret):
    """Authenticates with the Alteryx Server API to get a session key."""
    token_url = f"{base_url}/webapi/oauth2/token"
    payload = {'grant_type': 'client_credentials', 'client_id': client_id, 'client_secret': client_secret}
    try:
        response = requests.post(token_url, data=payload, verify=get_verify_option())
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.exceptions.RequestException as e:
        print(f"Error getting session key: {e}")
        return None

def get_user_map(base_url):
    """Fetches all users from the server using admin credentials and returns a dict mapping user ID to full name."""
    admin_client_id = os.getenv("ADMIN_CLIENT_ID")
    admin_client_secret = os.getenv("ADMIN_CLIENT_SECRET")
    if not all([admin_client_id, admin_client_secret]):
        print("Admin credentials not found in .env file. Cannot fetch user names.")
        return {}
    
    admin_s_key = get_session_key(base_url, admin_client_id, admin_client_secret)
    if not admin_s_key:
        print("Failed to get admin session key.")
        return {}

    users_url = f"{base_url}/webapi/v3/users"
    headers = {'Authorization': f'Bearer {admin_s_key}'}
    try:
        response = requests.get(users_url, headers=headers, verify=get_verify_option())
        response.raise_for_status()
        users_list = response.json()
        user_map = {user['id']: f"{user.get('firstName', '')} {user.get('lastName', '')}".strip() for user in users_list}
        return user_map
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve users: {e}")
        return {}


def get_workflows(base_url, session_key):
    """Fetches all workflows accessible to the user, including extra metadata."""
    user_map = get_user_map(base_url)
    headers = {'Authorization': f'Bearer {session_key}'}
    workflows_url = f"{base_url}/webapi/v3/workflows"
    
    try:
        response = requests.get(workflows_url, headers=headers, verify=get_verify_option())
        response.raise_for_status()
        workflows_list = response.json()
        if not isinstance(workflows_list, list):
            print(f"API Error: Expected a list of workflows but got a {type(workflows_list)}")
            return []
            
        detailed_workflows = []
        for wf in workflows_list:
            date_str = wf.get('dateCreated', 'N/A')
            friendly_date = date_str.split('T')[0] if 'T' in date_str else date_str
            owner_id = wf.get('ownerId', 'N/A')
            detailed_workflows.append({
                'id': wf.get('id'),
                'name': wf.get('name'),
                'ownerName': user_map.get(owner_id, owner_id), # Use name from map, fallback to ID
                'dateCreated': friendly_date,
                'publishedVersionNumber': wf.get('publishedVersionNumber', 'N/A')
            })
            
        sorted_workflows = sorted(detailed_workflows, key=lambda w: w['name'].lower())
        return sorted_workflows
        
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve workflows: {e}")
        return []

def download_and_unpack_workflow(base_url, session_key, workflow_id, download_dir):
    """
    Downloads a packaged workflow (.yxzp), unpacks it, and returns the path to the .yxmd file.
    """
    download_url = f"{base_url}/webapi/v3/workflows/{workflow_id}/package"
    headers = {'Authorization': f'Bearer {session_key}'}
    yxzp_path = os.path.join(download_dir, f"{workflow_id}.yxzp")
    unpacked_path = None
    
    try:
        os.makedirs(download_dir, exist_ok=True)
        
        with requests.get(download_url, headers=headers, stream=True, verify=get_verify_option()) as r:
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
        
        if yxzp_path and os.path.exists(yxzp_path):
            os.remove(yxzp_path)

        return unpacked_path

    except Exception as e:
        print(f"Failed to download or unpack workflow {workflow_id}: {e}")
        if os.path.exists(yxzp_path):
            os.remove(yxzp_path)
        return None


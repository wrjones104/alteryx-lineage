import os
import re
import shutil
import tempfile
import zipfile
import requests
from dotenv import load_dotenv

load_dotenv()

try:
    # Check certificates against the Windows/macOS certificate store, so a server signed by an
    # internal company CA works without exporting a CA bundle.
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

# To trust a specific CA file, set REQUESTS_CA_BUNDLE in .env. ALTERYX_SSL_VERIFY=false turns
# checking off entirely; use it only as a last resort (e.g. an expired internal certificate).
VERIFY_SSL = os.getenv("ALTERYX_SSL_VERIFY", "true").strip().lower() not in ("false", "0", "no")
if not VERIFY_SSL:
    print("WARNING: ALTERYX_SSL_VERIFY is off; Alteryx Server certificates will not be checked.")
# (connect, read) seconds, so a stalled server can't hang the page
REQUEST_TIMEOUT = (10, 120)

def get_session_key(base_url, client_id, client_secret):
    """Authenticates with the Alteryx Server API to get a session key."""
    token_url = f"{base_url.rstrip('/')}/webapi/oauth2/token"
    payload = {'grant_type': 'client_credentials', 'client_id': client_id, 'client_secret': client_secret}
    try:
        response = requests.post(token_url, data=payload, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
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

    users_url = f"{base_url.rstrip('/')}/webapi/v3/users"
    headers = {'Authorization': f'Bearer {admin_s_key}'}
    try:
        response = requests.get(users_url, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        users_list = response.json()
        user_map = {user['id']: f"{user.get('firstName', '')} {user.get('lastName', '')}".strip() for user in users_list}
        return user_map
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve users: {e}")
        return {}


def get_workflows(base_url, session_key):
    """
    Fetches all workflows accessible to the user, including extra metadata. Returns None if the list
    couldn't be fetched (e.g. an expired session), so callers can tell that from an account with no workflows.
    """
    user_map = get_user_map(base_url)
    headers = {'Authorization': f'Bearer {session_key}'}
    workflows_url = f"{base_url.rstrip('/')}/webapi/v3/workflows"

    try:
        response = requests.get(workflows_url, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        workflows_list = response.json()
        if not isinstance(workflows_list, list):
            print(f"API Error: Expected a list of workflows but got a {type(workflows_list)}")
            return None
            
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
        return None

def download_and_unpack_workflow(base_url, session_key, workflow_id, download_dir):
    """
    Downloads a packaged workflow (.yxzp) and unpacks its main workflow into a new folder of its own
    inside download_dir. Returns the path to the .yxmd/.yxwz file; the caller deletes its folder
    (os.path.dirname of the path) when done. Returns None on failure.
    """
    download_url = f"{base_url.rstrip('/')}/webapi/v3/workflows/{workflow_id}/package"
    headers = {'Authorization': f'Bearer {session_key}'}
    os.makedirs(download_dir, exist_ok=True)
    # A folder per download, so two sessions downloading 'Workflow.yxmd' at once don't collide
    safe_id = re.sub(r'[^\w.-]', '_', str(workflow_id))
    work_dir = tempfile.mkdtemp(prefix=f"{safe_id}_", dir=download_dir)
    yxzp_path = os.path.join(work_dir, "package.yxzp")

    try:
        with requests.get(download_url, headers=headers, stream=True, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            with open(yxzp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        unpacked_path = None
        with zipfile.ZipFile(yxzp_path, 'r') as zip_ref:
            candidates = [f for f in zip_ref.infolist()
                          if not f.is_dir() and f.filename.lower().endswith(('.yxmd', '.yxwz'))]
            # Packages can bundle dependent workflows in subfolders; the main one is at the top level.
            candidates.sort(key=lambda f: f.filename.replace('\\', '/').count('/'))
            for candidate in candidates:
                # Only the file name is used, so an entry like '../x.yxmd' or 'C:\\x.yxmd' can't point outside work_dir
                name = os.path.basename(candidate.filename.replace('\\', '/'))
                if name in ('', '.', '..'): continue
                unpacked_path = os.path.join(work_dir, name)
                with zip_ref.open(candidate) as src, open(unpacked_path, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                break
        # Removed after the archive is closed (Windows can't delete an open file)
        os.remove(yxzp_path)
        if unpacked_path is None:
            shutil.rmtree(work_dir, ignore_errors=True)
        return unpacked_path

    except Exception as e:
        print(f"Failed to download or unpack workflow {workflow_id}: {e}")
        shutil.rmtree(work_dir, ignore_errors=True)
        return None


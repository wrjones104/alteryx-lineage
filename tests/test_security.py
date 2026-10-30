import importlib
import json
import os
import keyring
import pytest
from keyring.backend import KeyringBackend
from keyring.errors import NoKeyringError
import database_manager as db
from parser import parse_workflow


class MemoryKeyring(KeyringBackend):
    priority = 1

    def __init__(self):
        super().__init__()
        self.store = {}

    def get_password(self, service, username):
        return self.store.get((service, username))

    def set_password(self, service, username, password):
        self.store[(service, username)] = password

    def delete_password(self, service, username):
        self.store.pop((service, username), None)


@pytest.fixture
def memory_keyring(tmp_path, monkeypatch):
    monkeypatch.setattr(db, 'CONNECTIONS_FILE', str(tmp_path / 'connections.json'))
    backend = MemoryKeyring()
    previous = keyring.get_keyring()
    keyring.set_keyring(backend)
    yield backend
    keyring.set_keyring(previous)


def test_secrets_are_not_written_to_the_json_file(memory_keyring):
    assert db.save_connection('Prod', 'https://alteryx', 'client', 's3cret') == (True, True)
    assert 's3cret' not in open(db.CONNECTIONS_FILE).read()
    assert db.load_connections() == {'Prod': {'url': 'https://alteryx', 'client_id': 'client', 'client_secret': 's3cret'}}


def test_plaintext_secrets_from_older_versions_are_migrated(memory_keyring):
    with open(db.CONNECTIONS_FILE, 'w') as f:
        json.dump({'Old': {'url': 'https://alteryx', 'client_id': 'client', 'client_secret': 'legacy'}}, f)
    assert db.load_connections()['Old']['client_secret'] == 'legacy'
    assert 'client_secret' not in json.load(open(db.CONNECTIONS_FILE))['Old']
    assert memory_keyring.store[(db.KEYRING_SERVICE, 'Old')] == 'legacy'


def test_no_credential_store_saves_connection_without_secret(memory_keyring, monkeypatch):
    def fail(*args):
        raise NoKeyringError('no backend')
    monkeypatch.setattr(keyring, 'set_password', fail)
    monkeypatch.setattr(keyring, 'get_password', fail)
    assert db.save_connection('Prod', 'https://alteryx', 'client', 's3cret') == (True, False)
    assert 's3cret' not in open(db.CONNECTIONS_FILE).read()
    assert db.load_connections()['Prod']['client_secret'] == ''


def test_ssl_verification_is_on_by_default(monkeypatch):
    import server_client
    monkeypatch.delenv('ALTERYX_SSL_VERIFY', raising=False)
    assert importlib.reload(server_client).VERIFY_SSL is True
    monkeypatch.setenv('ALTERYX_SSL_VERIFY', 'false')
    assert importlib.reload(server_client).VERIFY_SSL is False


def test_external_entities_are_not_resolved(tmp_path):
    secret = tmp_path / 'secret.txt'
    secret.write_text('TOP SECRET')
    workflow = tmp_path / 'evil.yxmd'
    workflow.write_text(f"""<?xml version="1.0"?>
<!DOCTYPE AlteryxDocument [<!ENTITY leak SYSTEM "{secret.as_uri()}">]>
<AlteryxDocument yxmdVer="2023.1"><Nodes><Node ToolID="1">
  <GuiSettings Plugin="AlteryxBasePluginsGui.DbFileInput.DbFileInput" />
  <Properties><Configuration><File>&leak;</File></Configuration></Properties>
</Node></Nodes><Connections /></AlteryxDocument>""")
    tools, _, _ = parse_workflow(str(workflow))
    assert 'TOP SECRET' not in tools[0]['config_xml']


def test_failed_secret_save_does_not_keep_an_older_secret(memory_keyring, monkeypatch):
    db.save_connection('Prod', 'https://old', 'client', 'old-secret')
    def fail(*args):
        raise RuntimeError('credential store locked')
    monkeypatch.setattr(keyring, 'set_password', fail)
    assert db.save_connection('Prod', 'https://new', 'client2', 'new-secret') == (True, False)
    assert db.load_connections()['Prod'] == {'url': 'https://new', 'client_id': 'client2', 'client_secret': ''}


def test_package_entries_cannot_point_outside_the_download_folder(tmp_path, monkeypatch):
    import io
    import zipfile
    import server_client
    victim = tmp_path / 'victim.yxmd'
    victim.write_text('original')
    package = io.BytesIO()
    with zipfile.ZipFile(package, 'w') as zf:
        zf.writestr('../victim.yxmd', '<AlteryxDocument />')

    class FakeResponse:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def raise_for_status(self): pass
        def iter_content(self, chunk_size): yield package.getvalue()

    monkeypatch.setattr(server_client.requests, 'get', lambda *args, **kwargs: FakeResponse())
    download_dir = tmp_path / 'downloads'
    path = server_client.download_and_unpack_workflow('https://alteryx/', 'key', 'abc', str(download_dir))
    assert os.path.dirname(os.path.dirname(path)) == str(download_dir)
    assert os.path.basename(path) == 'victim.yxmd'
    assert victim.read_text() == 'original'

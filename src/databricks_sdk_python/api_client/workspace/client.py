from typing import Optional

from requests.auth import AuthBase

from databricks_sdk_python.api_client.client import BaseClient


class WorkspaceClient(BaseClient):
    def __init__(self, workspace_host: str, auth: Optional[AuthBase] = None):
        super().__init__(host=workspace_host, auth=auth)
        self.workspace_host = workspace_host

        from databricks_sdk_python.api_client.workspace.unity_catalog.client import UnityCatalogClient

        self.unity_catalog = UnityCatalogClient(self)


_client_cache = {}


def get_workspace_client(workspace_host: str, auth: Optional[AuthBase] = None) -> WorkspaceClient:
    client = _client_cache.get(workspace_host)
    if client is not None and (auth is None or client.auth == auth):
        return client
    _client_cache[workspace_host] = WorkspaceClient(workspace_host=workspace_host, auth=auth)
    return _client_cache[workspace_host]

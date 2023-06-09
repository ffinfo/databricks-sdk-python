from typing import Optional

from requests.auth import AuthBase

from databricks_sdk_python.api_client.client import BaseClient


class WorkspaceClient(BaseClient):
    def __init__(self, workspace_host: str, auth: Optional[AuthBase] = None):
        super().__init__(host=workspace_host, auth=auth)
        self.workspace_host = workspace_host

        from databricks_sdk_python.api_client.workspace.cluster_policies import ClusterPoliciesClient
        from databricks_sdk_python.api_client.workspace.instance_profiles import InstanceProfilesClient
        from databricks_sdk_python.api_client.workspace.permissions import PermissionsClient
        from databricks_sdk_python.api_client.workspace.unity_catalog.client import UnityCatalogClient

        self.cluster_policies = ClusterPoliciesClient(self)
        self.unity_catalog = UnityCatalogClient(self)
        self.permissions = PermissionsClient(self)
        self.instance_profiles = InstanceProfilesClient(self)


_client_cache = {}


def get_workspace_client(workspace_host: str, auth: Optional[AuthBase] = None) -> WorkspaceClient:
    client = _client_cache.get(workspace_host)
    if client is not None and (auth is None or client.auth == auth):
        return client
    _client_cache[workspace_host] = WorkspaceClient(workspace_host=workspace_host, auth=auth)
    return _client_cache[workspace_host]

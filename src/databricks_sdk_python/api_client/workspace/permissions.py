from typing import Optional

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.permissions import Permissions, PermissionLevels


class PermissionsClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def get(self, object_type: str, object_id: str) -> Optional[Permissions]:
        response = self.workspace_client._get(f"/api/2.0/permissions/{object_type}/{object_id}")
        if response.status_code == 200:
            return Permissions(**response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

    def get_permission_levels(self, object_type: str, object_id: str) -> Optional[PermissionLevels]:
        response = self.workspace_client._get(f"/api/2.0/permissions/{object_type}/{object_id}/permissionLevels")
        if response.status_code == 200:
            return PermissionLevels(**response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

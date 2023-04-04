from typing import Optional

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.metastores import WorkspaceMetastoreAssignment


class UnityCatalogMetastoreClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def get_current_assignment(self) -> Optional[WorkspaceMetastoreAssignment]:
        response = self.workspace_client._get("/api/2.1/unity-catalog/current-metastore-assignment")
        if response.status_code == 404:
            return None
        elif response.status_code == 200:
            return WorkspaceMetastoreAssignment(**response.json(), workspace_host=self.workspace_client.workspace_host)
        else:
            raise UnknownApiResponse(response)

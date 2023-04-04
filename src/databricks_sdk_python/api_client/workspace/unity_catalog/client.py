from databricks_sdk_python.api_client.workspace.client import WorkspaceClient


class UnityCatalogClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

        from databricks_sdk_python.api_client.workspace.unity_catalog.metastores import UnityCatalogMetastoreClient

        self.metastores = UnityCatalogMetastoreClient(self.workspace_client)

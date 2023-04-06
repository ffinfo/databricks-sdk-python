from typing import Dict, List, Optional

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.schemas import Schema


class UnityCatalogSchemaClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def list(self, catalog_name: str) -> List[Schema]:
        """List all schemas on the databricks account"""
        response = self.workspace_client._get("/api/2.1/unity-catalog/schemas", params={"catalog_name": catalog_name})
        if response.status_code == 200:
            return [Schema(**r, workspace_host=self.workspace_client.host) for r in response.json().get("schemas", [])]
        elif response.status_code == 404:
            return []
        else:
            raise UnknownApiResponse(response)

    def get_by_name(self, catalog_name: str, schema_name: str) -> Optional[Schema]:
        """Get schema by id"""
        response = self.workspace_client._get(f"/api/2.1/unity-catalog/schemas/{catalog_name}.{schema_name}")
        if response.status_code == 200:
            return Schema(**response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

    def create(
        self,
        catalog_name: str,
        schema_name: str,
        storage_root: Optional[str] = None,
        properties: Dict[str, str] = None,
        comment: Optional[str] = None,
    ) -> Schema:
        """Creates a new schema"""
        body = {
            "catalog_name": catalog_name,
            "name": schema_name,
            "storage_root": storage_root,
            "properties": properties,
            "comment": comment,
        }
        response = self.workspace_client._post("/api/2.1/unity-catalog/schemas", body=body)
        if response.status_code == 200:
            return Schema(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def update(
        self,
        catalog_name: str,
        schema_name: str,
        new_name: Optional[str] = None,
        properties: Dict[str, str] = None,
        comment: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> Schema:
        """Update a schema"""
        body = {
            "name": new_name,
            "comment": comment,
            "properties": properties,
            "owner": owner,
        }
        response = self.workspace_client._patch(
            f"/api/2.1/unity-catalog/schemas/{catalog_name}.{schema_name}", body=body
        )
        if response.status_code == 200:
            return Schema(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def delete(self, catalog_name: str, schema_name: str, force: bool = False):
        """Deletes a schema"""
        response = self.workspace_client._delete(
            f"/api/2.1/unity-catalog/schemas/{catalog_name}.{schema_name}", params={"force": str(force).lower()}
        )
        if response.status_code != 200:
            raise UnknownApiResponse(response)

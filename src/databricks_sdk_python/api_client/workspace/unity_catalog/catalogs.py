from typing import Dict, List, Optional

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.catalogs import Catalog


class UnityCatalogCatalogClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def list(self) -> List[Catalog]:
        """List all catalogs on the databricks account"""
        response = self.workspace_client._get("/api/2.1/unity-catalog/catalogs")
        if response.status_code == 200:
            return [
                Catalog(**r, workspace_host=self.workspace_client.host) for r in response.json().get("catalogs", [])
            ]
        elif response.status_code == 404:
            return []
        else:
            raise UnknownApiResponse(response)

    def get_by_name(self, catalog_name: str) -> Optional[Catalog]:
        """Get catalog by id"""
        response = self.workspace_client._get(f"/api/2.1/unity-catalog/catalogs/{catalog_name}")
        if response.status_code == 200:
            return Catalog(**response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

    def create(
        self,
        name: str,
        storage_root: Optional[str] = None,
        provider_name: Optional[str] = None,
        properties: Dict[str, str] = None,
        comment: Optional[str] = None,
        share_name: Optional[str] = None,
    ) -> Catalog:
        """Creates a new catalog"""
        body = {
            "name": name,
            "storage_root": storage_root,
            "provider_name": provider_name,
            "properties": properties,
            "comment": comment,
            "share_name": share_name,
        }
        response = self.workspace_client._post("/api/2.1/unity-catalog/catalogs", body=body)
        if response.status_code == 200:
            return Catalog(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def update(
        self,
        name: str,
        new_name: Optional[str] = None,
        comment: Optional[str] = None,
        properties: Dict[str, str] = None,
        owner: Optional[str] = None,
    ) -> Catalog:
        """Update a catalog"""
        body = {
            "name": new_name,
            "comment": comment,
            "properties": properties,
            "owner": owner,
        }
        response = self.workspace_client._patch(f"/api/2.1/unity-catalog/catalogs/{name}", body=body)
        if response.status_code == 200:
            return Catalog(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def delete(self, name: str, force: bool = False):
        """Deletes a catalog"""
        response = self.workspace_client._delete(
            f"/api/2.1/unity-catalog/catalogs/{name}", params={"force": str(force).lower()}
        )
        if response.status_code != 200:
            raise UnknownApiResponse(response)

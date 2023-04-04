from typing import List, Optional
from uuid import UUID

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.metastores import Metastore, WorkspaceMetastoreAssignment


class UnityCatalogMetastoreClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def get_current_assignment(self) -> Optional[WorkspaceMetastoreAssignment]:
        """Get current workspace assignment"""
        response = self.workspace_client._get("/api/2.1/unity-catalog/current-metastore-assignment")
        if response.status_code == 404:
            return None
        elif response.status_code == 200:
            return WorkspaceMetastoreAssignment(**response.json(), workspace_host=self.workspace_client.workspace_host)
        else:
            raise UnknownApiResponse(response)

    def list(self) -> List[Metastore]:
        """List all metastores on the databricks account"""
        response = self.workspace_client._get("/api/2.1/unity-catalog/metastores")
        if response.status_code == 200:
            return [
                Metastore(**r, workspace_host=self.workspace_client.host) for r in response.json().get("metastores", [])
            ]
        elif response.status_code == 404:
            return []
        else:
            raise UnknownApiResponse(response)

    def get_by_id(self, metastore_id: UUID) -> Optional[Metastore]:
        """Get metastore by id"""
        response = self.workspace_client._get(f"/api/2.1/unity-catalog/metastores/{metastore_id}")
        if response.status_code == 200:
            return Metastore(**response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

    def get_by_name(self, metastore_name: str) -> Optional[Metastore]:
        """Get metastore by name"""
        for m in self.list():
            if m.name == metastore_name:
                return m
        return None

    def create(self, name: str, storage_root: str, region: Optional[str] = None) -> Metastore:
        """Creates a new metastore"""
        body = {
            "name": name,
            "storage_root": storage_root,
            "region": region,
        }
        response = self.workspace_client._post("/api/2.1/unity-catalog/metastores", body=body)
        if response.status_code == 200:
            return Metastore(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def update(
        self,
        metastore_id: UUID,
        name: Optional[str] = None,
        delta_sharing_scope: Optional[str] = None,
        storage_root_credential_id: Optional[UUID] = None,
        privilege_model_version: Optional[str] = None,
        delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None,
        delta_sharing_organization_name: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> Metastore:
        """Update a metastore"""
        body = {
            "name": name,
            "delta_sharing_scope": delta_sharing_scope,
            "storage_root_credential_id": str(storage_root_credential_id),
            "privilege_model_version": privilege_model_version,
            "delta_sharing_recipient_token_lifetime_in_seconds": delta_sharing_recipient_token_lifetime_in_seconds,
            "delta_sharing_organization_name": delta_sharing_organization_name,
            "owner": owner,
        }
        response = self.workspace_client._patch(f"/api/2.1/unity-catalog/metastores/{metastore_id}", body=body)
        if response.status_code == 200:
            return Metastore(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def delete(self, metastore_id: UUID, force: bool = False):
        """Deletes a metastore"""
        response = self.workspace_client._delete(
            f"/api/2.1/unity-catalog/metastores/{metastore_id}", params={"force": str(force).lower()}
        )
        if response.status_code != 200:
            raise UnknownApiResponse(response)

    def create_assignment(
        self,
        workspace_id: int,
        metastore_id: UUID,
        default_catalog_name: str,
    ) -> WorkspaceMetastoreAssignment:
        body = {
            "metastore_id": str(metastore_id),
            "default_catalog_name": default_catalog_name,
        }
        response = self.workspace_client._put(f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", body=body)
        if response.status_code == 200:
            return WorkspaceMetastoreAssignment(
                workspace_id=workspace_id,
                metastore_id=metastore_id,
                default_catalog_name=default_catalog_name,
                workspace_host=self.workspace_client.host,
            )
        else:
            raise UnknownApiResponse(response)

    def update_assignment(
        self,
        workspace_id: int,
        metastore_id: UUID,
        default_catalog_name: Optional[str] = None,
    ) -> WorkspaceMetastoreAssignment:
        body = {
            "metastore_id": str(metastore_id),
            "default_catalog_name": default_catalog_name,
        }
        response = self.workspace_client._patch(
            f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", body=body
        )
        if response.status_code == 200:
            return WorkspaceMetastoreAssignment(
                workspace_id=workspace_id,
                metastore_id=metastore_id,
                default_catalog_name=default_catalog_name,
                workspace_host=self.workspace_client.host,
            )
        else:
            raise UnknownApiResponse(response)

    def delete_assignment(
        self,
        workspace_id: int,
        metastore_id: UUID,
    ):
        body = {
            "metastore_id": str(metastore_id),
        }
        response = self.workspace_client._delete(
            f"/api/2.1/unity-catalog/workspaces/{workspace_id}/metastore", body=body
        )
        if response.status_code != 200:
            raise UnknownApiResponse(response)

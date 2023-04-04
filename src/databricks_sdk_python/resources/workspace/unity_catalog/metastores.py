from typing import Optional
from uuid import UUID

from databricks_sdk_python.resources.base import WorkspaceModel


class WorkspaceMetastoreAssignment(WorkspaceModel):
    workspace_id: int
    metastore_id: UUID
    default_catalog_name: str

    def update(self, metastore_id: Optional[UUID] = None, default_catalog_name: Optional[str] = None):
        """Updates current assignment"""
        client = self.get_workspace_client()
        response = client.unity_catalog.metastores.update_assignment(
            self.workspace_id, metastore_id=metastore_id, default_catalog_name=default_catalog_name
        )
        self.metastore_id = response.metastore_id
        self.default_catalog_name = response.default_catalog_name

    def delete(self):
        """Remove metastore from workspace"""
        client = self.get_workspace_client()
        client.unity_catalog.metastores.delete_assignment(
            workspace_id=self.workspace_id, metastore_id=self.metastore_id
        )


class Metastore(WorkspaceModel):
    metastore_id: UUID
    global_metastore_id: str
    name: str
    full_name: str
    region: str
    cloud: str
    storage_root: str
    storage_root_credential_id: UUID
    storage_root_credential_name: str
    delta_sharing_scope: str
    delta_sharing_recipient_token_lifetime_in_seconds: Optional[int]
    owner: str
    privilege_model_version: str
    full_name: str
    securable_type: str
    securable_kind: str
    created_at: int
    updated_by: str
    updated_at: int
    updated_by: str

    def refresh(self):
        """Refresh to current state"""
        client = self.get_workspace_client()
        result = client.unity_catalog.metastores.get_by_id(self.metastore_id)
        if result is None:
            raise RuntimeError(f"{self.metastore_id} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def update(
        self,
        name: Optional[str] = None,
        delta_sharing_scope: Optional[str] = None,
        storage_root_credential_id: Optional[UUID] = None,
        privilege_model_version: Optional[str] = None,
        delta_sharing_recipient_token_lifetime_in_seconds: Optional[int] = None,
        delta_sharing_organization_name: Optional[str] = None,
        owner: Optional[str] = None,
    ):
        """Updates fields that can be updated"""
        client = self.get_workspace_client()
        result = client.unity_catalog.metastores.update(
            self.metastore_id,
            name=name,
            delta_sharing_scope=delta_sharing_scope,
            storage_root_credential_id=storage_root_credential_id,
            privilege_model_version=privilege_model_version,
            delta_sharing_recipient_token_lifetime_in_seconds=delta_sharing_recipient_token_lifetime_in_seconds,
            delta_sharing_organization_name=delta_sharing_organization_name,
            owner=owner,
        )
        for key, value in result:
            self.__dict__[key] = value

    def delete(self, force: bool = False):
        """Deletes workspace config"""
        client = self.get_workspace_client()
        client.unity_catalog.metastores.delete(self.metastore_id, force=force)

    def assign_to_workspace(self, workspace_id: int, default_catalog_name: str = "main"):
        client = self.get_workspace_client()
        return client.unity_catalog.metastores.create_assignment(
            workspace_id=workspace_id, metastore_id=self.metastore_id, default_catalog_name=default_catalog_name
        )

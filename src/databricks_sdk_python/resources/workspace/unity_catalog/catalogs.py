from typing import Dict, Optional
from uuid import UUID

from databricks_sdk_python.resources.base import WorkspaceModel


class Catalog(WorkspaceModel):
    name: str
    metastore_id: UUID
    comment: Optional[str]
    catalog_type: str
    storage_root: Optional[str]
    storage_location: Optional[str]
    provider_name: Optional[str]
    properties: Dict[str, str] = {}
    share_name: Optional[str]
    owner: str
    securable_type: str
    securable_kind: str
    created_at: int
    updated_by: str
    updated_at: int
    updated_by: str

    def refresh(self):
        """Refresh to current state"""
        client = self.get_workspace_client()
        result = client.unity_catalog.catalogs.get_by_name(self.name)
        if result is None:
            raise RuntimeError(f"{self.name} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def update(
        self,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        properties: Optional[dict] = None,
        owner: Optional[str] = None,
    ):
        """Updates fields that can be updated"""
        client = self.get_workspace_client()
        result = client.unity_catalog.catalogs.update(
            self.name,
            new_name=name,
            comment=comment,
            properties=properties,
            owner=owner,
        )
        for key, value in result:
            self.__dict__[key] = value

    def delete(self, force: bool = False):
        """Deletes workspace config"""
        client = self.get_workspace_client()
        client.unity_catalog.catalogs.delete(self.name, force=force)

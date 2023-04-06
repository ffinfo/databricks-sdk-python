from typing import Dict, Optional
from uuid import UUID

from databricks_sdk_python.resources.base import WorkspaceModel


class Schema(WorkspaceModel):
    name: str
    metastore_id: UUID
    catalog_type: str
    catalog_name: str
    comment: Optional[str]
    storage_root: Optional[str]
    storage_location: Optional[str]
    properties: Dict[str, str] = {}
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
        result = client.unity_catalog.schemas.get_by_name(catalog_name=self.catalog_name, schema_name=self.name)
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
        result = client.unity_catalog.schemas.update(
            catalog_name=self.catalog_name,
            schema_name=self.name,
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
        client.unity_catalog.schemas.delete(catalog_name=self.catalog_name, schema_name=self.name, force=force)

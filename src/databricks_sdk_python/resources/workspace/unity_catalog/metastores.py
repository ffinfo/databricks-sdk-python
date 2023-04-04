from typing import Optional
from uuid import UUID

from databricks_sdk_python.resources.base import WorkspaceModel


class WorkspaceMetastoreAssignment(WorkspaceModel):
    workspace_id: int
    metastore_id: UUID
    default_catalog_name: Optional[str]

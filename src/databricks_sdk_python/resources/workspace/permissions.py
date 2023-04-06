from typing import List, Optional, Union

from databricks_sdk_python.resources.base import WorkspaceModel
from pydantic import BaseModel


class Permission(BaseModel):
    permission_level: str
    inherited: bool
    inherited_from_object: Optional[List[str]]


class GroupPermission(Permission):
    group_name: str


class UserPermission(Permission):
    user_name: str


class ServicePrincipalPermission(Permission):
    service_principal_name: str


class PermissionsAccessControl(BaseModel):
    all_permissions: List[Union[UserPermission, GroupPermission, ServicePrincipalPermission]]


class Permissions(WorkspaceModel):
    object_id: str
    object_type: str
    access_control_list: List[PermissionsAccessControl]


class PermissionLevel(BaseModel):
    permission_level: str
    description: str


class PermissionLevels(WorkspaceModel):
    permission_levels: List[PermissionLevel]
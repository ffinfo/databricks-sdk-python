from typing import List, Optional, Union

from pydantic import BaseModel

from databricks_sdk_python.resources.base import WorkspaceModel


class PermissionLevel(BaseModel):
    permission_level: str
    description: str


class PermissionLevels(WorkspaceModel):
    permission_levels: List[PermissionLevel]


class ObjectPermission(BaseModel):
    permission_level: str


class UserObjectPermission(ObjectPermission):
    user_name: str


class GroupObjectPermission(ObjectPermission):
    group_name: str


class ServicePrincipalObjectPermission(ObjectPermission):
    service_principal_name: str


class Permission(BaseModel):
    permission_level: str
    inherited: bool
    inherited_from_object: Optional[List[str]]


class PermissionsAccessControl(BaseModel):
    all_permissions: List[Permission]


class GroupAccessControl(PermissionsAccessControl):
    group_name: str


class UserAccessControl(PermissionsAccessControl):
    user_name: str


class ServicePrincipalAccessControl(PermissionsAccessControl):
    service_principal_name: str


class Permissions(WorkspaceModel):
    object_id: str
    object_type: str
    access_control_list: List[Union[UserAccessControl, GroupAccessControl, ServicePrincipalAccessControl]]

    def _get_url_objets(self):
        result = self.object_id.strip("/").split("/")
        return {"object_type": result[0], "object_id": result[1]}

    def get_permission_levels(self) -> PermissionLevels:
        client = self.get_workspace_client()
        return client.permissions.get_permission_levels(**self._get_url_objets())

    def grant(
        self,
        access_control_list: List[Union[UserObjectPermission, GroupObjectPermission, ServicePrincipalObjectPermission]],
    ) -> "Permissions":
        client = self.get_workspace_client()
        result = client.permissions.grant(**self._get_url_objets(), access_control_list=access_control_list)
        self.access_control_list = result.access_control_list
        return self

    def replace(
        self,
        access_control_list: List[Union[UserObjectPermission, GroupObjectPermission, ServicePrincipalObjectPermission]],
    ) -> "Permissions":
        client = self.get_workspace_client()
        result = client.permissions.replace(**self._get_url_objets(), access_control_list=access_control_list)
        self.access_control_list = result.access_control_list
        return self

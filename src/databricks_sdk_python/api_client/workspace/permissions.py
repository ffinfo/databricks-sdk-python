from typing import List, Optional, Union

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.permissions import (
    GroupObjectPermission,
    PermissionLevels,
    Permissions,
    ServicePrincipalObjectPermission,
    UserObjectPermission,
)


class PermissionsClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def get(self, object_type: str, object_id: str) -> Optional[Permissions]:
        response = self.workspace_client._get(f"/api/2.0/permissions/{object_type}/{object_id}")
        if response.status_code == 200:
            return Permissions(**response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

    def get_permission_levels(self, object_type: str, object_id: str) -> PermissionLevels:
        response = self.workspace_client._get(f"/api/2.0/permissions/{object_type}/{object_id}/permissionLevels")
        if response.status_code == 200:
            return PermissionLevels(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def grant(
        self,
        object_type: str,
        object_id: str,
        access_control_list: List[Union[UserObjectPermission, GroupObjectPermission, ServicePrincipalObjectPermission]],
    ):
        body = {"access_control_list": [x.dict() for x in access_control_list]}
        response = self.workspace_client._patch(f"/api/2.0/permissions/{object_type}/{object_id}", body=body)
        if response.status_code == 200:
            return Permissions(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def replace(
        self,
        object_type: str,
        object_id: str,
        access_control_list: List[Union[UserObjectPermission, GroupObjectPermission, ServicePrincipalObjectPermission]],
    ):
        body = {"access_control_list": [x.dict() for x in access_control_list]}
        response = self.workspace_client._put(f"/api/2.0/permissions/{object_type}/{object_id}", body=body)
        if response.status_code == 200:
            return Permissions(**response.json(), workspace_host=self.workspace_client.host)
        else:
            raise UnknownApiResponse(response)

    def get_token_permissions(self):
        return self.get("authorization", "tokens")

    def get_password_permissions(self):
        return self.get("authorization", "passwords")

    def get_cluster_permissions(self, cluster_id: str):
        return self.get("clusters", cluster_id)

    def get_cluster_policy_permissions(self, cluster_policy_id: str):
        return self.get("cluster-policies", cluster_policy_id)

    def get_instance_pool_permissions(self, instance_pool_id: str):
        return self.get("instance-pools", instance_pool_id)

    def get_job_permissions(self, job_id: int):
        return self.get("jobs", str(job_id))

    def get_pipeline_permissions(self, pipeline_id: str):
        return self.get("pipelines", pipeline_id)

    def get_notebook_permissions(self, notebook_id: int):
        return self.get("notebooks", str(notebook_id))

    def get_directory_permissions(self, directory_id: int):
        return self.get("directories", str(directory_id))

    def get_experiment_permissions(self, experiment_id: int):
        return self.get("experiments", str(experiment_id))

    def get_registered_model_permissions(self, registered_model_id: str):
        return self.get("registered-models", registered_model_id)

    def get_sql_warehouses_permissions(self, warehouse_id: str):
        return self.get("sql/warehouses", warehouse_id)

    def get_repo_permissions(self, repo_id: int):
        return self.get("repos", str(repo_id))

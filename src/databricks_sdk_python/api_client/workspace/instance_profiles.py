from typing import List, Optional

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.instance_profiles import InstanceProfile


class InstanceProfilesClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def list(self) -> List[InstanceProfile]:
        """list all instance profiles"""
        response = self.workspace_client._get("/api/2.0/instance-profiles/list")
        if response.status_code == 200:
            return [
                InstanceProfile(**i, workspace_host=self.workspace_client.host)
                for i in response.json().get("instance_profiles", [])
            ]
        elif response.status_code == 404:
            return []
        else:
            raise UnknownApiResponse(response)

    def get(self, instance_profile: str) -> Optional[InstanceProfile]:
        """Get instance profile by arn or name"""
        for i in self.list():
            if i.instance_profile_arn == instance_profile or i.instance_profile_arn.endswith(f"/{instance_profile}"):
                return i
        return None

    def create(
        self,
        instance_profile_arn: str,
        iam_role_arn: Optional[str] = None,
        is_meta_instance_profile: bool = False,
        skip_validation: bool = False,
    ) -> InstanceProfile:
        """Creates an instance profile"""
        body = {
            "instance_profile_arn": instance_profile_arn,
            "iam_role_arn": iam_role_arn,
            "is_meta_instance_profile": is_meta_instance_profile,
            "skip_validation": skip_validation,
        }
        response = self.workspace_client._post("/api/2.0/instance-profiles/add", body=body)
        if response.status_code == 200:
            return InstanceProfile(
                instance_profile_arn=instance_profile_arn,
                iam_role_arn=iam_role_arn,
                is_meta_instance_profile=is_meta_instance_profile,
                workspace_host=self.workspace_client.host,
            )
        else:
            raise UnknownApiResponse(response)

    def update(
        self,
        instance_profile_arn: str,
        iam_role_arn: Optional[str] = None,
        is_meta_instance_profile: Optional[bool] = None,
    ) -> InstanceProfile:
        """Creates an instance profile"""
        body = {
            "instance_profile_arn": instance_profile_arn,
            "iam_role_arn": iam_role_arn,
            "is_meta_instance_profile": is_meta_instance_profile,
        }
        response = self.workspace_client._post("/api/2.0/instance-profiles/edit", body=body)
        if response.status_code == 200:
            return InstanceProfile(
                instance_profile_arn=instance_profile_arn,
                iam_role_arn=iam_role_arn,
                is_meta_instance_profile=is_meta_instance_profile,
                workspace_host=self.workspace_client.host,
            )
        else:
            raise UnknownApiResponse(response)

    def delete(self, instance_profile_arn: str):
        """Deletes an instance profile from the workspace"""
        body = {"instance_profile_arn": instance_profile_arn}
        response = self.workspace_client._post("/api/2.0/instance-profiles/remove", body=body)
        if response.status_code != 200:
            raise UnknownApiResponse(response)

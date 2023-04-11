from typing import Optional

from databricks_sdk_python.resources.base import WorkspaceModel


class InstanceProfile(WorkspaceModel):
    instance_profile_arn: str
    iam_role_arn: Optional[str]
    is_meta_instance_profile: bool

    def refresh(self):
        """Refresh to current state"""
        client = self.get_workspace_client()
        result = client.instance_profiles.get(self.instance_profile_arn)
        if result is None:
            raise RuntimeError(f"{self.instance_profile_arn} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def update(self, iam_role_arn: Optional[str] = None, is_meta_instance_profile: Optional[bool] = None):
        """Update instance profile"""
        client = self.get_workspace_client()
        result = client.instance_profiles.update(
            instance_profile_arn=self.instance_profile_arn,
            iam_role_arn=iam_role_arn,
            is_meta_instance_profile=is_meta_instance_profile,
        )
        for key, value in result:
            self.__dict__[key] = value

    def delete(self):
        """Deletes instance profile from workspace"""
        client = self.get_workspace_client()
        client.instance_profiles.delete(self.instance_profile_arn)

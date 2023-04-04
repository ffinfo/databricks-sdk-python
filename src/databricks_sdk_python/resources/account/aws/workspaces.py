from time import sleep
from typing import Optional
from uuid import UUID

from requests.auth import AuthBase

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.api_client.workspace.client import get_workspace_client as base_get_workspace_client
from databricks_sdk_python.resources.base import AwsAccountModel


class Workspace(AwsAccountModel):
    workspace_id: int
    workspace_name: str
    deployment_name: str
    aws_region: str
    pricing_tier: str
    workspace_status: str
    workspace_status_message: str
    credentials_id: UUID
    storage_configuration_id: UUID
    network_id: Optional[UUID]
    managed_services_customer_managed_key_id: Optional[UUID]
    private_access_settings_id: Optional[UUID]
    storage_customer_managed_key_id: Optional[UUID]
    is_no_public_ip_enabled: Optional[bool]
    creation_time: int

    def get_workspace_host(self):
        return f"{self.deployment_name}.cloud.databricks.com"

    def get_workspace_client(self, auth: Optional[AuthBase] = None) -> WorkspaceClient:
        return base_get_workspace_client(workspace_host=self.get_workspace_host(), auth=auth)

    def wait_on_provisioning(self):
        self.refresh()
        while self.workspace_status == "PROVISIONING":
            sleep(10)
            self.refresh()

    def refresh(self):
        """Update to current state"""
        client = self.get_account_client()
        result = client.workspaces.get_by_id(self.workspace_id)
        if result is None:
            raise RuntimeError(f"{self.workspace_id} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def update(
        self,
        aws_region: Optional[str] = None,
        credentials_id: Optional[UUID] = None,
        storage_configuration_id: Optional[UUID] = None,
        network_id: Optional[UUID] = None,
        managed_services_customer_managed_key_id: Optional[UUID] = None,
        storage_customer_managed_key_id: Optional[UUID] = None,
        private_access_settings_id: Optional[UUID] = None,
    ):
        """Updates fields that can be updated"""
        client = self.get_account_client()
        result = client.workspaces.update(
            self.workspace_id,
            aws_region=aws_region,
            credentials_id=credentials_id,
            storage_configuration_id=storage_configuration_id,
            network_id=network_id,
            managed_services_customer_managed_key_id=managed_services_customer_managed_key_id,
            storage_customer_managed_key_id=storage_customer_managed_key_id,
            private_access_settings_id=private_access_settings_id,
        )
        for key, value in result:
            self.__dict__[key] = value

    def delete(self):
        """Deletes workspace config"""
        client = self.get_account_client()
        client.workspaces.delete(self.workspace_id)

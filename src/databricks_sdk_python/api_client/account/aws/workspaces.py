from typing import List, Optional
from uuid import UUID

from databricks_sdk_python.api_client.account.aws.client import AwsAccountClient
from databricks_sdk_python.resources.aws_account.workspaces import Workspace


class AwsWorkspacesClient(object):
    def __init__(self, aws_account_client: AwsAccountClient):
        self.aws_account_client = aws_account_client

    def _get_path(self):
        return f"{self.aws_account_client._get_account_path()}/workspaces"

    def _get_id_path(self, workspace_id: int):
        return f"{self._get_path()}/{workspace_id}"

    def list(self) -> List[Workspace]:
        """List all workspace found on databricks account"""
        response = self.aws_account_client._get(self._get_path())
        if response.status_code == 404:
            return []
        return [Workspace(**x) for x in response.json()]

    def get_by_id(self, workspace_id: int) -> Optional[Workspace]:
        """Fetch a single workspace by id"""
        response = self.aws_account_client._get(self._get_id_path(workspace_id))
        if response.status_code == 404:
            return None
        return Workspace(**response.json())

    def get_by_name(self, workspace_name: str) -> Optional[Workspace]:
        """Fetch a single workspace by name"""
        for s in self.list():
            if s.workspace_name == workspace_name:
                return s
        return None

    def create(
        self,
        workspace_name: str,
        aws_region: str,
        pricing_tier: str,
        credentials_id: UUID,
        storage_configuration_id: UUID,
        network_id: Optional[UUID] = None,
        managed_services_customer_managed_key_id: Optional[UUID] = None,
        storage_customer_managed_key_id: Optional[UUID] = None,
        private_access_settings_id: Optional[UUID] = None,
        deployment_name: Optional[str] = None,
    ) -> Workspace:
        """Creates workspace on the databricks account"""
        body = {
            "workspace_name": workspace_name,
            "aws_region": aws_region,
            "pricing_tier": pricing_tier,
            "credentials_id": str(credentials_id),
            "storage_configuration_id": str(storage_configuration_id),
        }
        if network_id is not None:
            body["network_id"] = str(network_id)
        if managed_services_customer_managed_key_id is not None:
            body["managed_services_customer_managed_key_id"] = str(managed_services_customer_managed_key_id)
        if storage_customer_managed_key_id is not None:
            body["storage_customer_managed_key_id"] = str(storage_customer_managed_key_id)
        if private_access_settings_id is not None:
            body["private_access_settings_id"] = str(private_access_settings_id)
        if deployment_name is not None:
            body["deployment_name"] = str(deployment_name)
        response = self.aws_account_client._post(self._get_path(), body=body)
        return Workspace(**response.json())

    def update(
        self,
        workspace_id: int,
        aws_region: Optional[str] = None,
        credentials_id: Optional[UUID] = None,
        storage_configuration_id: Optional[UUID] = None,
        network_id: Optional[UUID] = None,
        managed_services_customer_managed_key_id: Optional[UUID] = None,
        storage_customer_managed_key_id: Optional[UUID] = None,
        private_access_settings_id: Optional[UUID] = None,
    ) -> Workspace:
        """Updates workspace on the databricks account"""
        body = {}
        if aws_region is not None:
            body["aws_region"] = aws_region
        if credentials_id is not None:
            body["credentials_id"] = str(credentials_id)
        if storage_configuration_id is not None:
            body["storage_configuration_id"] = str(storage_configuration_id)
        if network_id is not None:
            body["network_id"] = str(network_id)
        if managed_services_customer_managed_key_id is not None:
            body["managed_services_customer_managed_key_id"] = str(managed_services_customer_managed_key_id)
        if storage_customer_managed_key_id is not None:
            body["storage_customer_managed_key_id"] = str(storage_customer_managed_key_id)
        if private_access_settings_id is not None:
            body["private_access_settings_id"] = str(private_access_settings_id)
        response = self.aws_account_client._patch(self._get_id_path(workspace_id), body=body)
        return Workspace(**response.json())

    def delete(self, workspace_id: int):
        """Deletes a workspace from the databricks account"""
        response = self.aws_account_client._delete(self._get_id_path(workspace_id))
        if response.status_code == 404:
            raise RuntimeError("Not found")
        if response.status_code == 409:
            raise RuntimeError(str(response.json()))

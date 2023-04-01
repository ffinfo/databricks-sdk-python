from typing import List, Optional
from uuid import UUID

from databricks_sdk_python.api_client.account.aws.client import AwsAccountClient
from databricks_sdk_python.resources.aws_account.storage_config import StorageConfiguration


class AwsStorageConfigurationClient(object):
    def __init__(self, aws_account_client: AwsAccountClient):
        self.aws_account_client = aws_account_client

    def _get_path(self):
        return f"{self.aws_account_client._get_account_path()}/storage-configurations"

    def _get_id_path(self, storage_configuration_id: UUID):
        return f"{self._get_path()}/{storage_configuration_id}"

    def list(self) -> List[StorageConfiguration]:
        """List all storage_configuration found on databricks account"""
        response = self.aws_account_client._get(self._get_path())
        if response.status_code == 404:
            return []
        return [StorageConfiguration(**x) for x in response.json()]

    def get_by_id(self, storage_configuration_id: UUID) -> Optional[StorageConfiguration]:
        """Fetch a single storage_configuration by id"""
        response = self.aws_account_client._get(self._get_id_path(storage_configuration_id))
        if response.status_code == 404:
            return None
        return StorageConfiguration(**response.json())

    def get_by_name(self, storage_configuration_name: str) -> Optional[StorageConfiguration]:
        """Fetch a single storage_configuration by name"""
        for s in self.list():
            if s.storage_configuration_name == storage_configuration_name:
                return s
        return None

    def create(self, storage_configuration_name: str, bucket_name: str) -> StorageConfiguration:
        """Creates storage_configuration on the databricks account"""
        body = {
            "storage_configuration_name": storage_configuration_name,
            "root_bucket_info": {"bucket_name": bucket_name},
        }
        response = self.aws_account_client._post(self._get_path(), body=body)
        return StorageConfiguration(**response.json())

    def delete(self, storage_configuration_id: UUID):
        """Deletes a storage_configuration from the databricks account"""
        response = self.aws_account_client._delete(self._get_id_path(storage_configuration_id))
        if response.status_code == 404:
            raise RuntimeError("Not found")
        if response.status_code == 409:
            raise RuntimeError(str(response.json()))

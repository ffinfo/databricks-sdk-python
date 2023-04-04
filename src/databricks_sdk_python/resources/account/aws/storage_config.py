from uuid import UUID

from pydantic import BaseModel

from databricks_sdk_python.resources.base import AwsAccountModel


class RootBucketInfo(BaseModel):
    bucket_name: str


class StorageConfiguration(AwsAccountModel):
    storage_configuration_id: UUID
    storage_configuration_name: str
    root_bucket_info: RootBucketInfo
    account_id: UUID
    creation_time: int

    def refresh(self):
        """Update to current state"""
        client = self.get_account_client()
        result = client.storage_configuration.get_by_id(self.storage_configuration_id)
        if result is None:
            raise RuntimeError(f"{self.storage_configuration_id} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def delete(self):
        """Deletes storage configuration"""
        client = self.get_account_client()
        client.storage_configuration.delete(self.storage_configuration_id)

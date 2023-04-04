from uuid import UUID

from pydantic import BaseModel

from databricks_sdk_python.resources.base import AwsAccountModel


class StsRole(BaseModel):
    role_arn: str
    external_id: UUID


class AwsCredentials(BaseModel):
    sts_role: StsRole


class Credentials(AwsAccountModel):
    credentials_id: UUID
    credentials_name: str
    aws_credentials: AwsCredentials
    account_id: UUID
    creation_time: int

    def refresh(self):
        """Update to current state"""
        client = self.get_account_client()
        result = client.credentials.get_by_id(self.credentials_id)
        if result is None:
            raise RuntimeError(f"{self.credentials_id} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def delete(self):
        """Deletes credential"""
        client = self.get_account_client()
        client.credentials.delete(self.credentials_id)

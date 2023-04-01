from typing import List, Optional
from uuid import UUID

from databricks_sdk_python.api_client.account.aws.client import AwsAccountClient
from databricks_sdk_python.resources.aws_account.credentials import Credentials


class AwsCredentialsClient(object):
    def __init__(self, aws_account_client: AwsAccountClient):
        self.aws_account_client = aws_account_client

    def _get_path(self):
        return f"{self.aws_account_client._get_account_path()}/credentials"

    def _get_id_path(self, credentials_id: UUID):
        return f"{self._get_path()}/{credentials_id}"

    def list(self) -> List[Credentials]:
        """List all credentials found on databricks account"""
        response = self.aws_account_client._get(self._get_path())
        if response.status_code == 404:
            return []
        return [Credentials(**x) for x in response.json()]

    def get_by_id(self, credentials_id: UUID) -> Optional[Credentials]:
        """Fetch a single credentials by id"""
        response = self.aws_account_client._get(self._get_id_path(credentials_id))
        if response.status_code == 404:
            return None
        return Credentials(**response.json())

    def get_by_name(self, credentials_name: str) -> Optional[Credentials]:
        """Fetch a single credentials by name"""
        for c in self.list():
            if c.credentials_name == credentials_name:
                return c

    def create(self, credentials_name: str, role_arn: str) -> Credentials:
        """Creates credentials on the databricks account"""
        body = {
            "credentials_name": credentials_name,
            "aws_credentials": {"sts_role": {"role_arn": role_arn}},
        }
        response = self.aws_account_client._post(self._get_path(), body=body)
        return Credentials(**response.json())

    def delete(self, credentials_id: UUID):
        """Deletes a credentials from the databricks account"""
        response = self.aws_account_client._delete(self._get_id_path(credentials_id))
        if response.status_code == 404:
            raise RuntimeError("Not found")
        if response.status_code == 409:
            raise RuntimeError(str(response.json()))

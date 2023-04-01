from typing import Dict, Optional
from uuid import UUID

from requests.auth import HTTPBasicAuth

from databricks_sdk_python.api_client.client import BaseClient

ACCOUNT_API_PREFIX = "api/2.0/accounts"
ACCOUNT_HOST = "accounts.cloud.databricks.com"


class AwsAccountClient(BaseClient):
    def __init__(self, account_id: UUID, auth: Optional[HTTPBasicAuth] = None):
        super().__init__(host=ACCOUNT_HOST, auth=auth)
        self.account_id = account_id

        from databricks_sdk_python.api_client.account.aws.credentials import AwsCredentialsClient

        self.credentials = AwsCredentialsClient(self)

        from databricks_sdk_python.api_client.account.aws.storage_configuration import AwsStorageConfigurationClient

        self.storage_configuration = AwsStorageConfigurationClient(self)

        from databricks_sdk_python.api_client.account.aws.networks import AwsNetworksClient

        self.networks = AwsNetworksClient(self)

    def _get_account_path(self):
        return f"{ACCOUNT_API_PREFIX}/{self.account_id}"


_client_cache: Dict[UUID, AwsAccountClient] = {}


def get_aws_account_client(account_id: UUID, auth: Optional[HTTPBasicAuth] = None) -> AwsAccountClient:
    client = _client_cache.get(account_id)
    if client is not None and (auth is None or client.auth == auth):
        return client
    _client_cache[account_id] = AwsAccountClient(account_id=account_id, auth=auth)
    return _client_cache[account_id]

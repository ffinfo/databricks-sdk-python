import uuid

import pytest
from requests.auth import HTTPBasicAuth

from databricks_sdk_python.api_client.account.aws.client import AwsAccountClient
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient, get_workspace_client


@pytest.fixture
def aws_account_client():
    account_id = uuid.uuid4()

    return AwsAccountClient(account_id=account_id, auth=HTTPBasicAuth("user", "pass"))


@pytest.fixture
def workspace_client() -> WorkspaceClient:
    return get_workspace_client(workspace_host="test.cloud.databricks.com", auth=HTTPBasicAuth("user", "pass"))

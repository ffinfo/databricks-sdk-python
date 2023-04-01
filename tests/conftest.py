import uuid

import pytest
from requests.auth import HTTPBasicAuth

from databricks_sdk_python.api_client.account.aws.client import AwsAccountClient


@pytest.fixture
def aws_account_client():
    account_id = uuid.uuid4()

    return AwsAccountClient(account_id=account_id, auth=HTTPBasicAuth("user", "pass"))

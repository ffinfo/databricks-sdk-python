from databricks_sdk_python.api_client.account.aws.client import ACCOUNT_API_PREFIX, AwsAccountClient


def test_get_account_path(aws_account_client: AwsAccountClient):
    assert aws_account_client._get_account_path() == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}"

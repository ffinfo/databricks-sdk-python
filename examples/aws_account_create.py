from uuid import UUID

from databricks_sdk_python.api_client.account.aws.client import get_aws_account_client

account_id = UUID("<databricks account id>")

# This will assume you have set credentials in ~/.netrc for 'accounts.cloud.databricks.com'
# If not please also supply a requests.auth.HTTPBasicAuth as auth argument
account_client = get_aws_account_client(account_id=account_id)

credentials = account_client.credentials.create(credentials_name="<name>", role_arn="<iam role arn>")
storage_configuration = account_client.storage_configuration.create(
    storage_configuration_name="<name>", bucket_name="<bucket name>"
)

workspace = account_client.workspaces.create(
    workspace_name="name",
    aws_region="eu-west-1",
    pricing_tier="PREMIUM",
    credentials_id=credentials.credentials_id,
    storage_configuration_id=storage_configuration.storage_configuration_id,
)

# this will wait until the status of the workspace is not provisioning anymore
workspace.wait_on_provisioning()

workspace_client = workspace.get_workspace_client(account_client.auth)

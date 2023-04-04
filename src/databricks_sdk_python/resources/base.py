from uuid import UUID

from pydantic import BaseModel


class AccountBase(BaseModel):
    account_id: UUID


class AwsAccountModel(AccountBase):
    def get_account_client(self):
        from databricks_sdk_python.api_client.account.aws.client import get_aws_account_client

        return get_aws_account_client(self.account_id)


class WorkspaceModel(BaseModel):
    workspace_host: str

    def get_workspace_client(self):
        from databricks_sdk_python.api_client.workspace.client import get_workspace_client

        return get_workspace_client(self.workspace_host)

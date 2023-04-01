from uuid import UUID

from pydantic import BaseModel


class ModelBase(BaseModel):
    account_id: UUID


class AwsAccountModel(ModelBase):
    def get_account_client(self):
        from databricks_sdk_python.api_client.account.aws.client import get_aws_account_client

        return get_aws_account_client(self.account_id)

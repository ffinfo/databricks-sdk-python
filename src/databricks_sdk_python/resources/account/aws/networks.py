from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel

from databricks_sdk_python.resources.base import AwsAccountModel


class NetworkWarning(BaseModel):
    warning_type: str
    warning_message: str


class NetworkError(BaseModel):
    error_type: str
    error_message: str


class NetworkVpcEndpoints(BaseModel):
    rest_api: List[str]
    dataplane_relay: List[str]


class Network(AwsAccountModel):
    network_id: UUID
    network_name: str
    vpc_id: str
    subnet_ids: List[str]
    security_group_ids: List[str]
    vpc_status: Optional[str]
    warning_messages: List[NetworkWarning] = []
    error_messages: List[NetworkError] = []
    workspace_id: Optional[int]
    creation_time: int
    vpc_endpoints: Optional[NetworkVpcEndpoints]

    def refresh(self):
        """Update to current state"""
        client = self.get_account_client()
        result = client.networks.get_by_id(self.network_id)
        if result is None:
            raise RuntimeError(f"{self.network_id} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def delete(self):
        """Deletes network config"""
        client = self.get_account_client()
        client.networks.delete(self.network_id)

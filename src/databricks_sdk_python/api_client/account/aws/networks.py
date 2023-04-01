import json
from typing import List, Optional
from uuid import UUID

from databricks_sdk_python.api_client.account.aws.client import AwsAccountClient
from databricks_sdk_python.resources.aws_account.networks import Network, NetworkVpcEndpoints


class AwsNetworksClient(object):
    def __init__(self, aws_account_client: AwsAccountClient):
        self.aws_account_client = aws_account_client

    def _get_path(self):
        return f"{self.aws_account_client._get_account_path()}/networks"

    def _get_id_path(self, network_id: UUID):
        return f"{self._get_path()}/{network_id}"

    def list(self) -> List[Network]:
        """List all network found on databricks account"""
        response = self.aws_account_client._get(self._get_path())
        if response.status_code == 404:
            return []
        return [Network(**x) for x in response.json()]

    def get_by_id(self, network_id: UUID) -> Optional[Network]:
        """Fetch a single network by id"""
        response = self.aws_account_client._get(self._get_id_path(network_id))
        if response.status_code == 404:
            return None
        return Network(**response.json())

    def get_by_name(self, network_name: str) -> Optional[Network]:
        """Fetch a single network by name"""
        for s in self.list():
            if s.network_name == network_name:
                return s
        return None

    def create(
        self,
        network_name: str,
        vpc_id: str,
        subnet_ids: List[str],
        security_group_ids: List[str],
        vpc_endpoints: Optional[NetworkVpcEndpoints] = None,
    ) -> Network:
        """Creates network on the databricks account"""
        body = {
            "network_name": network_name,
            "vpc_id": vpc_id,
            "subnet_ids": subnet_ids,
            "security_group_ids": security_group_ids,
        }
        if vpc_endpoints is not None:
            body["vpc_endpoints"] = json.loads(vpc_endpoints.json())
        response = self.aws_account_client._post(self._get_path(), body=body)
        return Network(**response.json())

    def delete(self, network_id: UUID):
        """Deletes a network from the databricks account"""
        response = self.aws_account_client._delete(self._get_id_path(network_id))
        if response.status_code == 404:
            raise RuntimeError("Not found")
        if response.status_code == 409:
            raise RuntimeError(str(response.json()))

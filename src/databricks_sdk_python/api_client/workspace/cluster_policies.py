import json
from typing import Dict, List, Optional

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.cluster_policies import ClusterPolicy, PolicyElement


class ClusterPoliciesClient(object):
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client

    def list(self, sort_order: Optional[str] = None, sort_column: Optional[str] = None) -> List[ClusterPolicy]:
        """list all instance profiles"""
        body = {
            "sort_order": sort_order,
            "sort_column": sort_column,
        }
        response = self.workspace_client._get("/api/2.0/policies/clusters/list", body=body)
        if response.status_code == 200:
            return [
                ClusterPolicy.parse_json(i, workspace_host=self.workspace_client.host)
                for i in response.json().get("policies", [])
            ]
        elif response.status_code == 404:
            return []
        else:
            raise UnknownApiResponse(response)

    def get_by_id(self, policy_id: str) -> Optional[ClusterPolicy]:
        body = {"policy_id": policy_id}
        response = self.workspace_client._get("/api/2.0/policies/clusters/get", body=body)
        if response.status_code == 200:
            return ClusterPolicy.parse_json(response.json(), workspace_host=self.workspace_client.host)
        elif response.status_code == 404:
            return None
        else:
            raise UnknownApiResponse(response)

    def get_by_name(self, policy_name: str) -> Optional[ClusterPolicy]:
        policies = self.list()
        for p in policies:
            if p.name == policy_name:
                return p
        return None

    def _create(
        self,
        policy_name: str,
        definition: Optional[Dict[str, PolicyElement]] = None,
        description: Optional[str] = None,
        policy_family_id: Optional[str] = None,
        policy_family_definition_overrides: Optional[Dict[str, PolicyElement]] = None,
    ) -> ClusterPolicy:
        body = {
            "name": policy_name,
            "description": description,
        }
        if policy_family_id is not None:
            body["policy_family_id"] = policy_family_id
        if definition is not None:
            body["definition"] = json.dumps({k: d.dict(skip_defaults=True) for k, d in definition.items()})
        if policy_family_definition_overrides is not None:
            body["policy_family_definition_overrides"] = json.dumps(
                {k: d.dict(skip_defaults=True) for k, d in policy_family_definition_overrides.items()}
            )
        response = self.workspace_client._post("/api/2.0/policies/clusters/create", body=body)
        if response.status_code == 200:
            return self.get_by_id(response.json().get("policy_id"))
        else:
            raise UnknownApiResponse(response)

    def create(
        self, policy_name: str, definition: Dict[str, PolicyElement], description: Optional[str] = None
    ) -> ClusterPolicy:
        """creates a policy without family"""
        return self._create(policy_name=policy_name, definition=definition, description=description)

    def create_with_family(
        self,
        policy_name: str,
        policy_family_id: str,
        policy_family_definition_overrides: Dict[str, PolicyElement],
        description: Optional[str] = None,
    ) -> ClusterPolicy:
        """creates a policy with family"""
        return self._create(
            policy_name=policy_name,
            policy_family_id=policy_family_id,
            policy_family_definition_overrides=policy_family_definition_overrides,
            description=description,
        )

    def update(
        self,
        policy_id: str,
        policy_name: str,
        definition: Optional[Dict[str, PolicyElement]] = None,
        description: Optional[str] = None,
        policy_family_id: Optional[str] = None,
        policy_family_definition_overrides: Optional[Dict[str, PolicyElement]] = None,
    ):
        body = {"policy_id": policy_id, "name": policy_name, "description": description}
        if policy_family_id is not None:
            body["policy_family_id"] = policy_family_id
        if definition is not None:
            body["definition"] = json.dumps({k: d.dict(skip_defaults=True) for k, d in definition.items()})
        if policy_family_definition_overrides is not None:
            body["policy_family_definition_overrides"] = json.dumps(
                {k: d.dict(skip_defaults=True) for k, d in policy_family_definition_overrides.items()}
            )
        response = self.workspace_client._post("/api/2.0/policies/clusters/edit", body=body)
        if response.status_code != 200:
            raise UnknownApiResponse(response)

    def delete(self, policy_id: str):
        """Deletes a policy"""
        body = {
            "policy_id": policy_id,
        }
        response = self.workspace_client._post("/api/2.0/policies/clusters/delete", body=body)
        if response.status_code != 200:
            raise UnknownApiResponse(response)

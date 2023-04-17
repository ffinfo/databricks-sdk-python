import json
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from databricks_sdk_python.resources.base import WorkspaceModel
from databricks_sdk_python.resources.workspace.permissions import (
    GroupObjectPermission,
    Permissions,
    ServicePrincipalObjectPermission,
    UserObjectPermission,
)


class PolicyElement(BaseModel):
    type: str
    value: Optional[Any]
    hidden: Optional[bool]
    defaultValue: Optional[Any]
    isOptional: Optional[bool]
    minValue: Optional[int]
    maxValue: Optional[int]
    values: Optional[List[Any]]
    pattern: Optional[str]


class ClusterPolicy(WorkspaceModel):
    policy_id: str
    name: str
    description: Optional[str]
    definition: Dict[str, PolicyElement]
    is_default: bool
    policy_family_id: Optional[str]
    policy_family_version: Optional[int]
    policy_family_definition_overrides: Optional[Dict[str, PolicyElement]]
    creator_user_name: Optional[str]
    created_at_timestamp: int

    @staticmethod
    def parse_json(json_dict: dict, workspace_host: str) -> "ClusterPolicy":
        definition = json_dict.get("definition")
        json_dict["definition"] = {k: PolicyElement(**v) for k, v in json.loads(definition).items()}

        policy_family_definition_overrides = json_dict.get("policy_family_definition_overrides")
        if policy_family_definition_overrides is not None:
            json_dict["policy_family_definition_overrides"] = json.loads(policy_family_definition_overrides)

        return ClusterPolicy(**json_dict, workspace_host=workspace_host)

    def get_permissions(self) -> Permissions:
        """Get permissions of cluster policy"""
        client = self.get_workspace_client()
        result = client.permissions.get_cluster_policy_permissions(self.policy_id)
        if result is None:
            raise RuntimeError("No permissions found, policy is still there?")
        return result

    def grant_use(
        self,
        user_name: Optional[str] = None,
        group_name: Optional[str] = None,
        service_principal_name: Optional[str] = None,
    ) -> Permissions:
        """Grant user, group or service_principal to be able to use the policy"""
        acl = []
        if user_name is not None:
            acl.append(UserObjectPermission(user_name=user_name, permission_level="CAN_USE"))
        if group_name is not None:
            acl.append(GroupObjectPermission(group_name=group_name, permission_level="CAN_USE"))
        if service_principal_name is not None:
            acl.append(
                ServicePrincipalObjectPermission(
                    service_principal_name=service_principal_name, permission_level="CAN_USE"
                )
            )
        return self.get_permissions().grant(acl)

    def replace_permissions(
        self, user_names: List[str] = None, group_names: List[str] = None, service_principal_names: List[str] = None
    ) -> Permissions:
        """Replace users, groups and service_principals to be able to use the policy"""
        acl = []
        if user_names is not None:
            for user_name in user_names:
                acl.append(UserObjectPermission(user_name=user_name, permission_level="CAN_USE"))
        if group_names is not None:
            for group_name in group_names:
                acl.append(GroupObjectPermission(group_name=group_name, permission_level="CAN_USE"))
        if service_principal_names is not None:
            for service_principal_name in service_principal_names:
                acl.append(
                    ServicePrincipalObjectPermission(
                        service_principal_name=service_principal_name, permission_level="CAN_USE"
                    )
                )
        return self.get_permissions().replace(acl)

    def refresh(self):
        """Refresh to current state"""
        client = self.get_workspace_client()
        result = client.cluster_policies.get_by_id(self.policy_id)
        if result is None:
            raise RuntimeError(f"{self.policy_id} does not exists anymore")
        for key, value in result:
            self.__dict__[key] = value

    def update(
        self,
        policy_name: Optional[str] = None,
        definition: Optional[Dict[str, PolicyElement]] = None,
        description: Optional[str] = None,
        policy_family_id: Optional[str] = None,
        policy_family_definition_overrides: Optional[Dict[str, PolicyElement]] = None,
    ) -> "ClusterPolicy":
        client = self.get_workspace_client()
        self.name = policy_name or self.name
        self.description = description or self.description
        if self.policy_family_id is None:
            self.definition = definition or self.definition
            client.cluster_policies.update(
                self.policy_id,
                policy_name=self.name,
                description=self.description,
                definition=self.definition,
            )
        else:
            self.policy_family_id = policy_family_id or self.policy_family_id
            self.policy_family_definition_overrides = (
                policy_family_definition_overrides or self.policy_family_definition_overrides
            )
            client.cluster_policies.update(
                self.policy_id,
                policy_name=policy_name or self.name,
                description=description or self.description,
                policy_family_id=policy_family_id or self.policy_family_id,
                policy_family_definition_overrides=policy_family_definition_overrides
                or self.policy_family_definition_overrides,
            )
        return self

    def delete(self):
        """Deletes policy from workspace"""
        client = self.get_workspace_client()
        client.cluster_policies.delete(self.policy_id)

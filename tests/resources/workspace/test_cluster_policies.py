import json

from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.cluster_policies import ClusterPolicy
from databricks_sdk_python.resources.workspace.permissions import PermissionLevels, Permissions


class ClusterPolicyFactory(ModelFactory):
    __model__ = ClusterPolicy
    workspace_host = "test.cloud.databricks.com"


class PermissionsFactory(ModelFactory):
    __model__ = Permissions
    workspace_host = "test.cloud.databricks.com"


class PermissionLevelsFactory(ModelFactory):
    __model__ = PermissionLevels
    workspace_host = "test.cloud.databricks.com"


def test_get_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    policy: ClusterPolicy = ClusterPolicyFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/cluster-policies/{policy.policy_id}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    assert policy.get_permissions() == expected


def test_grant_use(workspace_client: WorkspaceClient, requests_mock):
    permissions: Permissions = PermissionsFactory.build()
    policy: ClusterPolicy = ClusterPolicyFactory.build()
    permissions.object_id = f"/cluster-policies/{policy.policy_id}"
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/cluster-policies/{policy.policy_id}",
        status_code=200,
        text=permissions.json(exclude={"workspace_host"}),
    )

    mock_request = requests_mock.patch(
        f"https://{workspace_client.host}/api/2.0/permissions/cluster-policies/{policy.policy_id}",
        status_code=200,
        text=permissions.json(exclude={"workspace_host"}),
    )

    assert (
        policy.grant_use(user_name="user", group_name="group", service_principal_name="service_principal")
        == permissions
    )

    assert mock_request.last_request.json() == {
        "access_control_list": [
            {"permission_level": "CAN_USE", "user_name": "user"},
            {"group_name": "group", "permission_level": "CAN_USE"},
            {"permission_level": "CAN_USE", "service_principal_name": "service_principal"},
        ]
    }


def test_replace_permissions(workspace_client: WorkspaceClient, requests_mock):
    permissions: Permissions = PermissionsFactory.build()
    policy: ClusterPolicy = ClusterPolicyFactory.build()
    permissions.object_id = f"/cluster-policies/{policy.policy_id}"
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/cluster-policies/{policy.policy_id}",
        status_code=200,
        text=permissions.json(exclude={"workspace_host"}),
    )

    mock_request = requests_mock.put(
        f"https://{workspace_client.host}/api/2.0/permissions/cluster-policies/{policy.policy_id}",
        status_code=200,
        text=permissions.json(exclude={"workspace_host"}),
    )

    assert (
        policy.replace_permissions(
            user_names=["user"], group_names=["group"], service_principal_names=["service_principal"]
        )
        == permissions
    )

    assert mock_request.last_request.json() == {
        "access_control_list": [
            {"permission_level": "CAN_USE", "user_name": "user"},
            {"group_name": "group", "permission_level": "CAN_USE"},
            {"permission_level": "CAN_USE", "service_principal_name": "service_principal"},
        ]
    }


def test_refresh(workspace_client: WorkspaceClient, requests_mock):
    policy: ClusterPolicy = ClusterPolicyFactory.build()
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    expected.policy_id = policy.policy_id

    response_json = json.loads(expected.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/get",
        status_code=200,
        json=response_json,
    )

    policy.refresh()

    assert policy == expected


def test_update(workspace_client: WorkspaceClient, requests_mock):
    policy: ClusterPolicy = ClusterPolicyFactory.build()
    policy.policy_family_id = None
    policy.definition = {}

    response_json = json.loads(policy.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/edit",
        status_code=200,
        json=response_json,
    )

    policy.update()

    assert mock_request.last_request.json() == {
        "definition": response_json["definition"],
        "description": policy.description,
        "name": policy.name,
        "policy_id": policy.policy_id,
    }


def test_update_family(workspace_client: WorkspaceClient, requests_mock):
    policy: ClusterPolicy = ClusterPolicyFactory.build()
    policy.policy_family_id = "id"
    policy.policy_family_definition_overrides = {}

    response_json = json.loads(policy.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/edit",
        status_code=200,
        json=response_json,
    )

    policy.update()

    assert mock_request.last_request.json() == {
        "policy_family_definition_overrides": response_json["policy_family_definition_overrides"],
        "policy_family_id": policy.policy_family_id,
        "description": policy.description,
        "name": policy.name,
        "policy_id": policy.policy_id,
    }


def test_delete(workspace_client: WorkspaceClient, requests_mock):
    policy: ClusterPolicy = ClusterPolicyFactory.build()

    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/delete",
        status_code=200,
    )

    policy.delete()

    assert mock_request.last_request.json() == {"policy_id": policy.policy_id}

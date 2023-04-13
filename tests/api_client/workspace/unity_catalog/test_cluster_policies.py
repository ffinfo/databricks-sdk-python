import json

from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.cluster_policies import ClusterPolicy


class ClusterPolicyFactory(ModelFactory):
    __model__ = ClusterPolicy
    workspace_host = "test.cloud.databricks.com"


def test_list_empty(workspace_client: WorkspaceClient, requests_mock):
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/list",
        status_code=404,
        json={"error": "Not found"},
    )
    metastores = workspace_client.cluster_policies.list()
    assert metastores == []

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/list",
        status_code=200,
        json={"policies": []},
    )
    metastores = workspace_client.cluster_policies.list()
    assert metastores == []


def test_list(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    response_json = json.loads(expected.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/list",
        status_code=200,
        json={"policies": [response_json]},
    )
    metastores = workspace_client.cluster_policies.list()
    assert metastores == [expected]


def test_get_by_name(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    response_json = json.loads(expected.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/list",
        status_code=200,
        json={"policies": [response_json]},
    )
    cluster_policy = workspace_client.cluster_policies.get_by_name(expected.name)
    assert cluster_policy == expected


def test_get_by_id(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    response_json = json.loads(expected.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    mock_request = requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/get",
        status_code=200,
        json=response_json,
    )
    cluster_policy = workspace_client.cluster_policies.get_by_id(expected.policy_id)
    assert cluster_policy == expected

    assert mock_request.last_request.json() == {
        "policy_id": expected.policy_id,
    }


def test_create(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    response_json = json.loads(expected.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/create",
        status_code=200,
        json={"policy_id": expected.policy_id},
    )
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/get",
        status_code=200,
        json=response_json,
    )
    result = workspace_client.cluster_policies.create(
        policy_name=expected.name,
        description=expected.description,
        definition=expected.definition,
    )
    assert result == expected

    assert mock_request.last_request.json() == {
        "name": expected.name,
        "description": expected.description,
        "definition": response_json["definition"],
    }


def test_create_with_family(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    expected.policy_family_definition_overrides = {}
    expected.policy_family_id = "id"
    response_json = json.loads(expected.json(exclude={"workspace_host"}))
    response_json["definition"] = json.dumps(response_json["definition"])
    response_json["policy_family_definition_overrides"] = json.dumps(
        response_json["policy_family_definition_overrides"]
    )
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/create",
        status_code=200,
        json={"policy_id": expected.policy_id},
    )
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/get",
        status_code=200,
        json=response_json,
    )
    result = workspace_client.cluster_policies.create_with_family(
        policy_name=expected.name,
        description=expected.description,
        policy_family_id=expected.policy_family_id,
        policy_family_definition_overrides=expected.policy_family_definition_overrides,
    )
    assert result == expected

    assert mock_request.last_request.json() == {
        "name": expected.name,
        "description": expected.description,
        "policy_family_id": expected.policy_family_id,
        "policy_family_definition_overrides": response_json["policy_family_definition_overrides"],
    }


def test_update(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/edit",
        status_code=200,
        json={},
    )
    workspace_client.cluster_policies.update(
        policy_id=expected.policy_id,
        policy_name=expected.name,
    )

    assert mock_request.last_request.json() == {
        "policy_id": expected.policy_id,
        "name": expected.name,
    }


def test_delete(workspace_client: WorkspaceClient, requests_mock):
    expected: ClusterPolicy = ClusterPolicyFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/policies/clusters/delete",
        status_code=200,
        json={},
    )
    workspace_client.cluster_policies.delete(expected.policy_id)

    assert mock_request.last_request.json() == {
        "policy_id": expected.policy_id,
    }

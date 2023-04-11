import json

from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.instance_profiles import InstanceProfile


class InstanceProfileFactory(ModelFactory):
    __model__ = InstanceProfile
    workspace_host = "test.cloud.databricks.com"


def test_list_empty(workspace_client: WorkspaceClient, requests_mock):
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/list",
        status_code=404,
        json={"error": "Not found"},
    )
    metastores = workspace_client.instance_profiles.list()
    assert metastores == []

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/list",
        status_code=200,
        json={"instance_profiles": []},
    )
    metastores = workspace_client.instance_profiles.list()
    assert metastores == []


def test_list(workspace_client: WorkspaceClient, requests_mock):
    expected: InstanceProfile = InstanceProfileFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/list",
        status_code=200,
        json={"instance_profiles": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    metastores = workspace_client.instance_profiles.list()
    assert metastores == [expected]


def test_get_by_arn(workspace_client: WorkspaceClient, requests_mock):
    expected: InstanceProfile = InstanceProfileFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/list",
        status_code=200,
        json={"instance_profiles": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    instance_profile = workspace_client.instance_profiles.get(expected.instance_profile_arn)
    assert instance_profile == expected


def test_get_by_name(workspace_client: WorkspaceClient, requests_mock):
    expected: InstanceProfile = InstanceProfileFactory.build()
    expected.instance_profile_arn = "blablabla/name"
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/list",
        status_code=200,
        json={"instance_profiles": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    instance_profile = workspace_client.instance_profiles.get("name")
    assert instance_profile == expected


def test_create(workspace_client: WorkspaceClient, requests_mock):
    expected: InstanceProfile = InstanceProfileFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/add",
        status_code=200,
        json={},
    )
    result = workspace_client.instance_profiles.create(
        instance_profile_arn=expected.instance_profile_arn,
        iam_role_arn=expected.iam_role_arn,
        is_meta_instance_profile=expected.is_meta_instance_profile,
    )
    assert result == expected

    assert mock_request.last_request.json() == {
        "instance_profile_arn": expected.instance_profile_arn,
        "iam_role_arn": expected.iam_role_arn,
        "is_meta_instance_profile": expected.is_meta_instance_profile,
        "skip_validation": False,
    }


def test_update(workspace_client: WorkspaceClient, requests_mock):
    expected: InstanceProfile = InstanceProfileFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/edit",
        status_code=200,
        json={},
    )
    result = workspace_client.instance_profiles.update(
        instance_profile_arn=expected.instance_profile_arn,
        iam_role_arn=expected.iam_role_arn,
        is_meta_instance_profile=expected.is_meta_instance_profile,
    )
    assert result == expected

    assert mock_request.last_request.json() == {
        "instance_profile_arn": expected.instance_profile_arn,
        "iam_role_arn": expected.iam_role_arn,
        "is_meta_instance_profile": expected.is_meta_instance_profile,
    }


def test_delete(workspace_client: WorkspaceClient, requests_mock):
    expected: InstanceProfile = InstanceProfileFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.0/instance-profiles/remove",
        status_code=200,
        json={},
    )
    workspace_client.instance_profiles.delete(expected.instance_profile_arn)

    assert mock_request.last_request.json() == {
        "instance_profile_arn": expected.instance_profile_arn,
    }

import json
import uuid

import pytest
from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.account.aws.client import ACCOUNT_API_PREFIX, ACCOUNT_HOST, AwsAccountClient
from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.resources.account.aws.workspaces import Workspace


class WorkspaceFactory(ModelFactory):
    __model__ = Workspace


def test_get_workspaces_path(aws_account_client: AwsAccountClient):
    assert (
        aws_account_client.workspaces._get_path() == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/workspaces"
    )


def test_get_workspace_id_path(aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    assert (
        aws_account_client.workspaces._get_id_path(i)
        == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/workspaces/{i}"
    )


def test_list_workspaces_empty(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_path()}", status_code=404)
    assert aws_account_client.workspaces.list() == []

    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_path()}",
        status_code=200,
        json=[],
    )
    assert aws_account_client.workspaces.list() == []


def test_list_workspaces(requests_mock, aws_account_client: AwsAccountClient):
    expected = WorkspaceFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.workspaces.list() == [expected]


def test_get_workspaces_by_id(requests_mock, aws_account_client: AwsAccountClient):
    expected: Workspace = WorkspaceFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_id_path(expected.workspace_id)}",
        status_code=200,
        text=expected.json(),
    )

    assert aws_account_client.workspaces.get_by_id(expected.workspace_id) == expected


def test_get_workspaces_by_id_not_found(requests_mock, aws_account_client: AwsAccountClient):
    expected: Workspace = WorkspaceFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_id_path(expected.workspace_id)}",
        status_code=404,
    )

    assert aws_account_client.workspaces.get_by_id(expected.workspace_id) is None


def test_get_workspaces_by_name(requests_mock, aws_account_client: AwsAccountClient):
    expected: Workspace = WorkspaceFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.workspaces.get_by_name(expected.workspace_name) == expected


def test_get_workspaces_by_name_not_found(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_path()}",
        status_code=200,
        json=[],
    )

    assert aws_account_client.workspaces.get_by_name("do_not_exist") is None


def test_create_workspaces(requests_mock, aws_account_client: AwsAccountClient):
    expected: Workspace = WorkspaceFactory.build()
    expected.network_id = uuid.uuid4()
    expected.managed_services_customer_managed_key_id = uuid.uuid4()
    expected.private_access_settings_id = uuid.uuid4()
    expected.storage_customer_managed_key_id = uuid.uuid4()
    mock_request = requests_mock.post(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_path()}",
        status_code=201,
        text=expected.json(),
    )

    result = aws_account_client.workspaces.create(
        workspace_name=expected.workspace_name,
        deployment_name=expected.deployment_name,
        aws_region=expected.aws_region,
        pricing_tier=expected.pricing_tier,
        credentials_id=expected.credentials_id,
        storage_configuration_id=expected.storage_configuration_id,
        network_id=expected.network_id,
        managed_services_customer_managed_key_id=expected.managed_services_customer_managed_key_id,
        private_access_settings_id=expected.private_access_settings_id,
        storage_customer_managed_key_id=expected.storage_customer_managed_key_id,
    )
    assert mock_request.last_request.json() == {
        "deployment_name": expected.deployment_name,
        "aws_region": expected.aws_region,
        "credentials_id": str(expected.credentials_id),
        "pricing_tier": expected.pricing_tier,
        "storage_configuration_id": str(expected.storage_configuration_id),
        "workspace_name": expected.workspace_name,
        "network_id": str(expected.network_id),
        "managed_services_customer_managed_key_id": str(expected.managed_services_customer_managed_key_id),
        "private_access_settings_id": str(expected.private_access_settings_id),
        "storage_customer_managed_key_id": str(expected.storage_customer_managed_key_id),
    }
    assert result == expected


def test_update_workspaces(requests_mock, aws_account_client: AwsAccountClient):
    expected: Workspace = WorkspaceFactory.build()
    expected.network_id = uuid.uuid4()
    expected.managed_services_customer_managed_key_id = uuid.uuid4()
    expected.private_access_settings_id = uuid.uuid4()
    expected.storage_customer_managed_key_id = uuid.uuid4()
    mock_request = requests_mock.patch(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_id_path(expected.workspace_id)}",
        status_code=200,
        text=expected.json(),
    )

    result = aws_account_client.workspaces.update(
        workspace_id=expected.workspace_id,
        aws_region=expected.aws_region,
        credentials_id=expected.credentials_id,
        storage_configuration_id=expected.storage_configuration_id,
        network_id=expected.network_id,
        managed_services_customer_managed_key_id=expected.managed_services_customer_managed_key_id,
        private_access_settings_id=expected.private_access_settings_id,
        storage_customer_managed_key_id=expected.storage_customer_managed_key_id,
    )
    assert mock_request.last_request.json() == {
        "aws_region": expected.aws_region,
        "credentials_id": str(expected.credentials_id),
        "storage_configuration_id": str(expected.storage_configuration_id),
        "network_id": str(expected.network_id),
        "managed_services_customer_managed_key_id": str(expected.managed_services_customer_managed_key_id),
        "private_access_settings_id": str(expected.private_access_settings_id),
        "storage_customer_managed_key_id": str(expected.storage_customer_managed_key_id),
    }
    assert result == expected


def test_delete_workspaces(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_id_path(i)}",
        status_code=200,
    )

    aws_account_client.workspaces.delete(i)
    assert mock_request.called_once


def test_delete_workspaces_not_found(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.workspaces._get_id_path(i)}",
        status_code=404,
        json={"error": "Not found"},
    )

    with pytest.raises(UnknownApiResponse) as e:
        aws_account_client.workspaces.delete(i)
    assert e.value.response.status_code == 404
    assert mock_request.called_once

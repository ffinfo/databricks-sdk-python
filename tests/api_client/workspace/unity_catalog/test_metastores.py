import json
import uuid

import pytest
from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.metastores import Metastore, WorkspaceMetastoreAssignment


class MetastoreFactory(ModelFactory):
    __model__ = Metastore
    workspace_host = "test.cloud.databricks.com"


class WorkspaceMetastoreAssignmentFactory(ModelFactory):
    __model__ = WorkspaceMetastoreAssignment
    workspace_host = "test.cloud.databricks.com"


def test_get_current_assignment_error(workspace_client: WorkspaceClient, requests_mock):
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/current-metastore-assignment",
        status_code=500,
        json={"error": "Boom"},
    )
    with pytest.raises(UnknownApiResponse) as e:
        workspace_client.unity_catalog.metastores.get_current_assignment()
    assert e.value.response.status_code == 500


def test_get_current_assignment(workspace_client: WorkspaceClient, requests_mock):
    mock_request = requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/current-metastore-assignment",
        status_code=404,
        json={"error": "Not found"},
    )
    assignment = workspace_client.unity_catalog.metastores.get_current_assignment()
    assert mock_request.called_once
    assert assignment is None

    expected: WorkspaceMetastoreAssignment = WorkspaceMetastoreAssignmentFactory.build()

    mock_request = requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/current-metastore-assignment",
        status_code=200,
        json={
            "workspace_id": expected.workspace_id,
            "metastore_id": str(expected.metastore_id),
            "default_catalog_name": expected.default_catalog_name,
        },
    )
    assignment = workspace_client.unity_catalog.metastores.get_current_assignment()
    assert mock_request.called_once
    assert assignment == expected


def test_list_empty(workspace_client: WorkspaceClient, requests_mock):
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores",
        status_code=404,
        json={"error": "Not found"},
    )
    metastores = workspace_client.unity_catalog.metastores.list()
    assert metastores == []

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores",
        status_code=200,
        json={"metastores": []},
    )
    metastores = workspace_client.unity_catalog.metastores.list()
    assert metastores == []


def test_list(workspace_client: WorkspaceClient, requests_mock):
    expected: Metastore = MetastoreFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores",
        status_code=200,
        json={"metastores": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    metastores = workspace_client.unity_catalog.metastores.list()
    assert metastores == [expected]


def test_get_by_id(workspace_client: WorkspaceClient, requests_mock):
    expected: Metastore = MetastoreFactory.build()

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores/{expected.metastore_id}",
        status_code=404,
        json={"error": "Not found"},
    )
    metastore = workspace_client.unity_catalog.metastores.get_by_id(expected.metastore_id)
    assert metastore is None

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores/{expected.metastore_id}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    metastore = workspace_client.unity_catalog.metastores.get_by_id(expected.metastore_id)
    assert metastore == expected


def test_get_by_name(workspace_client: WorkspaceClient, requests_mock):
    expected: Metastore = MetastoreFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores",
        status_code=200,
        json={"metastores": []},
    )
    metastore = workspace_client.unity_catalog.metastores.get_by_name(expected.name)
    assert metastore is None

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores",
        status_code=200,
        json={"metastores": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    metastore = workspace_client.unity_catalog.metastores.get_by_name(expected.name)
    assert metastore == expected


def test_create(requests_mock, workspace_client: WorkspaceClient):
    expected: Metastore = MetastoreFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )

    result = workspace_client.unity_catalog.metastores.create(
        name=expected.name,
        storage_root=expected.storage_root,
        region=expected.region,
    )
    assert mock_request.last_request.json() == {
        "name": expected.name,
        "storage_root": expected.storage_root,
        "region": expected.region,
    }
    assert result == expected


def test_update(requests_mock, workspace_client: WorkspaceClient):
    expected: Metastore = MetastoreFactory.build()
    mock_request = requests_mock.patch(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores/{expected.metastore_id}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )

    result = workspace_client.unity_catalog.metastores.update(
        metastore_id=expected.metastore_id,
        name=expected.name,
        delta_sharing_scope=expected.delta_sharing_scope,
        storage_root_credential_id=expected.storage_root_credential_id,
        privilege_model_version=expected.privilege_model_version,
        delta_sharing_recipient_token_lifetime_in_seconds=expected.delta_sharing_recipient_token_lifetime_in_seconds,
        # delta_sharing_organization_name=expected.delta_sharing_organization_name,
        owner=expected.owner,
    )
    assert mock_request.last_request.json() == {
        "name": expected.name,
        "delta_sharing_scope": expected.delta_sharing_scope,
        "storage_root_credential_id": str(expected.storage_root_credential_id),
        "privilege_model_version": expected.privilege_model_version,
        "delta_sharing_recipient_token_lifetime_in_seconds": expected.delta_sharing_recipient_token_lifetime_in_seconds,
        "delta_sharing_organization_name": None,
        "owner": expected.owner,
    }
    assert result == expected


def test_delete(requests_mock, workspace_client: WorkspaceClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/metastores/{i}",
        status_code=200,
    )

    workspace_client.unity_catalog.metastores.delete(i)
    assert mock_request.called_once


def test_create_assignment(requests_mock, workspace_client: WorkspaceClient):
    expected: WorkspaceMetastoreAssignment = WorkspaceMetastoreAssignmentFactory.build()
    mock_request = requests_mock.put(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/workspaces/{expected.workspace_id}/metastore",
        status_code=200,
        json={
            "metastore_id": str(expected.metastore_id),
            "default_catalog_name": expected.default_catalog_name,
        },
    )
    assignment = workspace_client.unity_catalog.metastores.create_assignment(
        workspace_id=expected.workspace_id,
        metastore_id=expected.metastore_id,
        default_catalog_name=expected.default_catalog_name,
    )
    assert assignment == expected

    assert mock_request.last_request.json() == {
        "metastore_id": str(expected.metastore_id),
        "default_catalog_name": expected.default_catalog_name,
    }


def test_update_assignment(requests_mock, workspace_client: WorkspaceClient):
    expected: WorkspaceMetastoreAssignment = WorkspaceMetastoreAssignmentFactory.build()
    mock_request = requests_mock.patch(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/workspaces/{expected.workspace_id}/metastore",
        status_code=200,
        json={
            "metastore_id": str(expected.metastore_id),
            "default_catalog_name": expected.default_catalog_name,
        },
    )
    assignment = workspace_client.unity_catalog.metastores.update_assignment(
        workspace_id=expected.workspace_id,
        metastore_id=expected.metastore_id,
        default_catalog_name=expected.default_catalog_name,
    )
    assert assignment == expected

    assert mock_request.last_request.json() == {
        "metastore_id": str(expected.metastore_id),
        "default_catalog_name": expected.default_catalog_name,
    }


def test_delete_assignment(requests_mock, workspace_client: WorkspaceClient):
    expected: WorkspaceMetastoreAssignment = WorkspaceMetastoreAssignmentFactory.build()
    mock_request = requests_mock.delete(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/workspaces/{expected.workspace_id}/metastore",
        status_code=200,
    )

    workspace_client.unity_catalog.metastores.delete_assignment(
        workspace_id=expected.workspace_id, metastore_id=expected.metastore_id
    )
    assert mock_request.called_once

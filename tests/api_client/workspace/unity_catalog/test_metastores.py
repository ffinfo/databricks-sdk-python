import pytest
from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.metastores import WorkspaceMetastoreAssignment


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

import json

from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.schemas import Schema


class SchemaFactory(ModelFactory):
    __model__ = Schema
    workspace_host = "test.cloud.databricks.com"


def test_list_empty(workspace_client: WorkspaceClient, requests_mock):
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas",
        status_code=404,
        json={"error": "Not found"},
    )
    schemas = workspace_client.unity_catalog.schemas.list(catalog_name="test")
    assert schemas == []

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas",
        status_code=200,
        json={"schemas": []},
    )
    schemas = workspace_client.unity_catalog.schemas.list(catalog_name="test")
    assert schemas == []


def test_list(workspace_client: WorkspaceClient, requests_mock):
    expected: Schema = SchemaFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas",
        status_code=200,
        json={"schemas": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    schemas = workspace_client.unity_catalog.schemas.list(catalog_name=expected.catalog_name)
    assert schemas == [expected]


def test_get_by_name(workspace_client: WorkspaceClient, requests_mock):
    expected: Schema = SchemaFactory.build()

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas/{expected.catalog_name}.{expected.name}",
        status_code=404,
        json={"error": "Not found"},
    )
    schema = workspace_client.unity_catalog.schemas.get_by_name(
        catalog_name=expected.catalog_name, schema_name=expected.name
    )
    assert schema is None

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas/{expected.catalog_name}.{expected.name}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    schema = workspace_client.unity_catalog.schemas.get_by_name(
        catalog_name=expected.catalog_name, schema_name=expected.name
    )
    assert schema == expected


def test_create(requests_mock, workspace_client: WorkspaceClient):
    expected: Schema = SchemaFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )

    result = workspace_client.unity_catalog.schemas.create(
        catalog_name=expected.catalog_name,
        schema_name=expected.name,
        storage_root=expected.storage_root,
        comment=expected.comment,
        properties=expected.properties,
    )
    assert mock_request.last_request.json() == {
        "catalog_name": expected.catalog_name,
        "name": expected.name,
        "storage_root": expected.storage_root,
        "comment": expected.comment,
        "properties": expected.properties,
    }
    assert result == expected


def test_update(requests_mock, workspace_client: WorkspaceClient):
    expected: Schema = SchemaFactory.build()
    mock_request = requests_mock.patch(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas/{expected.catalog_name}.{expected.name}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )

    result = workspace_client.unity_catalog.schemas.update(
        catalog_name=expected.catalog_name,
        schema_name=expected.name,
        new_name=expected.name,
        comment=expected.comment,
        properties=expected.properties,
        owner=expected.owner,
    )
    assert mock_request.last_request.json() == {
        "name": expected.name,
        "comment": expected.comment,
        "properties": expected.properties,
        "owner": expected.owner,
    }
    assert result == expected


def test_delete(requests_mock, workspace_client: WorkspaceClient):
    expected: Schema = SchemaFactory.build()
    mock_request = requests_mock.delete(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/schemas/{expected.catalog_name}.{expected.name}",
        status_code=200,
    )

    workspace_client.unity_catalog.schemas.delete(catalog_name=expected.catalog_name, schema_name=expected.name)
    assert mock_request.called_once

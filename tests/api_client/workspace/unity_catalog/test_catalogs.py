import json
import uuid

from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.unity_catalog.catalogs import Catalog


class CatalogFactory(ModelFactory):
    __model__ = Catalog
    workspace_host = "test.cloud.databricks.com"


def test_list_empty(workspace_client: WorkspaceClient, requests_mock):
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs",
        status_code=404,
        json={"error": "Not found"},
    )
    catalogs = workspace_client.unity_catalog.catalogs.list()
    assert catalogs == []

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs",
        status_code=200,
        json={"catalogs": []},
    )
    catalogs = workspace_client.unity_catalog.catalogs.list()
    assert catalogs == []


def test_list(workspace_client: WorkspaceClient, requests_mock):
    expected: Catalog = CatalogFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs",
        status_code=200,
        json={"catalogs": [json.loads(expected.json(exclude={"workspace_host"}))]},
    )
    catalogs = workspace_client.unity_catalog.catalogs.list()
    assert catalogs == [expected]


def test_get_by_name(workspace_client: WorkspaceClient, requests_mock):
    expected: Catalog = CatalogFactory.build()

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs/{expected.name}",
        status_code=404,
        json={"error": "Not found"},
    )
    catalog = workspace_client.unity_catalog.catalogs.get_by_name(expected.name)
    assert catalog is None

    requests_mock.get(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs/{expected.name}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    catalog = workspace_client.unity_catalog.catalogs.get_by_name(expected.name)
    assert catalog == expected


def test_create(requests_mock, workspace_client: WorkspaceClient):
    expected: Catalog = CatalogFactory.build()
    mock_request = requests_mock.post(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )

    result = workspace_client.unity_catalog.catalogs.create(
        name=expected.name,
        storage_root=expected.storage_root,
        comment=expected.comment,
        properties=expected.properties,
        provider_name=expected.provider_name,
        share_name=expected.share_name,
    )
    assert mock_request.last_request.json() == {
        "name": expected.name,
        "storage_root": expected.storage_root,
        "comment": expected.comment,
        "properties": expected.properties,
        "provider_name": expected.provider_name,
        "share_name": expected.share_name,
    }
    assert result == expected


def test_update(requests_mock, workspace_client: WorkspaceClient):
    expected: Catalog = CatalogFactory.build()
    mock_request = requests_mock.patch(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs/{expected.name}",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )

    result = workspace_client.unity_catalog.catalogs.update(
        name=expected.name,
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
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{workspace_client.host}/api/2.1/unity-catalog/catalogs/{i}",
        status_code=200,
    )

    workspace_client.unity_catalog.catalogs.delete(i)
    assert mock_request.called_once

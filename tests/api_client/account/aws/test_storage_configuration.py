import json
import uuid

import pytest
from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.account.aws.client import ACCOUNT_API_PREFIX, ACCOUNT_HOST, AwsAccountClient
from databricks_sdk_python.resources.aws_account.storage_config import StorageConfiguration


class StorageConfigurationFactory(ModelFactory):
    __model__ = StorageConfiguration


def test_get_account_path(aws_account_client: AwsAccountClient):
    assert aws_account_client._get_account_path() == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}"


def test_get_storage_configuration_path(aws_account_client: AwsAccountClient):
    assert (
        aws_account_client.storage_configuration._get_path()
        == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/storage-configurations"
    )


def test_get_storage_configuration_id_path(aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    assert (
        aws_account_client.storage_configuration._get_id_path(i)
        == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/storage-configurations/{i}"
    )


def test_list_storage_configuration_empty(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_path()}", status_code=404)
    assert aws_account_client.storage_configuration.list() == []

    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_path()}",
        status_code=200,
        json=[],
    )
    assert aws_account_client.storage_configuration.list() == []


def test_list_storage_configuration(requests_mock, aws_account_client: AwsAccountClient):
    expected = StorageConfigurationFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.storage_configuration.list() == [expected]


def test_get_storage_configuration_by_id(requests_mock, aws_account_client: AwsAccountClient):
    expected: StorageConfiguration = StorageConfigurationFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_id_path(expected.storage_configuration_id)}",
        status_code=200,
        text=expected.json(),
    )

    assert aws_account_client.storage_configuration.get_by_id(expected.storage_configuration_id) == expected


def test_get_storage_configuration_by_id_not_found(requests_mock, aws_account_client: AwsAccountClient):
    expected: StorageConfiguration = StorageConfigurationFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_id_path(expected.storage_configuration_id)}",
        status_code=404,
    )

    assert aws_account_client.storage_configuration.get_by_id(expected.storage_configuration_id) is None


def test_get_storage_configuration_by_name(requests_mock, aws_account_client: AwsAccountClient):
    expected: StorageConfiguration = StorageConfigurationFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.storage_configuration.get_by_name(expected.storage_configuration_name) == expected


def test_get_storage_configuration_by_name_not_found(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_path()}",
        status_code=200,
        json=[],
    )

    assert aws_account_client.storage_configuration.get_by_name("do_not_exist") is None


def test_create_storage_configuration(requests_mock, aws_account_client: AwsAccountClient):
    expected: StorageConfiguration = StorageConfigurationFactory.build()
    mock_request = requests_mock.post(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_path()}",
        status_code=200,
        text=expected.json(),
    )

    result = aws_account_client.storage_configuration.create(
        storage_configuration_name=expected.storage_configuration_name,
        bucket_name=expected.root_bucket_info.bucket_name,
    )
    assert mock_request.last_request.json() == {
        "storage_configuration_name": expected.storage_configuration_name,
        "root_bucket_info": {"bucket_name": expected.root_bucket_info.bucket_name},
    }
    assert result == expected


def test_delete_storage_configuration(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_id_path(i)}",
        status_code=200,
    )

    aws_account_client.storage_configuration.delete(i)
    assert mock_request.called_once


def test_delete_storage_configuration_not_found(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.storage_configuration._get_id_path(i)}",
        status_code=404,
    )

    with pytest.raises(RuntimeError) as e:
        aws_account_client.storage_configuration.delete(i)
    assert e.value.args[0] == "Not found"
    assert mock_request.called_once

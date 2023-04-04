import json
import uuid

import pytest
from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.account.aws.client import ACCOUNT_API_PREFIX, ACCOUNT_HOST, AwsAccountClient
from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.resources.account.aws.networks import Network


class NetworksFactory(ModelFactory):
    __model__ = Network


def test_get_account_path(aws_account_client: AwsAccountClient):
    assert aws_account_client._get_account_path() == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}"


def test_get_networks_path(aws_account_client: AwsAccountClient):
    assert aws_account_client.networks._get_path() == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/networks"


def test_get_networks_id_path(aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    assert (
        aws_account_client.networks._get_id_path(i)
        == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/networks/{i}"
    )


def test_list_networks_empty(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_path()}", status_code=404)
    assert aws_account_client.networks.list() == []

    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_path()}",
        status_code=200,
        json=[],
    )
    assert aws_account_client.networks.list() == []


def test_list_networks(requests_mock, aws_account_client: AwsAccountClient):
    expected = NetworksFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.networks.list() == [expected]


def test_get_networks_by_id(requests_mock, aws_account_client: AwsAccountClient):
    expected: Network = NetworksFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_id_path(expected.network_id)}",
        status_code=200,
        text=expected.json(),
    )

    assert aws_account_client.networks.get_by_id(expected.network_id) == expected


def test_get_networks_by_id_not_found(requests_mock, aws_account_client: AwsAccountClient):
    expected: Network = NetworksFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_id_path(expected.network_id)}",
        status_code=404,
    )

    assert aws_account_client.networks.get_by_id(expected.network_id) is None


def test_get_networks_by_name(requests_mock, aws_account_client: AwsAccountClient):
    expected: Network = NetworksFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.networks.get_by_name(expected.network_name) == expected


def test_get_networks_by_name_not_found(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_path()}",
        status_code=200,
        json=[],
    )

    assert aws_account_client.networks.get_by_name("do_not_exist") is None


def test_create_networks(requests_mock, aws_account_client: AwsAccountClient):
    expected: Network = NetworksFactory.build()
    mock_request = requests_mock.post(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_path()}",
        status_code=201,
        text=expected.json(),
    )

    result = aws_account_client.networks.create(
        network_name=expected.network_name,
        vpc_id=expected.vpc_id,
        subnet_ids=expected.subnet_ids,
        security_group_ids=expected.security_group_ids,
        vpc_endpoints=expected.vpc_endpoints,
    )

    expected_body = {
        "network_name": expected.network_name,
        "vpc_id": expected.vpc_id,
        "subnet_ids": expected.subnet_ids,
        "security_group_ids": expected.security_group_ids,
    }
    if expected.vpc_endpoints is not None:
        expected_body["vpc_endpoints"] = json.loads(expected.vpc_endpoints.json())

    assert mock_request.last_request.json() == expected_body
    assert result == expected


def test_delete_networks(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_id_path(i)}",
        status_code=200,
    )

    aws_account_client.networks.delete(i)
    assert mock_request.called_once


def test_delete_networks_not_found(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.networks._get_id_path(i)}",
        status_code=404,
        json={"error": "Not found"},
    )

    with pytest.raises(UnknownApiResponse) as e:
        aws_account_client.networks.delete(i)
    assert e.value.response.status_code == 404
    assert mock_request.called_once

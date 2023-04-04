import json
import uuid

import pytest
from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.account.aws.client import ACCOUNT_API_PREFIX, ACCOUNT_HOST, AwsAccountClient
from databricks_sdk_python.api_client.utils import UnknownApiResponse
from databricks_sdk_python.resources.account.aws.credentials import Credentials


class CredentialsFactory(ModelFactory):
    __model__ = Credentials


def test_get_credentials_path(aws_account_client: AwsAccountClient):
    assert (
        aws_account_client.credentials._get_path()
        == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/credentials"
    )


def test_get_credentials_id_path(aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    assert (
        aws_account_client.credentials._get_id_path(i)
        == f"{ACCOUNT_API_PREFIX}/{aws_account_client.account_id}/credentials/{i}"
    )


def test_list_credentials_empty(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_path()}", status_code=404)
    assert aws_account_client.credentials.list() == []

    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_path()}",
        status_code=200,
        json=[],
    )
    assert aws_account_client.credentials.list() == []


def test_list_credentials(requests_mock, aws_account_client: AwsAccountClient):
    expected = CredentialsFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.credentials.list() == [expected]


def test_get_credentials_by_id(requests_mock, aws_account_client: AwsAccountClient):
    expected: Credentials = CredentialsFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_id_path(expected.credentials_id)}",
        status_code=200,
        text=expected.json(),
    )

    assert aws_account_client.credentials.get_by_id(expected.credentials_id) == expected


def test_get_credentials_by_id_not_found(requests_mock, aws_account_client: AwsAccountClient):
    expected: Credentials = CredentialsFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_id_path(expected.credentials_id)}",
        status_code=404,
    )

    assert aws_account_client.credentials.get_by_id(expected.credentials_id) is None


def test_get_credentials_by_name(requests_mock, aws_account_client: AwsAccountClient):
    expected: Credentials = CredentialsFactory.build()
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_path()}",
        status_code=200,
        json=[json.loads(expected.json())],
    )

    assert aws_account_client.credentials.get_by_name(expected.credentials_name) == expected


def test_get_credentials_by_name_not_found(requests_mock, aws_account_client: AwsAccountClient):
    requests_mock.get(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_path()}",
        status_code=200,
        json=[],
    )

    assert aws_account_client.credentials.get_by_name("do_not_exist") is None


def test_create_credentials(requests_mock, aws_account_client: AwsAccountClient):
    expected: Credentials = CredentialsFactory.build()
    mock_request = requests_mock.post(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_path()}",
        status_code=201,
        text=expected.json(),
    )

    result = aws_account_client.credentials.create(
        credentials_name=expected.credentials_name, role_arn=expected.aws_credentials.sts_role.role_arn
    )
    assert mock_request.last_request.json() == {
        "credentials_name": expected.credentials_name,
        "aws_credentials": {"sts_role": {"role_arn": expected.aws_credentials.sts_role.role_arn}},
    }
    assert result == expected


def test_delete_credentials(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_id_path(i)}",
        status_code=200,
    )

    aws_account_client.credentials.delete(i)
    assert mock_request.called_once


def test_delete_credentials_not_found(requests_mock, aws_account_client: AwsAccountClient):
    i = uuid.uuid4()
    mock_request = requests_mock.delete(
        f"https://{ACCOUNT_HOST}/{aws_account_client.credentials._get_id_path(i)}",
        status_code=404,
        json={"error": "Not found"},
    )

    with pytest.raises(UnknownApiResponse) as e:
        aws_account_client.credentials.delete(i)
    assert e.value.response.status_code == 404
    assert mock_request.called_once

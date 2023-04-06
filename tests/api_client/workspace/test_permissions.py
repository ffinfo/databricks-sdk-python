from pydantic_factories import ModelFactory

from databricks_sdk_python.api_client.workspace.client import WorkspaceClient
from databricks_sdk_python.resources.workspace.permissions import PermissionLevels, Permissions


class PermissionsFactory(ModelFactory):
    __model__ = Permissions
    workspace_host = "test.cloud.databricks.com"


class PermissionLevelsFactory(ModelFactory):
    __model__ = PermissionLevels
    workspace_host = "test.cloud.databricks.com"


def test_get(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/t/i",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get("t", "i")
    assert permissions == expected


def test_grant(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.patch(
        f"https://{workspace_client.host}/api/2.0/permissions/t/i",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.grant("t", "i", [])
    assert permissions == expected


def test_replace(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.put(
        f"https://{workspace_client.host}/api/2.0/permissions/t/i",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.replace("t", "i", [])
    assert permissions == expected


def test_get_levels(workspace_client: WorkspaceClient, requests_mock):
    expected: PermissionLevels = PermissionLevelsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/t/i/permissionLevels",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_permission_levels("t", "i")
    assert permissions == expected


def test_get_token_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/authorization/tokens",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_token_permissions()
    assert permissions == expected


def test_get_password_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/authorization/passwords",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_password_permissions()
    assert permissions == expected


def test_get_cluster_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/clusters/id",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_cluster_permissions("id")
    assert permissions == expected


def test_get_cluster_policies_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/cluster-policies/id",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_cluster_policy_permissions("id")
    assert permissions == expected


def test_get_instance_pool_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/instance-pools/id",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_instance_pool_permissions("id")
    assert permissions == expected


def test_get_job_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/jobs/id",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_job_permissions("id")
    assert permissions == expected


def test_get_pipeline_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/pipelines/id",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_pipeline_permissions("id")
    assert permissions == expected


def test_get_notebook_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/notebooks/1",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_notebook_permissions(1)
    assert permissions == expected


def test_get_directory_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/directories/1",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_directory_permissions(1)
    assert permissions == expected


def test_get_experiment_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/experiments/1",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_experiment_permissions(1)
    assert permissions == expected


def test_get_registered_model_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/registered-models/1",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_registered_model_permissions(1)
    assert permissions == expected


def test_get_sql_warehouses_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/sql/warehouses/1",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_sql_warehouses_permissions(1)
    assert permissions == expected


def test_get_repo_permissions(workspace_client: WorkspaceClient, requests_mock):
    expected: Permissions = PermissionsFactory.build()
    requests_mock.get(
        f"https://{workspace_client.host}/api/2.0/permissions/repos/1",
        status_code=200,
        text=expected.json(exclude={"workspace_host"}),
    )
    permissions = workspace_client.permissions.get_repo_permissions(1)
    assert permissions == expected

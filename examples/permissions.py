from databricks_sdk_python.api_client.workspace.client import get_workspace_client
from databricks_sdk_python.resources.workspace.permissions import GroupObjectPermission

workspace_client = get_workspace_client(workspace_host="<hostname of workspace>")

cluster_permissions = workspace_client.permissions.get_cluster_permissions("<cluster id>")

# Adds permissions
cluster_permissions.grant([GroupObjectPermission(group_name="<group name>", permission_level="CAN_RESTART")])

# Replacing permissions
cluster_permissions.replace(
    [
        GroupObjectPermission(group_name="admins", permission_level="CAN_MANAGE"),
    ]
)

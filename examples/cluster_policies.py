from databricks_sdk_python.api_client.workspace.client import get_workspace_client

client = get_workspace_client(workspace_host="<hostname of workspace>")

cluster_policies = client.cluster_policies.list()

cluster_policy = client.cluster_policies.create(
    policy_name="<name of policy>",
    definition={"cluster_type": {"type": "fixed", "value": "all-purpose"}},
    description="<Some fancy description>",
)

cluster_policy = client.cluster_policies.get_by_id("<id of policy>")
cluster_policy = client.cluster_policies.get_by_name("<name of policy>")

# Fetching current permissions
permissions = cluster_policy.get_permissions()

# grant user, group or server principal to be able to use the policy
cluster_policy.grant_use(user_name="<user_name>")
cluster_policy.grant_use(group_name="<group_name>")
cluster_policy.grant_use(service_principal_name="<service_principal_name>")

# Updates policy
cluster_policy.update(definition={"cluster_type": {"type": "fixed", "value": "job"}})

# Deletes policy
cluster_policy.delete()

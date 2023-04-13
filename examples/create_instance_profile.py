from databricks_sdk_python.api_client.workspace.client import get_workspace_client

workspace_client = get_workspace_client(workspace_host="<hostname of workspace>")

instance_profiles = workspace_client.instance_profiles.list()
instance_profile = workspace_client.instance_profiles.get("<arn or name>")

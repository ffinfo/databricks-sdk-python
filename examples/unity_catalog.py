from databricks_sdk_python.api_client.workspace.client import get_workspace_client

workspace_client = get_workspace_client(workspace_host="<hostname of workspace>")

metastores = workspace_client.unity_catalog.metastores.list()
current_metastore = workspace_client.unity_catalog.metastores.get_current_assignment()

catalogs = workspace_client.unity_catalog.catalogs.list()
catalog = workspace_client.unity_catalog.catalogs.get_by_name("<name of catalog>")

schemas = catalog.list_schemas()
schema = catalog.get_schema("<schema_name>")

# TODO: tables

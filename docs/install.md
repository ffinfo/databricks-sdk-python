# Setup

## Install

### pip

```shell
pip install databricks-sdk-python
```

### poetry
```shell
poetry add databricks-sdk-python
```

## Authentication

There are 2 options for Authentication.

If a token is used make the username: `token`. See also [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html)

### netrc

Put the following to the `~/.netrc` file:
```
# Aws account portal
machine accounts.cloud.databricks.com
login <user_name>
password <password>

# Workspace
machine <workspace host name>
login <user_name>
password <password> # or token
```

### in code

```python
from requests.auth import HTTPBasicAuth
from uuid import UUID

from databricks_sdk_python.api_client.account.aws.client import get_aws_account_client
from databricks_sdk_python.api_client.workspace.client import get_workspace_client

account_id = UUID("<databricks account id>")

# Get user / password from a secret manager
auth = HTTPBasicAuth(username="<user_name>", password="<password>")

aws_account_client = get_aws_account_client(account_id=account_id, auth=auth)
workspace_client = get_workspace_client(workspace_host="<hostname of workspace>", auth=auth)
```

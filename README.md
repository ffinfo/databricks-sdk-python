[![PyPI version](https://badge.fury.io/py/databricks-sdk-python.svg)](https://badge.fury.io/py/databricks-sdk-python)
[![codecov](https://codecov.io/github/ffinfo/databricks-sdk-python/branch/main/graph/badge.svg?token=EOJDSTI5KN)](https://codecov.io/github/ffinfo/databricks-sdk-python)
[![Python package](https://github.com/ffinfo/databricks-sdk-python/actions/workflows/python-package.yml/badge.svg)](https://github.com/ffinfo/databricks-sdk-python/actions/workflows/python-package.yml)

# databricks-sdk-python

### Install

```bash
pip install databricks-sdk-python
```


### Install for developers

###### Install package

- Requirement: Poetry 1.*

```shell
poetry install
```

###### Run unit tests
```shell
pytest
coverage run -m pytest  # with coverage
# or (depends on your local env) 
poetry run pytest
poetry run coverage run -m pytest  # with coverage
```

##### Run linting

The linting is checked in the github workflow. To fix and review issues run this:
```shell
black .   # Auto fix all issues
isort .   # Auto fix all issues
pflake .  # Only display issues, fixing is manual
```

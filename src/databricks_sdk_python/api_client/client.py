import logging
import ssl
from typing import Optional

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase, HTTPBasicAuth
from requests.utils import get_netrc_auth

try:
    from requests.packages.urllib3.poolmanager import PoolManager
    from requests.packages.urllib3.util.retry import Retry
except ImportError:
    from urllib3.poolmanager import PoolManager
    from urllib3.util.retry import Retry


logger = logging.getLogger("databricks-sdk-python")


class TlsV1HttpAdapter(HTTPAdapter):
    """
    A HTTP adapter implementation that specifies the ssl version to be TLS1.
    This avoids problems with openssl versions that
    use SSL3 as a default (which is not supported by the server side).
    """

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block, ssl_version=ssl.PROTOCOL_TLSv1_2
        )


class BaseClient(object):
    def __init__(self, host: str, auth: Optional[AuthBase] = None):
        self.host = host
        self.auth = auth
        if self.auth is None:
            net_rc = get_netrc_auth(f"https://{host}")
            if net_rc is not None:
                self.auth = HTTPBasicAuth(*net_rc)
            else:
                raise AttributeError("No databricks credentials found, please supply with netrc of give a Auth class")

        self.session = requests.Session()
        self.session.auth = self.auth
        retries = Retry(
            total=6,
            backoff_factor=1,
            status_forcelist=[429],
            method_whitelist={"POST"} | set(Retry.DEFAULT_METHOD_WHITELIST),
            respect_retry_after_header=True,
            raise_on_status=False,  # return original response when retries have been exhausted
        )
        self.session.mount("https://", TlsV1HttpAdapter(max_retries=retries))

    def _get_url(self, path: str):
        return f"https://{self.host}/{path}"

    def _request(self, method, path: str, params: Optional[dict] = None, body: Optional[dict] = None) -> Response:
        response = self.session.request(method, url=self._get_url(path), params=params, json=body, auth=self.auth)
        if response.status_code == 400 or response.status_code == 401 or response.status_code >= 500:
            logger.error(response.text)
            response.raise_for_status()
        return response

    def _get(self, path: str, params: Optional[dict] = None, body: Optional[dict] = None) -> Response:
        return self._request("GET", path, params=params, body=body)

    def _post(self, path: str, params: Optional[dict] = None, body: Optional[dict] = None) -> Response:
        return self._request("POST", path, params=params, body=body)

    def _delete(self, path: str, params: Optional[dict] = None, body: Optional[dict] = None) -> Response:
        return self._request("DELETE", path, params=params, body=body)

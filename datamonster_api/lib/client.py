import datetime
import hashlib
import hmac
import requests

from .errors import DataMonsterError


class Client(object):
    """Low level client for interacting with the DataMonster server"""

    server = "https://dm.adaptivemgmt.com"

    def __init__(self, key_id, secret, server=None, verify=True):
        self.key_id = key_id
        self.secret = secret
        if server:
            self.server = server

        self.verify = verify

    def compute_hash_string(self, method, path, date_str, secret_str):
        msg_to_hash = "\n".join([method, path, date_str])
        secret_binary = bytes(bytearray.fromhex(secret_str))

        return hmac.new(
            secret_binary, msg_to_hash.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def _get_session(self, method, path, headers=None):
        """
        :param path: (six.text_type) url path
        :param headers: (dict or None) Additional optional header items

        :return: the Response, appropriately formatted/deserialized
        :raises: DataMonsterError, if requests `get` returns with status_code != 200,
            or if content type of response is neither json nor avro
        """
        # so that `headers` doesn't have mutable default value {}
        headers = headers or {}

        date = datetime.datetime.utcnow()
        date_str = date.strftime("%a, %d %b %Y %H:%M:%S") + " +0000"

        # Probably should fix this on the server to keep the get params in the hash
        hash_path = path.split("?")[0]
        try:
            hash_str = self.compute_hash_string(
                method, hash_path, date_str, self.secret
            )
        except ValueError:
            raise ValueError("Bad key provided")
        session = requests.Session()
        session.headers["Date"] = date_str
        session.headers["Authorization"] = "DM {}:{}".format(self.key_id, hash_str)
        session.headers["Accept"] = "application/json"
        session.headers.update(headers)

        retry = requests.packages.urllib3.util.retry.Retry(
            total=3,
            backoff_factor=5,
            status_forcelist=(500, 502, 504),
            method_whitelist=frozenset(["GET", "POST"])
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _format_response(self, response):
        """Format the response from a rest call"""

        if response.status_code != 200:
            raise DataMonsterError(response.reason, response.content)

        if response.headers["Content-Type"] == "application/json":
            return response.json()
        elif response.headers["Content-Type"] == "avro/binary":
            return response
        else:
            raise DataMonsterError(
                "Unexpected content type: {}".format(response.headers["Content-Type"])
            )

    def get(self, path, headers=None, stream=False):
        """
        :param path: (six.text_type) url path
        :param headers: (dict or None) Additional optional header items

        :return: the Response, appropriately formatted/deserialized
        :raises: DataMonsterError, if requests `get` returns with status_code != 200,
            or if content type of response is neither json nor avro
        """

        session = self._get_session("GET", path, headers)

        url = "{}{}".format(self.server, path)
        response = session.get(url, verify=self.verify, stream=stream)

        return self._format_response(response)

    def post(self, path, json, headers=None, stream=False):
        """
        :param path: (six.text_type) url path
        :param data: (dict) post data
        :param headers: (dict or None) Additional optional header items

        :return: the Response, appropriately formatted/deserialized
        :raises: DataMonsterError, if requests `get` returns with status_code != 200,
            or if content type of response is neither json nor avro
        """

        session = self._get_session("POST", path, headers)

        url = "{}{}".format(self.server, path)
        response = session.post(url, json=json, verify=self.verify, stream=stream)

        return self._format_response(response)

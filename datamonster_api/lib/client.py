import datetime
import hmac
import hashlib
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

    def get(self, path, headers=None, stream=False):
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
        method = "GET"

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

        url = "{}{}".format(self.server, path)
        resp = session.get(url, verify=self.verify, stream=stream)

        if resp.status_code != 200:
            raise DataMonsterError(resp.content)

        if resp.headers["Content-Type"] == "application/json":
            return resp.json()
        elif resp.headers["Content-Type"] == "avro/binary":
            return resp
        else:
            raise DataMonsterError(
                "Unexpected content type: {}".format(resp.headers["Content-Type"])
            )

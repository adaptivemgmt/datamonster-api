import datetime
import hmac
import hashlib
import requests

from .errors import DataMonsterError


class Client(object):
    """Low level client for interacting with the server"""

    # domain = 'https://staging.adaptivemgmt.com'
    domain = 'http://127.0.0.1:5000'

    def __init__(self, key_id, secret, domain=None):
        self.key_id = key_id
        self.secret = secret
        if domain:
            self.domain = domain

    def compute_hash_string(self, method, path, date_str, secret_str):
        msg_to_hash = '\n'.join([method, path, date_str])
        secret_binary = bytes(bytearray.fromhex(secret_str))
        return hmac.new(
            secret_binary,
            msg_to_hash.encode('utf-8'),
            hashlib.sha256
        ).digest().encode('hex')

    def get(self, path, headers={}):
        date = datetime.datetime.utcnow()
        date_str = date.strftime('%a, %d %b %Y %H:%M:%S') + ' +0000'
        method = 'GET'

        # Probably should fix this on the server to keep the get params in the hash
        hash_path = path.split('?')[0]
        hash_str = self.compute_hash_string(method, hash_path, date_str, self.secret)

        session = requests.Session()
        session.headers['Date'] = date_str
        session.headers['Authorization'] = 'DM {}:{}'.format(self.key_id, hash_str)
        session.headers['Accept'] = 'application/json'
        session.headers.update(headers)

        url = '{}{}'.format(self.domain, path)
        resp = session.get(url)

        if resp.status_code != 200:
            raise DataMonsterError(resp.content)

        if resp.headers['Content-Type'] == 'application/json':
            return resp.json()
        elif resp.headers['Content-Type'] == 'avro/binary':
            return resp.content
        else:
            raise DataMonsterError(
                'Unexpected content type: {}'.format(resp.headers['Content-Type'])
            )

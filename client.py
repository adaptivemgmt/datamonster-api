import datetime
import hmac
import hashlib
import requests


def compute_hash_string(method, path, date_str, secret_str):
    msg_to_hash = '\n'.join([method, path, date_str])
    secret_binary = bytes(bytearray.fromhex(secret_str))
    return hmac.new(
        secret_binary,
        msg_to_hash.encode('utf-8'),
        hashlib.sha256
    ).digest().encode('hex')


kevin_key_id = '9e49323c3590b0db58647a080408d3c3'
kevin_secret = '3be6c1af983d0e0caea9ea98dead4a99'


# adefelice@firtree.com
alex_key_id = 'c70150608f61a04150632ad5d4f90ce7'
alex_secret = 'ce6fb6da982eea02dcd3c75458fb7e91'


# karen menezes
# DM_API_KEY_ID = '43bdd2ae9a28e3264c14179f1e1cada8'
# DM_API_SECRET = '47d3d9069b4eec60ced98a4890a0e4cc'


def send_request(key_id, secret, path, params):
    date = datetime.datetime.utcnow()
    date_str = date.strftime('%a, %d %b %Y %H:%M:%S') + ' +0000'
    method = 'GET'
    hash_str = compute_hash_string(method, path, date_str, secret)

    session = requests.Session()
    session.headers['Date'] = date_str
    session.headers['Authorization'] = 'DM {}:{}'.format(key_id, hash_str)
    session.headers['Accept'] = 'application/json'

    url = 'https://staging.adaptivemgmt.com{}'.format(path)
    return session.get(url, params=params, verify=False)


def all_companies():
    params = {
    }
    return send_request(alex_key_id, alex_secret, '/rest/company', params)


def company_by_ticker():
    params = {
        'q': 'aapl',
    }
    return send_request(alex_key_id, alex_secret, '/rest/company', params)


def company_by_datasource():
    params = {
        'datasource': 'c5fbbf0d-a630-4523-952f-cf8709b6f540',
    }
    return send_request(kevin_key_id, kevin_secret, '/rest/company', params)


def all_datasources():
    params = {
    }
    return send_request(alex_key_id, alex_secret, '/rest/datasource', params)


def datasources_by_company():
    params = {
        'company': 971,
    }
    return send_request(alex_key_id, alex_secret, '/rest/datasource', params)


def data_with_date_range():
    params = {
        'companyId': 730,
        'startDate': '2018-01-05',
        'endDate': '2018-01-10',
    }
    return send_request(alex_key_id, alex_secret, '/rest/datasource/c5fbbf0d-a630-4523-952f-cf8709b6f540/data', params)


def data_fiscal_quarter_aggregated():
    params = {
        'companyId': 205,
        'startDate': '2018-01-05',
        'endDate': '2018-07-31',
        'aggregation': 'fiscalQuarter',
    }
    return send_request(kevin_key_id, kevin_secret, '/rest/datasource/c5fbbf0d-a630-4523-952f-cf8709b6f540/data', params)

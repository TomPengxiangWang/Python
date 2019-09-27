import json
import logging
import os
import traceback
import urllib
import urlparse

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials
from botocore.httpsession import URLLib3Session
from botocore.session import Session

# The following parameters are required to configure the ES cluster, value comes from environment variable
ES_ENDPOINT = os.environ['es_endpoint']
INDEX_NAME = os.environ['index_name']

# The following parameters can be optionally customized
DOC_TABLE_FORMAT = '{}'  # Python formatter to generate index name from the DynamoDB table name
DOC_TYPE_FORMAT = '{}_type'  # Python formatter to generate type name from the DynamoDB table name, default is to add '_type' suffix
ES_REGION = None  # If not set, use the runtime lambda region
ES_MAX_RETRIES = 3  # Max number of retries for exponential backoff
DEBUG = True  # Set verbose debugging information

print "Streaming to ElasticSearch"
logger = logging.getLogger()
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

class ES_Exception(Exception):
    '''Exception capturing status_code from Client Request'''
    status_code = 0
    payload = ''

    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload
        Exception.__init__(self, 'ES_Exception: status_code={}, payload={}'.format(status_code, payload))


# Low-level POST data to Amazon Elasticsearch Service generating a Sigv4 signed request
def put_data_to_es(payload, path, method='PUT', proto='https://'):
    es_url = urlparse.urlparse(ES_ENDPOINT)
    es_endpoint = es_url.netloc or es_url.path  # Extract the domain name in ES_ENDPOINT
    '''Post data to ES endpoint with SigV4 signed http headers'''
    req = AWSRequest(method=method, url=proto + es_endpoint + '/' + urllib.quote(path), data=payload,
                     headers={'Host': es_endpoint, 'Content-Type': 'application/json'})
    es_region = ES_REGION or os.environ['AWS_REGION']
    session = Session()
    SigV4Auth(get_credentials(session), 'es', os.environ['AWS_REGION']).add_auth(req)
    http_session = URLLib3Session()
    res = http_session.send(req.prepare())
    if res.status_code >= 200 and res.status_code <= 299:
        return res._content
    else:
        raise ES_Exception(res.status_code, res._content)

# Low-level HEAD data from Amazon Elasticsearch Service generating a Sigv4 signed request
def head_index_from_es(index_name, method='HEAD', proto='https://'):
    es_url = urlparse.urlparse(ES_ENDPOINT)
    es_endpoint = es_url.netloc or es_url.path  # Extract the domain name in ES_ENDPOINT
    '''Post data to ES endpoint with SigV4 signed http headers'''
    req = AWSRequest(method=method, url=proto + es_endpoint + '/' + urllib.quote(index_name),
                     headers={'Host': es_endpoint})
    es_region = ES_REGION or os.environ['AWS_REGION']
    session = Session()
    SigV4Auth(get_credentials(session), 'es', os.environ['AWS_REGION']).add_auth(req)
    http_session = URLLib3Session()
    res = http_session.send(req.prepare())
    if res.status_code >= 200 and res.status_code <= 299:
        logger.info('Index %s do exists, continue update setting', index_name)
        return True
    else:
        logger.warning('Index %s does not exists, need to create.', index_name)
        return False

def _lambda_handler():
    if not head_index_from_es(index_name=INDEX_NAME):
        # Index does not exist, need to create
        put_data_to_es(payload=json.dumps({}), path=INDEX_NAME)

    # Upload setting to index
    es_payload = {
        "index": {
            "max_result_window": "1000000"
        }
    }
    put_data_to_es(payload=json.dumps(es_payload), path=INDEX_NAME + '/_settings', method='PUT')

# Global lambda handler - catches all exceptions to avoid dead letter in the DynamoDB Stream
def lambda_handler(event, context):
    try:
        return _lambda_handler()
    except Exception:
        logger.error(traceback.format_exc())

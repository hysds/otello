import getpass
import json
import requests
import boto3
from requests.auth import HTTPBasicAuth


def get_authentication_token(hysds_host, username, aws_secret_id=None):
    """
    Prompt user for password and returns authentication token
    :param hysds_host: str, ex: "hysds-mozart.jpl.nasa.gov"
    :param username: str
    :param aws_secret_id: str, id of the secret created in AWS Secrets Manager
    :return: str, "authentication token"
    """
    if aws_secret_id:
        sm = boto3.client('secretsmanager')
        secret = sm.get_secret_value(SecretId=aws_secret_id)
        password = json.loads(secret['SecretString'])[username]
    else:
        password = getpass.getpass(prompt='Password: ')

    req = requests.get(
        f'https://{hysds_host}/mozart/api/v0.1/login',
        auth=HTTPBasicAuth(username, password)
    )
    req.raise_for_status()
    return req.json()['result']

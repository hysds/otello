import getpass
import requests
from requests.auth import HTTPBasicAuth


def get_authentication_token(hysds_host, username):
    """
    Prompt user for password and returns authentication token
    :param hysds_host: str, ex: "hysds-mozart.jpl.nasa.gov"
    :param username: str
    :return: str, "authentication token"
    """
    password = getpass.getpass(prompt='Password: ')
    req = requests.get(
        f'https://{hysds_host}/mozart/api/v0.1/login',
        auth=HTTPBasicAuth(username, password)
    )
    req.raise_for_status()
    return req.json()['result']

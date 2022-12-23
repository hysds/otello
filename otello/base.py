import os
from pathlib import Path
import yaml
import boto3
import requests
import json


class Base:
    def __init__(self, cfg=None, session=None):
        if cfg is None:
            cfg_dir = os.path.join(str(Path.home()), '.config/otello')
            cfg = os.path.join(cfg_dir, 'config.yml')

        self._cfg_file = cfg
        try:
            with open(cfg, 'r') as f:
                self._cfg = yaml.safe_load(f)
        except FileNotFoundError as e:
            raise FileNotFoundError(e)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(e)
        except Exception as e:
            raise Exception(e)

        if session:
            self._session = session
        else:
            self._session = requests.Session()
            if self._cfg["auth"] is True:
                try:
                    client = boto3.client("secretsmanager")
                    response = client.get_secret_value(
                        SecretId=self._cfg["aws_secret_id"]
                    )
                    secret_string = json.loads(response["SecretString"])
                    self._session.auth = (self._cfg["username"],
                                          secret_string[self._cfg["username"]]
                                          )
                except Exception as e:
                    raise Exception(f"Error occurred while trying to set "
                                    f"authentication using AWS Secrets "
                                    f"Manager:\n{str(e)}")
            self._session.verify = False

    def get_cfg(self):
        return self._cfg

    def build_auth_headers(self, headers=None, cfg=None):
        """
        TODO: need to implement authentication/SSO first

        take in cfg dictionary and builds authentication headers for request
        :param cfg: dict[str, str]
        :param headers: dict[str, str]
        :return:
        """

    def refresh_access_token(self):
        """
        TODO: need to implement authentication/SSO first

        use refresh token in config.yml to generate new access token
        write access_token and refresh_token to config.yml
        add access_token and refresh_token to self._cfg dictionary
        :return:
        """

    def generate_tokens(self):
        """
        TODO: need to implement authentication/SSO first

        prompt the user to login with username and password to generate new tokens and write to config.yml
        """

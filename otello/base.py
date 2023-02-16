import os
from pathlib import Path
import yaml
import boto3
import requests
import json
import warnings


class Base:
    def __init__(self, cfg=None, session=None, ssl_verify=None):
        if cfg is None:
            cfg_dir = os.path.join(str(Path.home()), '.config/otello')
            cfg = os.path.join(cfg_dir, 'config.yml')

        if isinstance(cfg, str):
            self._cfg_file = cfg
            with open(cfg, 'r') as f:
                self._cfg = yaml.safe_load(f)
                cfg_loaded_from_file = True
        elif isinstance(cfg, dict):
            self._cfg = cfg
        else:
            raise TypeError("cfg must be a path to a yaml file or a dict")

        if session:
            self._session = session
        else:
            self._session = requests.Session()
            if ssl_verify is None:
                ssl_verify = True
            elif ssl_verify is False:
                warnings.warn(
                    '''
                    SSL VERIFICATION HAS BEEN DISABLED! Credentials may
                    potentially be compromised. Do not use this option unless
                    necessary and acceptable to the use case.
                    '''
                )

            self._session.verify = ssl_verify

            if self._cfg.get("auth") is True:
                if self._cfg.get("username") is None:
                    raise ValueError("No username provided")

                if self._cfg.get("aws_secret_id") is not None:
                    try:
                        client = boto3.client("secretsmanager")
                        response = client.get_secret_value(
                            SecretId=self._cfg["aws_secret_id"]
                        )
                        secret_string = json.loads(response["SecretString"])
                        self._session.auth = (self._cfg["username"],
                                              secret_string[self._cfg["username"]])
                    except Exception as e:
                        raise Exception(f"Error occurred while trying to set "
                                        f"authentication using AWS Secrets "
                                        f"Manager:\n{str(e)}")
                elif self._cfg.get("password") is not None:
                    if cfg_loaded_from_file:
                        raise ValueError("Password provided in a plaintext "
                                         "file. Please remove password from "
                                         "config.yml and use AWS Secrets Manager "
                                         "instead")

                    self._session.auth = (self._cfg["username"],
                                          self._cfg["password"])
                else:
                    raise ValueError("No password or AWS secret ID provided")

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

import os
from pathlib import Path
import yaml
import requests


class Base:
    def __init__(self, cfg=None):
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

        self._session = requests.Session()
        if "pass" in self._cfg:
            self._session.headers.update({"Authorization", f"Basic {self._cfg['pass']}"})
        else:
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

import os
import requests

# from otello.utils import decorator
from otello.base import Base


class CI(Base):
    def __init__(self, repo=None, branch=None, cfg=None):
        """
        :param repo: str (required) git HTTPS repo url
        :param branch: str (optional) git branch
        :param cfg: file path to config.yml (default to ~/.config/otello/config.yml if not supplied)
        """
        if repo is None:
            raise RuntimeError("repo (+ branch) must be supplied")

        super().__init__(cfg=cfg)
        self.repo = repo
        self.branch = branch

    def check_job_exists(self):
        """
        Check if job is registered in Jenkins
        :return: True/False
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/job-builder')

        data = {
            'repo': self.repo,
        }
        if self.branch:
            data['branch'] = self.branch
        req = requests.get(endpoint, params=data, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        return res['success']

    def register(self):
        """
        Register job in Jenkins using the Mozart REST API: -X POST /api/ci/register
        :return: None
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/register')

        data = {
            'repo': self.repo,
        }
        if self.branch:
            data['branch'] = self.branch
        req = requests.post(endpoint, data=data, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        print(req.text)

    def unregister(self):
        """
        Delete job in Jenkins: -X DELETE /api/ci/register
        :return: dict[str, str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/register')

        payload = {
            'repo': self.repo,
        }
        if self.branch:
            payload['branch'] = self.branch
        req = requests.delete(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def submit_build(self):
        """
        Submit a Jenkins job build with the Mozart REST API
        :return: dict[str, str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/job-builder')

        data = {
            'repo': self.repo,
        }
        if self.branch:
            data['branch'] = self.branch
        req = requests.post(endpoint, data=data, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def get_build_status(self, build_number=None):
        """
        Retrieves build status
        :param build_number: int, (optional) will retrieve the latest build status if not supplied
        :return: dict[str, str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/build')

        payload = {
            'repo': self.repo,
        }
        if self.branch:
            payload['branch'] = self.branch
        if build_number is not None:
            payload['build_number'] = build_number

        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def stop_build(self):
        """
        Stops latest Jenkins buiild
        :return: dict[str, str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/job-builder')

        payload = {
            'repo': self.repo,
        }
        if self.branch:
            payload['branch'] = self.branch
        req = requests.delete(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def delete_build(self, build_number=None):
        """
        Deletes Jenkins job build (build must be stopped/failed/completed to delete)
        :return:
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/ci/build')

        payload = {
            'repo': self.repo,
        }
        if self.branch:
            payload['branch'] = self.branch
        if build_number is not None:
            payload['build_number'] = build_number

        req = requests.delete(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

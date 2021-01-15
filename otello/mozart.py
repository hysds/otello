import os
import json
from datetime import datetime
import requests

from otello.base import Base


class _MozartBase(Base):
    purge_job_name = 'job-lw-mozart-purge'

    def __init__(self, cfg=None):
        super().__init__(cfg=cfg)

    @staticmethod
    def __generate_tags():
        ts = datetime.now().isoformat()
        return 'otello_purge_%s' % ts

    def _get_job_status(self, _id):
        """
        Return job-status (mozart/api/v0.1/job/status/{_id})
        :param _id: ElasticSearch document id
        :return: str, {job-queued, job-started, job-completed, job-failed, job-deduped, job-offline}
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/status')
        payload = {
            'id': _id
        }
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        return res['status']

    def _get_job_info(self, _id):
        """
        Retrieve entire job payload (ES document)
        :param _id: str
        :return: dict[str, str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/info')

        payload = {
            'id': _id
        }
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def _get_generated_products(self, _id):
        """
        Return products staged for failed/completed jobs (mozart/api/v0.1/job/products/<_id>)
        :param _id: ElasticSearch document id
        :return: dict[str, str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/products/%s' % _id)
        req = requests.get(endpoint, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def _remove_job(self, _id, tags=None, priority=0, version='v1.0.5'):
        """
        Remove Job record with Purge Job PGE (mozart/api/v0.1/job/submit)
        :param _id: ElasticSearch document id
        :param tags: str; job tag
        :param priority: int; job priority in RabbitMQ
        :param version: str; purge job version (default v1.0.5)
        :return:
        """
        if _id is None:
            raise RuntimeError("ElasticSearch job _id must be supplied")
        if tags is None:
            tags = self.__generate_tags()
        if 9 < priority < 0:
            print("priority not in range (0-9], defaulting to 5")
            priority = 5

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"_id": _id}}
                    ]
                }
            }
        }
        params = {
            "query": query,
            "operation": "purge",
            "component": "mozart"
        }
        job_payload = {
            'queue': 'system-jobs-queue',
            'priority': priority,
            'job_name': self.purge_job_name,
            'tags': '["%s"]' % tags,
            'type': '%s:%s' % (self.purge_job_name, version),
            'params': json.dumps(params),
            'enable_dedup': False
        }
        print(json.dumps(job_payload, indent=2))

        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/submit')
        req = requests.post(endpoint, data=job_payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        job_id = res['result']
        print("purge job submitted, id: %s" % job_id)
        return Job(job_id=job_id, cfg=self._cfg_file)

    def _revoke_job(self, _id, tags=None, priority=0, version='v1.0.5'):
        """
        revoke Mozart job
        :param _id: (required) ElasticSearch document id
        :param tags: (optional) Tag job to track it
        :param priority: int (between 0-9)
        :param version: job version
        :return:
        """
        if _id is None:
            raise RuntimeError("ElasticSearch job _id must be supplied")
        if tags is None:
            tags = self.__generate_tags()
        if 9 < priority < 0:
            print("priority not in range (0-9], defaulting to 5")
            priority = 5

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"_id": _id}}
                    ]
                }
            }
        }
        params = {
            "query": query,
            "operation": "revoke",
            "component": "mozart"
        }
        job_payload = {
            'queue': 'system-jobs-queue',
            'priority': priority,
            'job_name': self.purge_job_name,
            'tags': '["%s"]' % tags,
            'type': '%s:%s' % (self.purge_job_name, version),
            'params': json.dumps(params),
            'enable_dedup': False
        }
        print(json.dumps(job_payload, indent=2))

        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/submit')
        req = requests.post(endpoint, data=job_payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        job_id = res['result']
        print("purge job submitted, id: %s" % job_id)
        return Job(job_id=job_id, cfg=self._cfg_file)


class Mozart(_MozartBase):
    def get_jobs(self):
        """
        retrieve list of PGE jobs
        :return: List[dict[str, str]]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'grq/api/v0.1/grq/on-demand')

        req = requests.get(endpoint, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        return req.json()

    def submit_job(self, job_name=None, queue=None, tags=None, priority=0, params=None):
        """
        Submit mozart job:

        params = {...}  # kwargs for job
        job_payload = {
            'queue': queue,
            'priority': '3',
            'job_name': job_type,
            'tags': '["{}_{}"]'.format(tag_name, slc_id),
            'type': "{}:{}".format(job_type, job_release),
            'params': json.dumps(params),
            'enable_dedup': False
        }
        :param job_name: str; HySDS job_name + job release (ie. job-lw-tosca-purge:v1.0.5)
        :param queue: str; job queue
        :param tags: (optional) str; what to tag your job (default to something if not supplied)
        :param priority: (optional) int; between 0-9
        :param params: (optional) dict; job parameters: ex:
            {
                "entity_id": "LC80101172015002LGN00",
                "min_lat": -79.09923,
                "max_lon": -125.09297,
                "id": "dumby-product-20161114180506209624",
                "acq_time": "2015-01-02T15:49:05.571384",
                "min_sleep": 1,
                "max_lat": -77.7544,
                "min_lon": -139.66082,
                "max_sleep": 10
            }
        :return: Job class instance
        """
        # TODO: need to check if /job/submit iterates through the results returned by the query
        if job_name is None and queue is None:
            raise RuntimeError("")
        if tags is None:
            tags = self.__generate_tags()

        if params is None:
            params = {}

        job_split = job_name.split('/')

        job_payload = {
            'queue': queue,
            'priority': priority,
            'job_name': job_split[0],
            'tags': '["%s"]' % tags,
            'type': job_name,
            'params': json.dumps(params),
            'enable_dedup': False
        }
        print(json.dumps(job_payload, indent=2))

        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/submit')
        req = requests.post(endpoint, data=job_payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        job_id = res['result']
        return Job(job_id=job_id, cfg=self._cfg_file)

    def get_job_info(self, _id):
        """
        Retrieve entire job payload (ES document)
        :param _id: str
        :return: dict[str, str]
        """
        return self._get_job_info(_id)

    def get_generated_products(self, _id):
        """
        Return products staged for failed/completed jobs
        :param _id: ElasticSearch document id
        :return: dict[str, str]
        """
        self._get_generated_products(_id)

    def get_job_status(self, _id):
        """
        Return job-status
        :return: str, {job-queued, job-started, job-completed, job-failed, job-deduped, job-offline}
        """
        return self._get_job_status(_id)

    def revoke_job(self, _id, **kwargs):
        """
        Submit revoke job with Revoke Job PGE
        :param _id ElasticSearch document id
        :param kwargs: tags (optional), priority, version
        :return:
        """
        return self._revoke_job(_id, **kwargs)

    def remove_job(self, _id, **kwargs):
        """
        Remove Job record with Purge Job PGE
        :param _id ElasticSearch document id
        :param kwargs: tags (optional), priority, version
        :return:
        """
        return self._remove_job(_id, **kwargs)


class Job(_MozartBase):
    def __init__(self, job_id=None, cfg=None):
        super().__init__(cfg=cfg)
        self.job_id = job_id

    def get_status(self):
        """
        Return job-status
        :return: str, {job-queued, job-started, job-completed, job-failed, job-deduped, job-offline}
        """
        return self._get_job_status(self.job_id)

    def get_info(self):
        """
        Retrieve entire job payload (ES document)
        :return: dict[str, str]
        """
        return self._get_job_info(self.job_id)

    def revoke(self, **kwargs):
        """
        Submit revoke job with Revoke Job PGE
        :param kwargs: tags (optional), priority, version
        :return:
        """
        return self._revoke_job(self.job_id, **kwargs)

    def remove(self, **kwargs):
        """
        Remove Job record with Purge Job PGE
        :param kwargs: tags (optional), priority, version
        :return:
        """
        return self._remove_job(self.job_id, **kwargs)

    def get_generated_products(self):
        """
        Return products staged for failed/completed jobs
        :return: dict[str, str]
        """
        return self._get_generated_products(self.job_id)

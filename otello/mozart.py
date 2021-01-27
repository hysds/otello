import os
import json
from datetime import datetime
import time
import requests

from otello.base import Base


class _MozartBase(Base):
    purge_job_name = 'job-lw-mozart-purge'

    def __init__(self, cfg=None):
        super().__init__(cfg=cfg)

    @staticmethod
    def __generate_tags(job_type):
        ts = datetime.now().isoformat()
        return 'otello_%s_%s' % (job_type, ts)

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
        res = req.json()
        return res['result']

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
        res = req.json()
        return res['results']

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
            raise Exception("ElasticSearch job _id must be supplied")
        if tags is None:
            tags = self.__generate_tags('purge')
        if 9 < priority < 0:
            print("priority not in range [0-9], defaulting to 5")
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
            raise Exception("ElasticSearch job _id must be supplied")
        if tags is None:
            tags = self.__generate_tags('revoke')
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

        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/submit')
        req = requests.post(endpoint, data=job_payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        job_id = res['result']
        print("purge job submitted, id: %s" % job_id)
        return Job(job_id=job_id, cfg=self._cfg_file)

    # def _retry_job(self, job_id):
    #     """
    #     retry job
    #     :param job_id: str, ElasticSearch job document ID
    #     :return:
    #     """
    #     current_job_status = self._get_job_status(job_id)
    #     if current_job_status in ('job-queued', 'job-started'):
    #         raise Exception("job (%s) is currently in %s state, cannot retry" % (job_id, current_job_status))


class Mozart(_MozartBase):
    def get_job_types(self):
        """
        retrieve list of PGE jobs
        :return: dict[str, JobType]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'grq/api/v0.1/grq/on-demand')

        req = requests.get(endpoint, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        job_types = res['result']

        jobs = {}
        for j in job_types:
            hysds_io = j['hysds_io']
            job_spec = j['job_spec']
            label = j.get('label')
            jobs[job_spec] = JobType(hysds_io=hysds_io, job_spec=job_spec, label=label)
        return jobs

    def get_job_type(self, job):
        """
        retrieve single PGE job
        :return: JobType
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'grq/api/v0.1/grq/on-demand')

        payload = {'id': job}
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()

        job_type = res['result']
        hysds_io = job_type['hysds_io']
        job_spec = job_type['job_spec']
        label = job_type.get('label')

        return JobType(hysds_io=hysds_io, job_spec=job_spec, label=label)

    def get_queue(self, job_name):
        """
        retrieve queue list and recommended queue
        :param job_name: str, job_spec name
        :return: dict[str, List[str]]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/queue/list')

        payload = {'id': job_name}
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        return res['result']

    def get_job_params(self, job_name):
        """
        grq/api/v0.1/grq/job-params?job_type=job-SCIFLO_GCOV:develop
        :param job_name: str, job_spec name
        :return: List[str]
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'grq/api/v0.1/grq/job-params')

        payload = {'job_type': job_name}
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        return res['params']

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
        :param params: (optional) dict; job parameters: will grab default params if not supplied
        ex:
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
            raise Exception("")
        if tags is None:
            tags = self.__generate_tags('submit_job')

        if params is None:
            params = {}
            default_params = self.get_job_params(job_name)
            for p in default_params:
                param_name = p['name']
                default_param_val = p.get('default', None)
                if default_param_val:
                    params[param_name] = default_param_val
                    continue
                if not p.get('optional', False) and default_param_val is None:
                    raise Exception('%s is not optional and default value not given' % param_name)

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
        return self._get_generated_products(_id)

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


class JobType(_MozartBase):
    def __init__(self, hysds_io=None, job_spec=None, label=None, cfg=None):
        """
        :param hysds_io: (str) hysds_ios ID
        :param job_spec: (str) job-specification
        """
        super().__init__(cfg=cfg)

        if hysds_io is None or job_spec is None:
            raise Exception("both hysds_io and job_spec must be supplied")

        self.hysds_io = hysds_io
        self.job_spec = job_spec
        self.label = label

        self.hysds_ios = {}
        self.job_specs = {}

        self.queues = {}
        self.default_queue = None

        self._params = {
            'dataset_params': {},
            'hardwired_params': {},
            'submitter_params': {}
        }

    def __str__(self):
        """
        Proper formatted job
        :return: str
        """
        if self.label:
            return 'HySDS Job: %s (%s)' % (self.label, self.job_spec)
        else:
            return 'HySDS Job: %s' % self.job_spec

    def get_queues(self):
        """
        retrieve and save the queue list and recommended queue(s)
        :return: {
            "queues": [...],
            "recommended": [...]
        }
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/queue/list')

        payload = {'id': self.job_spec}
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()

        queues = res['result']
        self.queues = queues
        if len(queues.get('recommended', [])) > 0:
            self.default_queue = queues['recommended'][0]

        return queues

    def initialize(self):
        """
        makes necessary backend API calls to get the HySDS-io params, TODO: maybe sets other things also
        :return:
        """
        host = self._cfg['host']
        endpoint = os.path.join(host, 'grq/api/v0.1/hysds_io/type')

        # TODO: params separated into 3 types (from): dataset_jpath, value, submitter
        #       "dataset_jpath": probably check if "from" starts with dataset_jpath (ex. dataset_jpath:_source.dataset)
        #                        will map to "dataset_params" (need more information)
        #       "value": map to "hardwired_params" (DONE)
        #       "submitter": map to "submitter_params" (DONE)

        payload = {'id': self.hysds_io}
        req = requests.get(endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()

        self.hysds_ios = res['result']  # saving the HySDS ios

        params = res['result']['params']
        for p in params:
            param_name = p['name']

            if p['from'] == 'value':  # hardwired params
                self._params['hardwired_params'][param_name] = p['value']

            if p['from'] == 'submitter':
                default_value = p.get('default', None)  # submitter params
                self._params['submitter_params'][param_name] = default_value

    def describe(self):
        """
        gets HySDS label, job_spec, hysds-ios, submitter parameters with descriptions of placeholders

        Job type: job-SCIFLO_GCOV:gmanipon-test-ade
        Tunable parameters:
          name: processing_type
          desc: Processing type: forward | reprocessing | urgent. Default: forward
          choices: forward, reprocessing, urgent
          ---
          name: fullcovariance
          desc: Compute cross-elements (True) or diagonals only (False). Default: False
          choices: False, True
          ---
          name: output_type
          desc: "Choices: 'None' (to turn off RTC) or 'gamma0'
          choices: None, gamma0
          ---
          name: algorithm_type
          desc: Choices: 'area-projection' (default) and 'David-Small'
          choices: area-projection, David-Small
          ---
          name: output_posting
          desc: Output posting in same units as output EPSG. Single value or list indicating the output posting for each
                frequency. Default '[20, 100]'
        Dataset parameters:
          name: product_paths
          ---
          name: product_metadata
          ---
          name: input_dataset_id
          ---
          name: dataset_type
        """
        if not self.hysds_ios:
            raise Exception("Job specifications is empty, please initialize the JobType with .initialize()")

        output = 'Job Type: %s\n' % self.hysds_ios['job-specification']
        if self.hysds_ios.get('label'):
            output += 'Label: %s\n' % self.hysds_ios['label']
        output += '\n'

        tunable_params = 'Tunable Parameters:\n'
        dataset_params = 'Dataset Parameters:\n'

        for p in self.hysds_ios['params']:
            param_name = p['name']
            placeholder = p.get('placeholder')

            if p['from'] == 'submitter':
                tunable_params += '\tname: %s\n' % param_name
                if placeholder:
                    tunable_params += '\tdesc: %s\n' % placeholder
                if p['type'] == 'enum':
                    tunable_params += '\tchoices: %s\n' % p['enumerables']
                tunable_params += '\n'

            if p['from'].startswith('dataset_jpath'):
                dataset_params += '\tname: %s\n' % param_name
                dataset_params += '\n'
        print(output + '\n' + tunable_params + '\n' + dataset_params)

    def set_input_params(self):
        """
        prompting user for submitter inputs
        """
        if not self.hysds_ios:
            raise Exception("Job specifications is empty, please initialize the JobType with .initialize()")

        submitter_params = filter(lambda x: x['from'] == 'submitter', self.hysds_ios['params'])
        for p in submitter_params:
            param_name = p['name']
            default_value = p.get('default', None)
            placeholder = p.get('placeholder', None)

            if placeholder:
                print('NAME: %s (%s)' % (param_name, placeholder))
            else:
                print('NAME: %s' % param_name)

            if p['type'] == 'enum':
                options = json.dumps(p['enumerables'])
                if default_value is not None:
                    param_value = input('Set value, options: %s\nSkip to use default (%s): ' % (options, default_value))
                    if not param_value:
                        param_value = default_value
                else:
                    param_value = input('Set value: options: %s: ' % options)
            else:
                if default_value is not None:
                    param_value = input('Set value, skip to use default (%s): ' % default_value)
                    if not param_value:
                        param_value = default_value
                else:
                    param_value = input('Set value: ')
            print('')
            self._params['submitter_params'][param_name] = param_value

    def set_input_dataset(self, dataset=None):
        """
        dataset taken from Pele and sets it to the dataset params in hysds-ios
        :param dataset: dict[str, str|dict|list]
        """
        if dataset is None:
            raise Exception("dataset must be set for your job")
        if not self.hysds_ios:
            raise Exception("Job specifications is empty, please initialize the JobType with .initialize()")

        dataset_params = filter(lambda x: x['from'].startswith('dataset_jpath'), self.hysds_ios['params'])
        for p in dataset_params:
            param_name = p['name']
            if 'lambda' in p:
                f = eval(p['lambda'])
                self._params['dataset_params'][param_name] = f(dataset)
            else:
                parsed_path = p['from'].replace('dataset_jpath:', '').replace('_source.', '')
                if parsed_path == '_id':
                    # case 1: if _id, get id instead from pele results
                    self._params['dataset_params'][param_name] = dataset['id']
                else:
                    # case 2: remove dataset_jpath:_source, get list of paths and traverse
                    parsed_path = parsed_path.split('.')
                    for path in parsed_path:
                        dataset = dataset[path]
                    self._params['dataset_params'][param_name] = dataset

    def get_hardwire_params(self):
        return self._params['hardwired_params']

    def get_input_dataset(self):
        return self._params['dataset_params']

    def get_input_params(self):
        return self._params['submitter_params']

    def submit_job(self, queue=None, tag=None, priority=1):
        """
        :param tag:
        :param priority: int, job priority [1-9] in RabbitMQ
        :param queue:
        :return: Job class object with _id
        """
        if queue is None and self.default_queue is None:
            raise Exception("queue must be supplied")
        if tag is None:
            tag = self.__generate_tags('submit_job')

        params = {
            **self._params['dataset_params'],
            **self._params['hardwired_params'],
            **self._params['submitter_params']
        }
        job_split = self.job_spec.split(':')
        job_payload = {
            'queue': queue or self.default_queue,
            'priority': priority,
            'job_name': job_split[0],
            'tags': '["%s"]' % tag,
            'type': self.job_spec,
            'params': json.dumps(params),
            'enable_dedup': False
        }
        host = self._cfg['host']
        endpoint = os.path.join(host, 'mozart/api/v0.1/job/submit')
        req = requests.post(endpoint, data=job_payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()
        job_id = res['result']
        return Job(job_id=job_id, cfg=self._cfg_file)


class Job(_MozartBase):
    def __init__(self, job_id=None, cfg=None):
        super().__init__(cfg=cfg)
        self.job_id = job_id

    def __str__(self):
        return 'Mozart Job: <%s>' % self.job_id

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

    def wait_for_completion(self):
        """
        will loop (with 30 second delay) until the job compeltes (or fails)
        :return: str: job status when job completed (or fails)
        """
        time.sleep(3)
        while True:
            try:
                status = self.get_status()
                print(f"{status} {datetime.utcnow().isoformat('T')}")
                if status not in ('job-failed', 'job-deduped', 'job-completed', 'job-offline'):
                    print('%s job status: %s' % (self.job_id, status))
                else:
                    return status
            except Exception as e:
                print(e)
            time.sleep(30)


class JobSet(_MozartBase):
    def __init__(self, job_set=None, cfg=None):
        """
        List of Job class objects to track multiple job submissions
        :param job_set: list[Job], list of Job(s)
        """
        if job_set is None:
            raise Exception("job_set must be supplied, ex. [<Job class object>, <Job class object>, ...]")
        super().__init__(cfg=cfg)
        self.job_set = job_set

    def get_statuses(self):
        pass

    # def wait_for_completion(self):
    #     pass

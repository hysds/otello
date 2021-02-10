import os
import ast
import json
from datetime import datetime
import time
import requests

from otello.base import Base


class _MozartBase(Base):
    PURGE_JOB_NAME = 'job-lw-mozart-purge'

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
        payload = {'id': _id}
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

        payload = {'id': _id}
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
            'job_name': _MozartBase.PURGE_JOB_NAME,
            'tags': '["%s"]' % tags,
            'type': '%s:%s' % (_MozartBase.PURGE_JOB_NAME, version),
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
            'job_name': _MozartBase.PURGE_JOB_NAME,
            'tags': '["%s"]' % tags,
            'type': '%s:%s' % (_MozartBase.PURGE_JOB_NAME, version),
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


class Mozart(_MozartBase):
    """
    Driver class for Mozart for users to get a list of available jobs. etc

    methods:
        get_job_type: returns a singular JobType
        get_job_types: retrieves a Dictionary of JobType(s) with the job_name
    """
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
    """
    Mozart Job Type developers can use to submit to HySDS as jobs

    methods:
        initialize: grab the job wiring and queue(s) from the HySDS rest API
        get_queues: retrieve and set the queue(s)
        describe: print the Job Type description
        set_input_params: set the tune-able parameters (dictionary as input)
        get_input_params: retrieve the tune-able parameters
        set_input_dataset: set a HySDS dataset (retrieved from Pele) into the job type to submit
        get_input_dataset: retrieve the dataset parameters
        submit_job: submit Job to HySDS, returns a Job class object
    """
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
        self.queues = {}
        self.default_queue = None

        self._params = {
            'dataset_params': {},
            'hardwired_params': {},
            'input_params': {}
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

    def initialize(self):
        """
        makes necessary backend API calls to get the HySDS-io params
        :return:
        """

        self._retrieve_hysds_ios()  # retrieve the HySDS io's
        self._retrieve_queues()  # retrieve the queues

    def describe(self):
        """
        gets HySDS label, job_spec, hysds-ios, submitter parameters with descriptions of placeholders

        Job type: job-SCIFLO_GCOV:gmanipon-test-ade
        Tunable parameters:
          name: processing_type
          desc: Processing type: forward | reprocessing | urgent. Default: forward
          choices: forward, reprocessing, urgent
          ...

        Dataset parameters:
          name: product_paths
          ...
        :return:
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
                param_type = p.get('type', 'text')
                default_value = p.get('default')
                optional = p.get('optional', False)

                tunable_params += '\tname: %s\n' % param_name
                tunable_params += '\ttype: %s\n' % param_type

                if placeholder:
                    tunable_params += '\tdesc: %s\n' % placeholder
                if p['type'] == 'enum':
                    tunable_params += '\tchoices: %s\n' % p['enumerables']
                if default_value is not None:
                    tunable_params += '\tdefault: %s\n' % default_value
                if optional is True:
                    tunable_params += '\toptional: %s\n' % optional
                tunable_params += '\n'

            if p['from'].startswith('dataset_jpath'):
                dataset_params += '\tname: %s\n' % param_name
                dataset_params += '\n'
        print(output + '\n' + tunable_params + '\n' + dataset_params)

    def _retrieve_hysds_ios(self):
        """
        retrieve HySDS ios from GRQ's rest API and set the default input parameters
        :return: None
        """
        host = self._cfg['host']
        job_endpoint = os.path.join(host, 'grq/api/v0.1/hysds_io/type')

        payload = {'id': self.hysds_io}
        req = requests.get(job_endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()

        self.hysds_ios = res['result']  # saving the HySDS ios

        params = res['result']['params']
        for p in params:
            param_name = p['name']

            if p['from'] == 'value':  # hardwired params
                self._params['hardwired_params'][param_name] = p['value']
            elif p['from'] == 'submitter':
                default_value = p.get('default', None)  # submitter params
                if p['type'] == 'number':
                    if default_value is not None:
                        if type(default_value) == str:
                            default_value = ast.literal_eval(default_value)
                if p['type'] == 'boolean':
                    if default_value is not None:
                        if type(default_value) != bool:
                            default_value = True if default_value.lower() == 'true' else False
                self._params['input_params'][param_name] = default_value

    def _retrieve_queues(self):
        """
        retrieve the job queues from Mozart's Rest API
        :return: None
        """
        host = self._cfg['host']
        queue_endpoint = os.path.join(host, 'mozart/api/v0.1/queue/list')
        payload = {'id': self.job_spec}
        req = requests.get(queue_endpoint, params=payload, verify=False)
        if req.status_code != 200:
            raise Exception(req.text)
        res = req.json()

        queues = res['result']
        self.queues = queues
        if len(queues.get('recommended', [])) > 0:
            self.default_queue = queues['recommended'][0]

    def get_queues(self):
        """
        returned the queue and default queue saved in class
        :return: Dict[str, str|List]
        """
        if len(self.queues) == {}:
            raise Exception("Please initialize the JobType object with .initialize()")

        return {
            'queues': self.queues,
            'default': self.default_queue
        }

    def set_input_params(self, params):
        """
        setting the user input parameters for the job
        :param params: None
        """
        if type(params) != dict:
            raise Exception("params must be dictionary")

        current_params = self._params['input_params']
        constructed_params = {}

        for k, v in params.items():
            constructed_params[k] = v
        self._params['input_params'] = {
            **current_params,
            **constructed_params
        }

    def prompt_input_params(self):
        """
        prompting user for input parameters
        :return: None
        """
        current_params = self._params['input_params']
        constructed_params = {}

        input_params = filter(lambda x: x['from'] == 'submitter', self.hysds_ios['params'])

        for p in input_params:
            param_name = p['name']
            param_type = p['type']
            default = p.get('default', None)
            placeholder = p.get('placeholder', None)
            optional = p.get('optional', False)

            prompt = 'NAME: %s (%s)' % (param_name, param_type)
            if placeholder:
                prompt += ' (%s)' % placeholder
            print(prompt)

            input_prompt = 'SET VALUE'
            if param_type == 'enum':
                options = p['enumerables']
                input_prompt += '. options (%s)' % options
            elif param_type == 'boolean':
                input_prompt += ' (true/false)'

            if default is not None:
                input_prompt += '.\nSkip to use default (%s)' % default
            input_prompt += ': '

            value = input(input_prompt)
            if not value:
                if default is not None:  # if value is not given, use the default already set
                    continue
                elif optional is True:
                    constructed_params[param_name] = None
                    continue
                else:
                    raise ValueError("%s is required" % param_name)

            if param_type == 'number':
                value = ast.literal_eval(value)
                if type(value) not in (int, float):
                    raise ValueError('{} is not type: number' % value)
            elif param_type == 'boolean':
                value = True if value == 'true' else False
            elif param_type == 'object':
                value = ast.literal_eval(value)
                if type(value) not in (list, object):
                    raise ValueError('{} is not a List or Dict' % value)
            constructed_params[param_name] = value

        self._params['input_params'] = {
            **current_params,
            **constructed_params
        }

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
        return self._params['input_params']

    def submit_job(self, queue=None, tag=None, priority=1):
        """
        job_payload = {
            'queue': queue,
            'priority': '3',
            'job_name': job_type,
            'tags': '["{}_{}"]'.format(tag_name, slc_id),
            'type': "{}:{}".format(job_type, job_release),
            'params': json.dumps(params),
            'enable_dedup': False
        }
        :param tag: str, job tag to track
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
            **self._params['input_params']
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
    """
    Job submitted to HySDS

    methods:
        get_status: get job status {job-queued, job-started, job-completed, job-failed, job-deduped, job-offline}
        get_info: return Mozart's job payload
        revoke: submit a mozart Job to revoke current job
        remove: Remove Job record with Purge Job PGE
        get_generated_products: Return products staged for failed/completed jobs
        wait_for_completion: will loop (with 30 second delay) until the job compeltes (or fails)
    """
    def __init__(self, job_id=None, cfg=None):
        """
        :param job_id: str, job UUID
        """
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
                print(f"{self.job_id}: {status} {datetime.utcnow().isoformat('T')}")
                if status in ('job-failed', 'job-deduped', 'job-completed', 'job-offline'):
                    return status
            except Exception as e:
                print(e)
            time.sleep(30)


class JobSet(_MozartBase):
    """
    List of Job class objects to track multiple job submissions
    able to iterate through and check for multiple job completions

    methods:
        append: adding Job object to current set of jobs
        wait_for_completion: wait for all "completion" of jobs
    """
    def __init__(self, job_set=None, cfg=None):
        """
        :param job_set: list[Job], list of Job(s)
        """
        super().__init__(cfg=cfg)

        if job_set is None:
            self.job_set = []
        else:
            if type(job_set) != list:
                raise TypeError("job_set must be a List[<Job>]")
            for job in job_set:
                if job.__class__ != Job:
                    raise TypeError("all entries in job_set must be of type <Job>")
            self.job_set = job_set

    def __len__(self):
        return len(self.job_set)

    def __iter__(self):
        return (job for job in self.job_set)

    def __getitem__(self, i):
        return self.job_set[i]

    def append(self, job):
        """
        add submitted HySDS job to stored list of jobs
        :param job: Job object to be appended
        """
        if job.__class__ != Job:
            raise TypeError("appended job must be of type <Job>")
        self.job_set.append(job)

    def wait_for_completion(self):
        """
        will loop (with 30 second delay) until through all jobs and break if all jobs are completed (or failed)
        :return: str: job status when job completed (or fails)
        """
        time.sleep(3)
        while True:
            time.sleep(0.5)
            completed_jobs = 0
            for job in self.job_set:
                try:
                    status = job.get_status()
                    print(f"{job.job_id}: {status} {datetime.utcnow().isoformat('T')}")
                    if status in ('job-failed', 'job-deduped', 'job-completed', 'job-offline'):
                        completed_jobs += 1
                except Exception as e:
                    print(e)
                    completed_jobs += 1
            if completed_jobs == len(self.job_set):
                return
            time.sleep(30)

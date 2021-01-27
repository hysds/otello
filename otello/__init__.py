import urllib3

from otello.ci import CI
from otello.mozart import Mozart, Job, JobType, JobSet
from otello.client import initialize

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

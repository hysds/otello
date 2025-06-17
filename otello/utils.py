from datetime import datetime


def generate_tags(job_type):
    ts = datetime.now().isoformat()
    return 'otello_{}_{}'.format(job_type, ts)

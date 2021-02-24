from datetime import datetime


def generate_tags(job_type):
    ts = datetime.now().isoformat()
    return 'otello_%s_%s' % (job_type, ts)

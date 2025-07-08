from datetime import datetime, timezone


def generate_tags(job_type):
    ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    return 'otello_{}_{}'.format(job_type, ts)

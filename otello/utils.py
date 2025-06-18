from datetime import datetime, UTC


def generate_tags(job_type):
    ts = datetime.now(UTC).replace(tzinfo=None).isoformat()
    return 'otello_{}_{}'.format(job_type, ts)

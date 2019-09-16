"""
Based on https://github.com/timberio/timber-python/blob/master/timber/frame.py
"""
from datetime import datetime


def create_frame(record, message, rank):
    r = record.__dict__
    frame = {}
    frame['dt'] = datetime.utcfromtimestamp(r['created']).isoformat()
    frame['level'] = _levelname(r['levelname'])
    frame['severity'] = int(r['levelno'] / 10)
    frame['message'] = message
    frame['context'] = ctx = {}

    # Runtime context
    ctx['runtime'] = runtime = {}
    runtime['function'] = r['funcName']
    runtime['file'] = r['filename']
    runtime['line'] = r['lineno']
    runtime['thread_id'] = r['thread']
    runtime['thread_name'] = r['threadName']
    runtime['logger_name'] = r['name']

    # Runtime context
    ctx['system'] = system = {}
    system['pid'] = r['process']
    system['process_name'] = r['processName']
    system['rank'] = rank

    return frame


def _levelname(level):
    return {
        'debug': 'debug',
        'info': 'info',
        'warning': 'warn',
        'error': 'error',
        'critical': 'critical',
    }[level.lower()]

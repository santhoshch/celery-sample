import os
import errno
import functools
import collections
import json
import logging
from django.http.response import HttpResponse
import requests
import datetime
import base64
import itertools
from subprocess import Popen, PIPE
from celery.task.control import inspect
import time
from collections import namedtuple

logger = logging.getLogger(__name__)
retries = 5

FileInfo = namedtuple('fileinfo',
        ['dataset',         # Dataset this file belongs to
         'stage',           # Stage name of the file. _data for approved files
         'filetype',        # Type of the file
         'name',            # Name of the file
         'fullpath'         # Complete path that uniquely identifies
                            # the file, with in the storage system.
        ])



def queue_name(basename, worker_pool=None):
    worker_pool = worker_pool or os.environ.get('WORKER_POOL', None)
    if not worker_pool:
        return basename
    return "%s_%s" % (worker_pool, basename)


def tasks_queue_name(basename, task_pool, debug):
    if str(task_pool) in ('None', 'default', 'primary', 'main'):
        task_pool = 'tasks-pool'
    if not debug:
        task_pool = "%s_%s" % (task_pool, basename)
    else:
        task_pool = basename
    if not worker_availability(task_pool):
        logger.info('task_pool(%s) is not started,waiting for pool' % task_pool)

    return task_pool


def worker_availability(name):
    for i in range(retries):
        try:
            q = inspect().active_queues()
            if not q:
                return False
            for key in q:
                for ins in q[key]:
                    if name == str(ins['name']):
                        return True
            return False
        except Exception as e:
            wait_time = (i + 1) * 5
            if i == (retries-1):
                raise e
            logger.info("Retry - %s worker_availability failed with %s Waiting for %d seconds" % ((i+1), e, wait_time))
            time.sleep(wait_time)


def mkdir(p):
    try:
        os.mkdir(p)
    except OSError as e:
        if(e.errno == errno.EEXIST):
            pass
        else:
            raise


def mkdir_p(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            return
        else:
            raise OSError("A file already exist at the specified path")
    else:
        # Make sure parent exists or created
        parent = os.path.dirname(path)
        mkdir_p(parent)

        # Create the directory
        os.mkdir(path)
FileInfo = namedtuple('fileinfo',
        ['dataset',         # Dataset this file belongs to
         'stage',           # Stage name of the file. _data for approved files
         'filetype',        # Type of the file
         'name',            # Name of the file
         'fullpath'         # Complete path that uniquely identifies
                            # the file, with in the storage system.
        ])

# This package will hold all the offline tasks we allow customers to perform
from celery_sample.settings import DEBUG
import celery
from celery import current_task
import sys
import os
import logging
import json
from celery.result import AsyncResult
import time
import uuid
import celerytasks
import tempfile
import shutil
from domainmodel.app import Task
from datetime import datetime

from celery_sample import sec_context

logger = logging.getLogger('gnana.%s' % __name__)
task_statuses = {
    0: 'Created',
    1: 'Started',
    2: 'Finished',
    3: 'Error',
    4: 'Submitted'
}
large_response_key = 'large_response_key'
filename_key = 'filename'

dateUtils = datetime
probe_util = None

import threading


class Tracer(object):

    def __init__(self):
        self.thread_local = None

    def get_trace(self):
        if not self.thread_local:
            self.thread_local = threading.local()
        try:
            return self.thread_local.trace_id
        except AttributeError:
            self.thread_local.trace_id = str(uuid.uuid4())
            return self.thread_local.trace_id

    def set_trace(self, trace):
        if not self.thread_local:
            self.thread_local = threading.local()
        if trace:
            self.thread_local.trace_id = trace
        else:
            try:
                del self.thread_local.trace_id
            except AttributeError:
                pass

    trace = property(get_trace, set_trace)


tracer = Tracer()


def celery_task_handler(tasks, cleanup, kill_siblings_on_fail=False, d=0):

    from celerytasks import asynctasks

    def find_kill_children(traceid):
        criteria = {'object.parent_id': traceid}
        task_obj = Task()
        result = task_obj.getAll(criteria)
        for t in result:
            res = AsyncResult(t.extid)
            if(res.state not in ['SUCCESS', 'FAILURE']):
                t.status = Task.STATUS_ERROR
                t.save()
                t.current_status_msg = 'terminated'
                res.revoke(terminate=True, signal='SIGKILL')
    results = []
    failed_task_message = ''
    failed = False
    if d == 0:
        task_list = [asynctasks.subtask_handler(
            t[0], kwargs=t[1], queue=t[2]) for t in tasks]
    elif d == 1:
        task_list = [asynctasks.subtask_handler(t=t, d=d) for t in tasks]
    elif d == 2:
        task_list = [
            asynctasks.subtask_handler(t=t[0], queue=t[1], d=d) for t in tasks]

    while task_list:
        remainingtasks = task_list
        time.sleep(2)
        try:
            for t in remainingtasks:
                if t.ready():
                    # if isinstance(result,list) else results.append(result)
                    resp = t.get()
                    results.append(resp)
                    task_list.remove(t)

        except Exception as ex:
            failed = True
            failed_task_message += t.id + \
                ' failed with the message' + ex.message
            task_list.remove(t)
            if kill_siblings_on_fail:
                break

    if kill_siblings_on_fail and failed:
        for t in task_list:
            mytask = Task.getByFieldValue('extid', t.task_id)
            mytask.status = Task.STATUS_ERROR
            mytask.current_status_msg = 'terminated' + failed_task_message
            mytask.save()
            t.revoke(terminate=True, signal='SIGKILL')
            find_kill_children(t.task_id)
    if failed:
        if cleanup is not None:
            cleanup()
        raise Exception(
            'Failures detected in subtasks:\n ' + failed_task_message)
    return results


def gnana_task_support(f):
    """ A decorator function that wraps the given function with the ability
    to set the tenant name in tenant local, and saves the return values
    for later retrieval """
    fn = f

    def new_task(user_and_tenant, *args, **options):
        user_name = user_and_tenant[0]
        tenantname = user_and_tenant[1]
        logintenant = user_and_tenant[2]

        # Set the tenant name first
        try:
            oldname = (
                sec_context.user_name, sec_context.name, sec_context.login_tenant_name)
        except AttributeError:
            oldname = None

# Setup logging
#         if log_config:
#             log_config.restart()

        if 'trace' in options:
            old_trace = tracer.trace
            tracer.trace = options['trace']
        else:
            old_trace = None
            # Make sure a new trace id is generated
            tracer.trace = None

        t = None
        failed = False
        try:
            # Set the tenant name
            sec_context.set_context(user_name, tenantname, logintenant)
            logger.info(
                "Begin execution of Task %s[%s]", current_task.name, current_task.request.id)

            t = Task.getByFieldValue(
                'extid', current_task.request.id) if current_task.request.id else None
            if(t):
                if t.status != 4:  # 4 stands of Submitted
                    logger.info("Task %s[%s] has been terminated",
                                current_task.name, current_task.request.id)
                    logger.info("Expected status Submitted but has status %s",
                                task_statuses[t.status])
                    t.stop_time = dateUtils.now()
                    t.status = Task.STATUS_ERROR
                    t.save()
                    failed = True
                    return
                t.status = Task.STATUS_STARTED
                t.start_time = dateUtils.now()
                t.save()

            # Check for probeid and do the needful
            if 'probeid' in options:
                location = "TASK"
                try:
                    location += "." + fn.func_name
                except:
                    pass
#                 probe_util.start(options['probeid'], location)

            is_debug = options.get('debug', "")
            if is_debug and DEBUG:
                import pydevd
                pydevd.settrace(options['debug'])

            profile_type = options.get('profile', False)
            if profile_type:
                ret_value = {}
                if profile_type == 'line_profile':
                    import line_profiler
                    profiler = line_profiler.LineProfiler()
                    fn1 = profiler(f)
                    ret_value['ret'] = fn1(user_and_tenant, *args, **options)
                    profiler.dump_stats(
                        get_profile_filename("lp_" + fn.__name__))
                else:
                    import cProfile
                    cProfile.runctx('ret_value["ret"]=fn(user_and_tenant, *args, **options)',
                                    locals(), globals(),
                                    filename=get_profile_filename("cp_" + fn.__name__))
                return ret_value['ret']
            else:
                fn_response = fn(user_and_tenant, *args, **options)
                """ if response is a dictionary, the the large response is stored in S3, and the key is passed around """
                return check_response_size(fn_response, user_and_tenant, str(tracer.trace))

        except Exception as ge:
            # Do the house keeping first.  Saving error state may
            # fail too :-(
            failed = True
            logger.error("Task (%s) Failed to execute with GnanaError: %s \n",
                         current_task.request.id, ge,
                         exc_info=sys.exc_info())

            # We are fetching again to account for any updates by
            # the called tasks during their execution.
            t = Task.getByFieldValue('extid', current_task.request.id)
            if t:
                t.stop_time = dateUtils.now()
                t.status = Task.STATUS_ERROR
                t.save()
                # revokejobs(t.trace)

            raise Exception(
                "%s" % json.dumps(ge) if ge and isinstance(ge, dict) else "GnanaError raised.")

        except Exception as ex:
            # Do the house keeping first.  Saving error state may
            # fail too :-(
            failed = True
            logger.error("Task (%s) Failed to execute",
                         current_task.request.id, exc_info=sys.exc_info())

            # We are fetching again to account for any updates by
            # the called tasks during their execution.
            t = Task.getByFieldValue('extid', current_task.request.id)
            trace_id = None
            if t:
                t.stop_time = dateUtils.now()
                t.status = Task.STATUS_ERROR
                t.save()
                # revokejobs(t.trace)
            trace_id = None
            try:
                trace_id = t.trace
            except:
                pass
            raise Exception("%s, Trace_id: %s" % (ex.message, trace_id))
        finally:
            if not failed:
                # We are fetching again to account for any updates by
                # the called tasks during their execution.
                t = Task.getByFieldValue('extid', current_task.request.id)
                if t:
                    t.stop_time = dateUtils.now()
                    t.status = Task.STATUS_FINISHED
                    t.save()
                logger.info(
                    "Task (%s) completed successfully. ", current_task.request.id)
            # If we are probing end it
#             probe_util.end()
            if old_trace:
                tracer.trace = old_trace
            sec_context.set_context(*oldname)
    return new_task


"""
If the response size is big, thenstore the respnse in S3 and send the key back, so during reading the response,
the json is loaded from the key.
"""


def check_response_size(fn_response, user_and_tenant, trace):
    return fn_response


def get_profile_filename(prefix=""):
    return "%sprofile_%s_%s.prof" % (prefix, os.getpid(), time.time())


class UnknownTaskError(Exception):

    """ Raised when the completed task is not found  to be pending"""
    pass

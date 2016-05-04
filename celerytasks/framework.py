'''
Created on 02-May-2016

@author: ravi
'''
import datetime
import json
import logging
import sys

from celery import current_task

from celery_sample.settings import DEBUG, is_true
from celerytasks import tracer, sec_context
from domainmodel.app import Task
from utils import tasks_queue_name, queue_name, worker_availability
logger = logging.getLogger('gnana.%s' % __name__)


def subtask_handler(t, kwargs=None, queue=None, args=(), d=0):
    if kwargs is None:
        kwargs = {}
    tid = None

    if d == 0:
        tid = t.apply_async(args=args, kwargs=kwargs, queue=queue)

    elif d == 1:
        tid = t.apply_async()

    elif d == 2:
        tid = t.apply_async(queue=queue)

    trace = tracer.trace
    logger.info('trace used in whie saving task object %s', trace)
    if not current_task:
        ts = Task.getByFieldValue('trace', trace)
        ts.extid = tid.id
        ts.main_id = tid.id
        ts.submit_time = datetime.datetime.utcnow()
        ts.status = Task.STATUS_SUBMITTED
        ts.save(is_partial=True)

    else:

        pid = current_task.request.id
        na = tid.task_name
        di = {}
        if kwargs:
            di['kwargs'] = kwargs
        if args:
            di['args'] = args

        if not Task.get_mainid(trace):
            logger.info("Main task is None for (%s)", current_task.request.id)
        Task.set_task(trace, Task.get_mainid(
            trace), tid.id, pid, Task.STATUS_SUBMITTED, di, na)

    return tid


def submitasync(task, *args, **options):
    try:
        t = Task()
        t.type = str(task.name)
        t.createdby = 'sample_user'
#         context_data = self.get_context_data(request, *args, **options)
#         if(isinstance(context_data, HttpResponse)):
#             return context_data
#         elif(not isinstance(context_data, dict)):
#             raise Exception("Unexpected error.  Expecting a dictionary")
#
#         options.update(context_data)
#         options['profile'] = request.GET.get('profile')
#         options['debug'] = request.GET.get('debug')
        options['trace'] = tracer.trace
        # wait_for_pool will be string 'True'
        wait_for_pool = True

        # task_meta is the context information, so when we query for
        # mytasks, the information is a little more useful.
        task_meta = {}
        task_meta.update(options)
        task_meta['request_params'] = {}
#         task_meta['request_params'][
#             'path'] = request.path if not request.path == None else "No path in request?"
#         task_meta['request_params']['params'] = json.dumps(request.GET)

        t.task_meta = task_meta
        t.trace = tracer.trace
        t.tenant = sec_context.name

        base_queue_name = 'master'
        base_queue_name = options.pop('queue', base_queue_name)
        worker_pool = None
        if worker_pool in ('None', 'default', 'primary', 'main'):
            worker_pool = None
        #inspector = celery.current_app.control.inspect()
        #worker_pool in inspector
        celery_q = queue_name(base_queue_name, worker_pool)
        if base_queue_name == 'tasks':
            celery_q = tasks_queue_name(
                base_queue_name, None, DEBUG)
            worker_pool = celery_q.split('_')[0]

        logger.info("Submitting message into queue %s", celery_q)
        ret = {}
        if not DEBUG:
            if not worker_availability(celery_q):
                pool_name = worker_pool if worker_pool else '#PRIMARY POOL#'
                if is_true(wait_for_pool):
                    ret.update({
                        'worker_status': 'not_created',
                        'worker_pool': pool_name
                    })
                else:
                    kind_of_worker_pool = "{0} pool missing. \n Unable to submit the task, {1} pool not started, use 'wait_for_pool' = True to wait for pool to start".format(
                        'Non-celery' if base_queue_name == 'tasks' else 'Celery', pool_name)
                    logger.exception(kind_of_worker_pool)
                    raise Exception(kind_of_worker_pool)
        # Save first to make sure if something is picked up really fast, task
        # is still there
        t.save()
        jobid = subtask_handler(t=task, args=([sec_context.user_name, sec_context.name, sec_context.login_tenant_name],),
                                kwargs=options,
                                queue=celery_q)
        njobid = jobid.id
        if options.get('wait', False):
            return jobid.get()
        logger.info(
            "Job with ID %s is submitted into queue %s ", jobid, celery_q)
        ret.update({'jobid': njobid, 'trace': tracer.trace})
        return json.dumps(ret)
#         return HttpResponse(json.dumps(ret),
#                             "application/json")
    except:
        logger.error(
            "Unable to submit the async message.", exc_info=sys.exc_info())
        raise

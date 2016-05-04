from celerytasks.tasker.gtasks import run_master_task, load_task
from celerytasks.tasker.cache import TasksCache
from celerytasks import tracer
from utils import tasks_queue_name
import os
import logging
from celerytasks import asynctasks
from celery_sample.settings import DEBUG
from celery import current_task

logger = logging.getLogger('gnana.%s' % __name__)


def run_celery_task(user_and_tenant, task, params, context):
    """
    A synchronous task executor (celery based)

    :param user_and_tenant: list
    :param task: task object or path to a task 'tasks.x.y.TaskClass'
    :param params: dict - task parameters
    :param context: dict - task context
    :return: any
    """

    if isinstance(task, basestring):
        task = load_task(task)

    task = task(params=params)
    task_pool = context.get('task_pool', None)
    wait_for_pool = context.get('wait_for_pool', None)
    t = tracer.trace
    if current_task:
        logger.info('using the request id %s ' % current_task.request.id)
    else:
        logger.info('current task is none %s' % t)

    trace_id = current_task.request.id if current_task else t
    task_info = {'path': task.path,
                 'params': params,
                 'force': context.pop('force', False),
                 'trace': trace_id,
                 'context': context,
                 'task_pool': task_pool}
    job = asynctasks.subtask_handler(run_master_task,
                                     args=[user_and_tenant],
                                     kwargs=task_info,
                                     queue=tasks_queue_name('tasks', task_pool, DEBUG))

    res_id = job.get()["value"]
    return res_id

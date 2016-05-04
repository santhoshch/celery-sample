'''
Created on 02-May-2016

@author: ravi
'''
import celery
import importlib
from celerytasks import gnana_task_support
from celery import current_task
import json
import logging
# from celery_sample.settings import gnana_db
# from domainmodel.app import TaskCache
# from gnana.framework.tasker.cache import TasksCache
# from gnana.framework.tasker.task_utils import TaskStatusHelper
from collections import defaultdict

logger = logging.getLogger('gnana.%s' % __name__)


def load_task(path):
    """
    dynamically loads a class object by path: 'tasks.reports.generators.TaskName'
    """
    class_data = path.rsplit(".", 1)
    module_path = class_data[0]
    class_str = class_data[1]
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise Exception('Path does not exist: "{}" -- "{}"'.format(path, e))

    task = getattr(module, class_str, None)

    if not task:
        raise Exception('Task "{}" not found in "{}" module'.format(class_str, module_path))
    return task


@celery.task
def run_task(user_and_tenant, params, deps_keys, tinfo, **kwargs):
    return run_task_action(user_and_tenant, params, deps_keys, tinfo, **kwargs)


@gnana_task_support
def run_task_action(user_and_tenant, params, deps_keys, tinfo, **kwargs):
    """
    Task executor. Execute a single task

    :param user_and_tenant:
    :param params:
    :param deps_keys:
    :param tinfo:
    :param kwargs:
    :return:
    """

    from celerytasks.tasker.managers import CeleryTaskManager
    manager = CeleryTaskManager()
    context = tinfo.pop('context', {})
    path = tinfo.pop('path')
    res_id = tinfo.pop('res_id')
    task = load_task(path)(params=params, context=context)
    task._res_id = res_id
    task.task_id = tinfo.pop('task_id')
    task.parent_ids = tinfo.get('parent_tasks')
    return manager.execute(task=task, deps_keys=deps_keys)


@celery.task
def run_master_task(user_and_tenant, *args, **kwargs):
    return run_master_task_action(user_and_tenant, *args, **kwargs)


@gnana_task_support
def run_master_task_action(user_and_tenant, *args, **kwargs):
    """
    Master task executor. Will register tasks and run task tree

    :param user_and_tenant:
    :param args:
    :param kwargs:
    :return:
    """
    from celerytasks.tasker.managers import CeleryTaskManager
    manager = CeleryTaskManager()
    path = kwargs.pop('path', None)
    params = kwargs.pop('params', {})
    context = kwargs.pop('context', {})
    context.update({'user_and_tenant': user_and_tenant})
    context['trace_id'] = kwargs.get('trace')
    context['profile'] = kwargs.get('profile')
    task = load_task(path)(params=params, context=context)
    if kwargs.pop('preview', False):
        get_metadata = kwargs.pop('get_metadata', False)
        res = json.dumps(manager.cache_preview(task=task, norender=True, get_metadata=get_metadata))
    else:
        task.parent_ids = [current_task.request.id]  # hack for integration with current infrastructure
        res = manager.run(task=task)
    return {'value': res, 'type': 'json'}

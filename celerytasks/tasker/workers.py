import threading
import traceback
import sys
from utils import tasks_queue_name
from celery_sample import sec_context
from celery_sample.settings import DEBUG
from celerytasks.tasker.gtasks import run_task
import logging
import time
import uuid
from celery.result import AsyncResult

logger = logging.getLogger('gnana.%s' % __name__)


class BaseWorker(object):

    """
    Base worker class
    """

    def get(self):
        raise NotImplementedError('Must be implemented in a subclass')

    def is_ready(self):
        raise NotImplementedError('Must be implemented in a subclass')


class LocalWorker(threading.Thread, BaseWorker):

    """
    Local worker. Based on threading library. Useful for in-notebook task execution
    """
    job = None

    def __init__(self, *args, **kwargs):
        super(LocalWorker, self).__init__(*args, **kwargs)
        self.value = {}
        self.error = None
        self.status = 0
        try:
            self.taskobj = kwargs['kwargs']['task']
        except:
            self.taskobj = None
        logger.warn('task res id %s' % (self.get_res_id()))

    def get_res_id(self):
        return self.taskobj.res_id if self.taskobj else None

    def run(self):
        self.job = str(uuid.uuid4())
        if self._Thread__target is not None:
            try:
                self.value = {
                    'res': self._Thread__target(*self._Thread__args, **self._Thread__kwargs)}
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.value = {'res': None}
                self.error = traceback.format_exception(
                    exc_type, exc_value, exc_traceback)
        self.status = 1

    def get(self, *args, **kwargs):
        super(LocalWorker, self).join(*args, **kwargs)
        while self.job is None:
            time.sleep(1.0)
            logger.info("Waiting for worker to start task with res_id %s" %
                        (self.get_res_id()))
        if self.error:
            raise Exception(self.error)
        while self.status != 1:
            time.sleep(1)
            logger.warn('waiting for status to be set to complete [1]')
        # return self.job
        return self.value.get('res', None)

    def is_ready(self):
        return self.value != {}

    def get_id(self):
        return self.job

    def get_status(self):
        return self.status


class CeleryWorker(BaseWorker):

    """
    Celery worker. Based on Celery. Multi-server task execution
    """
    job = None
    taskobj = None

    def start(self, task, deps_keys):
        self.taskobj = task
        if task.parent_task:
            parent_task = [t_.task_id for t_ in task.parent_task]
        else:
            parent_task = task.parent_id if task.parent_id else None
        tinfo = {'path': task.path,
                 'task_id': task.task_id,
                 'parent_task': parent_task,
                 'context': task.context,
                 'res_id': task.res_id}
        task_pool = task.context.get('task_pool', None)
        if "v2task_type" in task.params.keys():
            if task_pool and task_pool != 'None':
                task_pool = task_pool + '_' + str(task.params['v2task_type'])
            else:
                task_pool = 'tasks-pool' + '_' + str(task.params['v2task_type'])
        user = task.context.get('tenant_and_user', None)
        profile = task.context.get('profile', None)
        kwargs = {'profile': profile}
        if not user:
            user = [sec_context.user_name, sec_context.name,
                    sec_context.login_tenant_name]
        logger.info('celery worker to submit to run task: path: %s | task_id %s | parent_task: %s | res_id: %s',
                    task.path, task.task_id, parent_task, task.res_id)
        queue_name = tasks_queue_name('tasks', task_pool, DEBUG)
        self.job = run_task.apply_async(args=[user, task.params, deps_keys, tinfo], kwargs=kwargs,
                                        queue=queue_name)
        return self.job.id

    def get(self, *args, **kwargs):
        while self.job is None:
            time.sleep(1.0)
            logger.info("Waiting for worker to start task %s %s" %
                        (self.taskobj.task_id, self.taskobj.res_id))
        try:
            timer = 0
            while not self.job.ready():
                timer += 1
                time.sleep(1)
                if timer % 60 == 0:
                    logger.info('Worker waiting for the job %s seconds task %s \
                                         - res id %s' % (timer,
                                                         self.taskobj.task_id,
                                                         self.taskobj.res_id))
            return self.job.get()
        except:
            logger.exception('ERROR: %s | %s failed to execute' % (self.taskobj.name, self.taskobj.res_id))

    def is_ready(self):
        return self.job.ready()

    def get_status(self):
        celery_result = AsyncResult(self.job.id)
        return celery_result.status

    def get_id(self, **_):
        return self.job.id

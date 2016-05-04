from collections import defaultdict
from copy import deepcopy
import logging
import sys
from time import sleep
import time
import traceback

from celery import current_task
import celery
from celery.result import AsyncResult

from celery_sample import sec_context
from celery_sample.settings import gnana_db
from celerytasks.tasker.cache import TasksCache
from celerytasks.tasker.workers import CeleryWorker, LocalWorker
from domainmodel.app import Task as TaskStatus
from taskframework.framework import V2Task
from utils import dateUtils
# from utils.notebook import plot_tasks_tree
store = {}
tasks = {}

logger = logging.getLogger('gnana.%s' % __name__)


def get_celery_id():
    try:
        return current_task.request.id
    except:
        return None


class RunWorker(object):

    def join(self, timeout=60.0):
        # Not using threads,we do not have to wait
        pass

    def start(self):
        self.run()

    @staticmethod
    def is_alive():
        return True

    def __init__(self,
                 inst,
                 task,
                 deps_workers,
                 weight
                 ):
        self.inst = inst
        self.task = task
        self.deps_workers = deps_workers
        self.weight = weight
        self.status = 'CREATED'

    def run(self):
        self.status = 'STARTED'
        info = "Getting sub workers for task %s with id %s and res_id %s"
        logger.info(info % (self.task.name,
                            self.task.task_id,
                            self.task.res_id))
        for key, dep in self.deps_workers.iteritems():
            info = "Getting sub worker %s for task name: %s | task_id: %s and res_id %s"
            logger.info(info % (dep['res_id'],
                                self.task.name,
                                self.task.task_id,
                                self.task.res_id))
            dep['async_id'].get()
            info = "Got sub worker %s for task name: %s | task_id: %s and res_id %s"
            logger.info(info % (dep['res_id'],
                                self.task.name,
                                self.task.task_id,
                                self.task.res_id))
        info = "Got sub workers for task name: %s | task_id %s and res_id %s"
        logger.info(info % (self.task.name,
                            self.task.task_id,
                            self.task.res_id))
        if self.task.trace_id in self.inst.wm.workers:
            self.status = 'SUBMITTED'
            self.worker = self.inst.run_worker(
                self.task, self.deps_workers, weight=self.weight)
        else:
            logging.info('master is closed.. . returning task id %s - res id %s ' % (self.task.task_id,
                                                                                     self.task.res_id))

    def get(self, master=False):
        timer = 0
        is_alive = True
        while is_alive:
            # self.join(timeout=60.0)
            # The following sleep is needed for local workers
            timer += 1
            is_alive = RunWorker.is_alive()
#             t = self.inst.get_task(self.task)
            worker_available = getattr(self, 'worker', None) is not None
#             cache_exists = self.inst.cache_exists(self.task.res_id)
            if is_alive:
                if timer % 60 == 0:
                    logger.info("Task %s with id %s and res_id %s has been alive for %s minutes \
                                worker_status is %s - RunWorker status - %s " % (self.task.name,
                                                                                 self.task.task_id,
                                                                                 self.task.res_id,
                                                                                 timer,
                                                                                 worker_available,
                                                                                 self.status))
            if worker_available:
                is_alive = False
            else:
                is_alive = True
                sleep(1)
        logger.info("************* Task %s with id %s and res_id %s has \
                                    worker status %s" % (self.task.name,
                                                         self.task.task_id,
                                                         self.task.res_id,
                                                         self.status,))
        if master:
            msg = "All the v2tasks are submitted for execution.."
            msg += "master is waiting for all the dependencies to complete"
            logger.info(msg)
            return self.worker.get()
        else:
            return self.worker.get_id()


class WorkerManager(object):

    def __init__(self):
        self.workers = {}

    def memory_usage_resource(self):
        import resource
        rusage_denom = 1024.
        if sys.platform == 'darwin':
            # ... it seems that in OSX the output is different units ...
            rusage_denom *= rusage_denom
        self.mem = resource.getrusage(
            resource.RUSAGE_SELF).ru_maxrss / rusage_denom
        return self.mem

    def refresh(self):
        inspector = celery.current_app.control.inspect()
        try:
            logger.info({'active': [(k, len(v)) for k, v in inspector.active().iteritems() if len(v)],
                         'reserved': [(k, len(v)) for k, v in inspector.reserved().iteritems() if len(v)],
                         'memory': self.memory_usage_resource(),
                         })
        except:
            logger.warning("NO WORKER INFO")
            pass


class BaseTaskManager(object):

    """
    Base Task Manager class.
    """
    cache = TasksCache()
    wm = WorkerManager()
    my_worker = None
    graph = {}
    visited_path = set()

    def run(self, task, master=True, preview=False):
        """
        This is the main entry point for TaskManager. It is called recursively to
        run the dependencies
        :param task: task object
        :param master: bool - if true will return lock process and wait for an output of 'task.execute' function
        :param preview: bool - if true will not run, just preview
        :return:
        """
        try:
            if master:
                self.wm.workers[task.trace_id] = {}
                self.deps_cache = {}
                self.register_dependencies(task)
                info = {k: v.__class__.__name__ for k, v in self.deps_cache.iteritems()}
                logger.info("Registering deps_cache tasks %s " % info)
                for task_ in self.deps_cache:
                    self.on_register(self.deps_cache[task_])
                if preview:
                    return self.deps_cache
            reduced_context = {k: v for k, v in task.context.iteritems() if k not in ['dataset_config',
                                                                                      'raw_dataset_config']}
            logger.info('preparing to run: %s | task_id=%s | res_id=%s | trace_id=%s | params=%s | context=%s',
                        task.name, task.task_id, task.res_id, task.trace_id, task.variables, reduced_context)
            workers = defaultdict(dict)
            profile = task.context.get('profile')
            rdsc = task.context.get('raw_dataset_config')
            dsc = task.context.get('dataset_config')
            if not self.cache_exists(task.res_id):
                for subname, subtask in task.dependencies.items():
                    subtask.context.update({'profile': profile,
                                            'raw_dataset_config': rdsc,
                                            'dataset_config': dsc})
                    workers[subname]['async_id'] = self.run(
                        subtask, master=False)
                    workers[subname]['res_id'] = subtask.res_id
                    workers[subname]['name'] = subtask.name
                    workers[subname]['task_id'] = subtask.task_id
            else:
                logger.info("Task is already cached not including : %s | task_id=%s | res_id=%s | trace_id=%s ",
                            task.name, task.task_id, task.res_id, task.trace_id)
            deps_workers = workers
            if task.res_id in self.wm.workers[task.trace_id].keys():
                res = self.wm.workers[task.trace_id][task.res_id]["res"]
                logger.info(
                    "Worker already exists for : " + task.name + " " + task.res_id)
            else:
                logger.info("authorizing worker: %s | task_id=%s | res_id=%s | trace_id=%s ",
                            task.name, task.task_id, task.res_id, task.trace_id)
                weight = self.get_authorization(task)
                res = RunWorker(
                    self, task, deps_workers, weight)
                self.wm.workers[task.trace_id][task.res_id] = {}
                self.wm.workers[task.trace_id][task.res_id]["res"] = res
                res.start()
                logger.info('worker running for: %s | task_id=%s | res_id=%s | trace_id=%s | params=%s | context=%s',
                            task.name, task.task_id, task.res_id, task.trace_id, task.variables, reduced_context)
            result = res
            if master:
                val = res.get(master=master)
                self.wm.refresh()
                return val
            return result
        except Exception as e:
            logger.exception(
                'failed %s.execute(%s): %s', task.name, task.variables, e)
            try:
                self.on_fail(task, e)
            except:
                pass
            raise Exception('%s~%s' % (task.name, e))
        finally:
            if master and not preview:
                try:
                    self.clean_dependencies(task)
                    t = self.get_task(task)
                    if t.status is TaskStatus.STATUS_ERROR:
                        logger.info("Summary of errors is:")
                        tasks_effected, failed_tasks = BaseTaskManager.get_errors(t)
                        for failed_task in failed_tasks:
                            items = (failed_tasks[failed_task]['task_name'],
                                     failed_tasks[failed_task]['res_id'],
                                     failed_tasks[failed_task]['parent_id'],
                                     failed_tasks[failed_task]['msg'])
                            logger.error('ERROR: %s | res_id= %s |parent_id = %s |failed with Error: %s' % items)
                        logger.error('ERROR: Effected parent tasks because of failure: %s' % tasks_effected)
                except:
                    pass
                del self.wm.workers[task.trace_id]

    def get_authorization(self, task, check=True):
        if check:
            if "task_weight" not in task.params.keys():
                return 0
            else:
                task_weight = task.params["task_weight"]
                timeout = 1
                count = 0
                weight = None
                waiting_time = timeout * 5.0
                while count < timeout:
                    count += 1
                    weight = 0
                    for trace_id in self.wm.workers.keys():
                        for res_id in self.wm.workers[trace_id].keys():
                            if "status" in self.wm.workers[trace_id][res_id].keys():
                                if self.wm.workers[trace_id][res_id]["status"] == 1:
                                    if self.cache_exists(res_id):
                                        self.wm.workers[trace_id][
                                            res_id]["status"] = 0
                                    else:
                                        weight += self.wm.workers[
                                            trace_id][res_id]["weight"]
                    if weight > 1 - task_weight:
                        sleep(waiting_time)
                    else:
                        return task_weight
                logger.info('waited for %s seconds for tree to get lighter | current weight: %s | task weight: %s',
                            waiting_time,
                            str(weight),
                            str(task_weight))
                return task_weight
        return 0

    def run_worker(self, task, deps_keys, weight=0):
        """
        Instantiate a worker and start the execution  of the task on it
        Worker must implement 'workers.BaseWorker' interface
        :param task: task
        :param deps_keys: deps
        :param weight: weight
        :return: the worker object that has been started
        """
        raise NotImplementedError('')

    def check_cycle(self, vertex_to_add):
        visited = set()

        def visit(vertex):
            if vertex in visited:
                return False
            visited.add(vertex)
            self.visited_path.add(vertex)
            for neighbour in self.graph.get(vertex, ()):
                if neighbour in self.visited_path or visit(neighbour):
                    return True
            self.visited_path.remove(vertex)
            return False
        if vertex_to_add:
            for i in vertex_to_add:
                if i in self.graph:
                    for j in vertex_to_add[i]:
                        self.graph[i].append(j)
                else:
                    self.graph[i] = vertex_to_add[i]
            return any(visit(v) for v in vertex_to_add)

    def register_dependencies(self, task):
        """
        Register dependencies in tracking system. also takes care of any
        cache cleaning if context has force=True
        :param task:
        :return:
        """
        if not isinstance(task, V2Task):
            logger.info('returning... taskss.')
            return
        if not task.res_id:
            task.register_res_id()
        message = "Task Info: %s | %s | %s with subtasks:\n%s"
        subtasks_message = map(lambda dd: "%s | %s | %s | %s" % (dd.name,
                                                                 id(dd),
                                                                 dd.task_id,
                                                                 dd.res_id),
                               task.dependencies.values())
        subtasks_message = '\n'.join(subtasks_message)
        logger.info(message % (id(task),
                               task.task_id,
                               task.res_id,
                               subtasks_message))
        if task.context.get("force", False) or task.params.get("force_process", False):
            try:
                if self.cache.exists(task.res_id):
                    logger.info("force=True: removing %s cache with res_id=%s for task_id=%s",
                                task.name, task.res_id, task.task_id)
                    self.cache.remove(task.res_id)
            except Exception as e:
                logger.exception(e)
                logger.warning(
                    "WARNING: questionable cleaning for res_id=%s", task.res_id)

        already = task.res_id in self.deps_cache
        if not already:
            deps = task.dependencies.values()
            add_vertex = {task.name: [d.name for d in deps]}
            cycle = self.check_cycle(add_vertex)
            if cycle:
                msg = 'task_node %s results in cycle in the graph and cyclic path is %s'
                logger.exception(msg % (task.name, self.visited_path))
                raise Exception("cycle detected in graph")
            self.deps_cache[task.res_id] = task
            if self.cache_exists(task.res_id):
                return
            deps_names = map(
                lambda dd: filter(lambda x: x.isupper(), dd.name), deps)
            composition = {}
            for name in deps_names:
                try:
                    composition[name] += 1
                except:
                    composition[name] = 1
            logger.info("Task Node: %s | %s | %s | %s" % (task.name,
                                                          composition,
                                                          task.task_id,
                                                          task.res_id))
            deps.sort()
            for sub_idx, subtask in enumerate(deps):
                if not subtask.res_id:
                    subtask.register_res_id()
                if subtask.res_id in self.deps_cache:
                    self.deps_cache[subtask.res_id].parent_task.append(task)
                    self.deps_cache[subtask.res_id].parent_id.append(
                        task.task_id)
                    self.subtree_sync(subtask)
                else:
                    subtask.parent_task = [task]
                    subtask.parent_id = [task.task_id]
                    subtask.trace_id = task.trace_id
                    subtask.update_context(task.context)
                    self.register_dependencies(subtask)
        else:
            logger.info("Task RESID is already in deps cache : %s | %s | %s" % (task.name,
                                                                                task.task_id,
                                                                                task.res_id))

    def subtree_sync(self, subtask):
        if not subtask.res_id:
            subtask.register_res_id()
        try:
            subtask.task_id = self.deps_cache[subtask.res_id].task_id
            subtask.parent_id = self.deps_cache[subtask.res_id].parent_id
            subtask.trace_id = self.deps_cache[subtask.res_id].trace_id
            subtask.parent_task = self.deps_cache[subtask.res_id].parent_task
            for dep in subtask.dependencies.values():
                self.subtree_sync(dep)
        except:
            pass

    def execute(self, task, deps_keys):
        """
        Task executor. We assume by this point all dependencies are complete and
        the only thing left is to use them to complete the task itself. However,
        since we do not want to repeat work, we will only actually do work if
        there is not result (and will block if there is already a task running with
        the same res_id)

        :param task: task object
        :param deps_keys: deps
        :return: res_id of current task
        """
        failed = False
        try:
            logger.info('Going to start execute for: %s | task_id=%s | res_id=%s | trace_id=%s',
                        task.name, task.task_id, task.res_id, task.trace_id)
            self.on_start(task)
            inputs = {}
            cached_res = {}
            if self.cache_exists(task.res_id):
                logger.info(
                    '%s with res_id=%s is already cached', task.name, task.res_id)
                self.on_success(task, did_work=False)
                return task.res_id
            task.tmanager = self
            deps = []
            failed_task_message = ''
            if hasattr(task, 'kill_siblings'):
                kill_siblings_on_fail = task.kill_siblings
            else:
                kill_siblings_on_fail = False
            logger.info('Going to get dependencies outputs for: %s | task_id=%s | res_id=%s | trace_id=%s',
                        task.name, task.task_id, task.res_id, task.trace_id)
            for key in deps_keys:
                dep_key = key
                async_id = deps_keys[key]['async_id']
                res_id = deps_keys[key]['res_id']
                task_id = deps_keys[key]['task_id']
                logger.info(' current res_id %s ' % res_id)
                result = (dep_key, async_id, res_id, task_id)
                deps.append(result)
            timer = 0
            while deps:
                timer += 1
                remaining_deps = deepcopy(deps)
                res_id = None
                for dep in remaining_deps:
                    try:
                        async_id = dep[1]
                        res_id = dep[2]
                        if self.cache_exists(res_id):
                            cached_res[dep[0]] = res_id
                            message = 'dependency exists in cache for: %s '
                            message += '| task_id=%s | res_id=%s | trace_id=%s | child_res_id=%s'
                            logger.info(message,
                                        task.name, task.task_id, task.res_id, task.trace_id, res_id)
                            deps.remove(dep)
                        elif async_id and self.is_ready(async_id):
                                logger.warn("dependency is not found in cache but its status is ready, not expecting.. \
                                 | task_name=%s | task_id=%s | res_id=%s | trace_id=%s | child_res_id=%s " % (task.name,
                                                                                                              task.task_id,
                                                                                                              task.res_id,
                                                                                                              task.trace_id,
                                                                                                              res_id))
                                self.get_result(async_id)
                                deps.remove(dep)
                        else:
                            if timer % 60 == 0:
                                logger.info(
                                    'cache or worker not ready  Waiting for  %s seconds  | task_name=%s | task_id=%s | \
                                    res_id=%s | trace_id=%s | child_res_id=%s ' % (timer,
                                                                                   task.name,
                                                                                   task.task_id,
                                                                                   task.res_id,
                                                                                   task.trace_id,
                                                                                   res_id))
                    except:
                        failed = True
                        failed_task_message = (task.name + '| task_id=' + task.task_id +
                                               ' |res_id=' + task.res_id + ' | trace_id=' + task.trace_id)
                        failed_task_message += ' detected error for dependency: ' + dep[0] + ' with res_id= ' + res_id
                        deps.remove(dep)
                        if kill_siblings_on_fail:
                            break
                        pass
                time.sleep(1)
            if kill_siblings_on_fail:
                for dep in deps:
                    dep_task = dep[1]
                    dep_taskid = dep[3]
                    t = TaskStatus.getBySpecifiedCriteria(
                        {'object.extid': dep_taskid})
                    t.status = TaskStatus.STATUS_ERROR
                    t.current_status_msg = 'terminated ' + failed_task_message
                    t.save()
                    dep_task.revoke(terminate=True, signal='SIGKILL')
            if failed:
                raise Exception(
                    'Failures detected in subtasks:\n  ' + failed_task_message)
            if cached_res:
                for key in cached_res:
                    inputs[key] = self.check_if_cache_load(cached_res[key], task)
            logger.info('execute: %s | task_id=%s | res_id=%s | trace_id=%s',
                        task.name, task.task_id, task.res_id, task.trace_id)
            out = task.execute(inputs)
            if not self.cache_put(task.res_id, out, metadata=task.metadata, overwrite=True, lifetime=task.lifetime):
                logger.warning(
                    "cache_put returned False for res_id=%s", task.res_id)
            self.on_success(task, did_work=True)
            logger.info('saving cache for %s | task_id=%s | res_id=%s | trace_id=%s',
                        task.name, task.task_id, task.res_id, task.trace_id)
            return task.res_id
        except Exception as e:
            self.on_fail(task, e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            if failed:
                logger.error('ERROR: failure in %s | task_id=%s | res_id=%s | trace_id=%s | %s',
                             task.name, task.task_id, task.res_id, task.trace_id, ''.join(error).strip())
            else:
                logger.exception('ERROR: failure in %s | task_id=%s | res_id=%s | trace_id=%s | %s',
                                 task.name, task.task_id, task.res_id, task.trace_id, ''.join(error).strip())
            raise Exception('%s | %s | %s ' %
                            (task.name, task.task_id, e))

    def check_if_cache_load(self, res_id, task):
        return self.load_cache(res_id)

    def load_cache(self, res_id):
        obj = self.cache_get(res_id)
        return obj

    def clean_dependencies(self, task):
        """
        After you get result for top level task, go through and make sure
        all dependencies are marked as being done (or not needing to be run).
        :param task: task
        """
        self.on_cleanup(task)
        for subtask in task.dependencies.values():
            if isinstance(subtask, V2Task):
                self.clean_dependencies(subtask)

    @staticmethod
    def get_errors(t):
        """
        Go through all dependencies to get the summary of errors
        :param t: task
        """
        c1 = {'object.main_id': str(t.main_id)}
        c2 = {'object.framework_version': '2'}
        c3 = {'object.status': 3}
        criteria = {'$and': [c1, c2, c3]}
        failed_tasks = defaultdict(dict)
        res = gnana_db.findDocuments(TaskStatus.getCollectionName(), criteria)
        parent_id_list = []
        for i in res:
            msg = i['object']['current_status_msg']
            res_id = i['object']['res_id']
            taskid = str(i['object']['extid'])
            tasktype = str(i['object']['tasktype'])
            task_name = tasktype.split('.')[-1]
            failed_tasks[taskid]['task_name'] = task_name
            failed_tasks[taskid]['res_id'] = res_id
            parentid = i['object']['parent_id']
            if parentid:
                parent_id_list = parent_id_list + parentid
            failed_tasks[taskid]['parent_id'] = parentid
            failed_tasks[taskid]['msg'] = msg
        parent_id_list = set(parent_id_list)
        failed_tasks_copy = failed_tasks.keys()
        for taskid in failed_tasks_copy:
            if taskid in parent_id_list:
                failed_tasks.pop(taskid)
        return parent_id_list, failed_tasks

    def cache_preview(self, task, mini=False, norender=False, get_metadata=False):
        """
        Render a preview of a task tree

        :param task: Task object
        :param mini:
        :param norender: render
        :param get_metadata: metadata
        :return: an image
        """
        deps_cache = self.run(task, master=True, preview=True)
        tasks_ = []
        for t in deps_cache.values():
            task_info = {'task_id': t.task_id,
                         'parent_id': t.parent_id,
                         'path': t.path,
                         'name': t.name,
                         'res_id': t.res_id,
                         'in_cache': self.cache_exists(t.res_id)}
            if get_metadata:
                task_info['metadata'] = t.task_meta
            tasks_.append(task_info)
        if norender:
            return tasks_
        return tasks_
#         return plot_tasks_tree(task, mini=mini)

    def on_register(self, task):
        """
        callback for when we are about to register a task and its dependencies
        :param task: task
        """
        raise NotImplementedError('')

    def on_submit(self, task, celery_id):
        """
        callback for when task is ready to be executed (i.e. all it's dependencies
        have been successfully ran and it is submitted)
        :param task: task
        :param celery_id: celery ID
        """
        raise NotImplementedError('')

    def on_start(self, task):
        """
        callback when task finally has permission to start running
        :param task: task
        """
        raise NotImplementedError('')

    def on_success(self, task, did_work):
        """
        callback when task is stopped (either successful completion or failure)
        :param task: task
        :param did_work: did work
        """
        raise NotImplementedError('')

    def on_cleanup(self, task):
        """
        callback for cleaning up tasks which did not need to be run for some
        reason or another
        :param task: task
        """
        raise NotImplementedError('')

    def on_progress(self, task, percent):
        """
        callback for progress during execution
        :param task: task
        :param percent: progression percentage
        """
        raise NotImplementedError('')

    def on_fail(self, task, error):
        """
        callback for when there is a failure
        :param task: task
        :param error: error
        """
        raise NotImplementedError('')

    def cache_get(self, key):
        """
        Get data from cache
        :param key: key
        """
        raise NotImplementedError('')

    def cache_put(self, key, value, metadata=None, overwrite=False, lifetime=V2Task.LIFETIME_SHORT):
        raise NotImplementedError('')

    def cache_exists(self, key):
        """
        Check if key in cache
        :param key: key
        """
        raise NotImplementedError('')


class CeleryTaskManager(BaseTaskManager):

    """
    Celery task mananger. Celery-based workers + MongoDB, S3 cache
    """

    def on_register(self, task):
        user, tenant, domain = sec_context.peek_context()
        t = TaskStatus()
        t.framework_version = "2"
        t.status = TaskStatus.STATUS_CREATED
        t.type = task.path
        t.createdby = '{}@{}'.format(user, domain)
        if 'trace' in task.context:
            t.main_id = t.get_mainid(task.context['trace'])
        elif 'trace_id' in task.context:
            t.main_id = t.get_mainid(task.context['trace_id'])
        t.res_id = task.res_id
        t.extid = task.task_id
        t.task_meta = {'params': task.params,
                       'context': task.context,
                       'lifetime': task.lifetime}
        task.task_meta = t.task_meta
        t.trace = task.trace_id
        t.tenant = tenant
        t.perc = 0.0
        if task.parent_task:
            t.parent_id = [t_.task_id for t_ in task.parent_task]
        elif task.parent_id:
            t.parent_id = task.parent_id
        else:
            t.parent_id = None
        logger.info("*** - registering %s, %s, %s",
                    task.name, task.res_id, task.task_id)
        t.save()

    def on_submit(self, task, celery_id):
        t = self.get_task(task)
        if t is None:
            error = "Could not load task: %s" % task.task_id
            raise Exception(error)
        t.status = TaskStatus.STATUS_SUBMITTED
        t.celery_id = celery_id
        t.submit_time = dateUtils.now()
        t.save()

    def on_start(self, task):
        t = self.get_task(task)
        if t.status == TaskStatus.STATUS_ERROR:
            raise Exception(
                "on_start found error status: %s", t.current_status_msg)
        if t.status != TaskStatus.STATUS_STARTED:
            t.status = TaskStatus.STATUS_STARTED
            t.res_id = task.res_id
            t.start_time = dateUtils.now()
            t.save()

    def on_success(self, task, did_work):
        t = self.get_task(task)
        t.status = TaskStatus.STATUS_FINISHED
        t.perc = 1.0
        t.did_work = did_work
        t.stop_time = dateUtils.now()
        t.save()
        logger.info("*** - success %s, %s, %s",
                    task.name, task.res_id, task.task_id)

    def on_fail(self, task, error):
        t = self.get_task(task)
        t.status = TaskStatus.STATUS_ERROR
        t.current_status_msg = str(error)
        t.stop_time = dateUtils.now()
        t.save()
        logger.info("*** - failed %s, %s, %s, %s",
                    task.name, task.res_id, task.task_id, error)

    def on_cleanup(self, task):
        t = self.get_task(task)
        if t.status in [TaskStatus.STATUS_CREATED,
                        TaskStatus.STATUS_SUBMITTED,
                        TaskStatus.STATUS_STARTED]:
            t.status = TaskStatus.STATUS_FINISHED
            t.perc = 1.0
            t.did_work = False
            t.stop_time = dateUtils.now()
            t.save()
        logger.info("*** - cleanup %s, %s, %s",
                    task.name, task.res_id, task.task_id)

    def on_progress(self, task, percent):
        percent = float(percent)
        if percent < 0:
            return
        t = CeleryTaskManager.get_task(task)
        t.perc = percent if percent <= 1 else (
            min(float(int(percent)), 100.0) / 100)
        t.save()

    @staticmethod
    def get_task(task):
        return TaskStatus.getBySpecifiedCriteria({'object.extid': task.task_id})

    def cache_put(self, key, value, metadata=None, overwrite=False, lifetime=V2Task.LIFETIME_SHORT):
        return self.cache.put(key, value, metadata=metadata, overwrite=overwrite, lifetime=lifetime)

    def cache_get(self, key):
        return self.cache.get(key)

    def cache_exists(self, key):
        return self.cache.exists(key)

    ''' Send the key in case to check the status of the child key, else use the current worker job
    '''

    def is_ready(self, key=None):
        if key:
            res = AsyncResult(key)
            return res.ready()
        else:
            return self.my_worker.is_ready() if self.my_worker else False

    def get_result(self, key=None):
        timer = 0
        if key:
            res = AsyncResult(key)
            while not res.ready():
                timer += 1
                sleep(1)
                if timer % 60 == 0:
                    logger.info(
                        'waiting for result in get_result using key %s -  %s seconds' % (key, timer))
            return res.get()
        else:
            while getattr(self, 'my_worker', None) is None:
                timer += 1
                sleep(1)
                if timer % 60 == 0:
                    logger.info(
                        'waiting in get_result for the my worker object % secs ' % timer)
            return self.my_worker.get()

    def run_worker(self, task, deps_workers, weight=0):
        # run execute through celery task
        worker = CeleryWorker()
        self.my_worker = worker
        logger.info("Creating new worker for: %s %s" %
                    (task.name, task.res_id))
        n_workers = len(self.wm.workers[task.trace_id])
        message = "Number of workers created on trace_id %s after creation of worker for res_id %s: %s"
        logger.info(message % (task.trace_id,
                               task.res_id,
                               str(n_workers)))
        self.wm.workers[task.trace_id][task.res_id]["task"] = task
        self.wm.workers[task.trace_id][task.res_id]["weight"] = weight
        self.wm.workers[task.trace_id][task.res_id]["status"] = 1
        deps_keys = {k: CeleryTaskManager.get_key(
            w) for k, w in deps_workers.iteritems()}
        celery_id = worker.start(task, deps_keys)
        self.on_submit(task, celery_id)
        return worker

    @staticmethod
    def get_key(worker):
        key = {k: v for k, v in worker.iteritems() if k != 'async_id'}
        key['async_id'] = worker['async_id'].worker.get_id()
        return key


class TestTaskManager(BaseTaskManager):

    """
    Test task manager. Thread-based workers + in memory cache (dictionaries)
    """

    def on_register(self, task):
        t = TaskStatus()
        t.framework_version = "2"
        t.status = TaskStatus.STATUS_CREATED
        t.type = task.path
        t.main_id = task.trace_id
        t.res_id = task.res_id
        t.extid = task.task_id
        t.task_meta = {'params': task.params,
                       'context': task.context,
                       'lifetime': task.lifetime}
        t.trace = task.trace_id
        t.perc = 0.0
        t.parent_task = task.parent_id
        tasks[task.task_id] = t
        logger.info('-- {0.name}-{0.task_id} created'.format(task))

    def on_submit(self, task, celery_id):
        task_id = task.task_id
        tasks[task_id].status = V2Task.STATUS_SUBMITTED
        tasks[task_id].celery_id = celery_id
        tasks[task_id].submit_time = dateUtils.now()
        logger.info(
            '-- {0.name}-{0.task_id} submitted'.format(task))

    def on_start(self, task):
        task_id = task.task_id
        tasks[task_id].status = V2Task.STATUS_STARTED
        tasks[task_id].res_id = task.res_id
        tasks[task_id].start_time = dateUtils.now()
        logger.info('-- %s-%s started', task.name, task.task_id)

    def on_success(self, task, did_work):
        task_id = task.task_id
        tasks[task_id].status = V2Task.STATUS_FINISHED
        tasks[task_id].perc = 100
        tasks[task_id].did_work = did_work
        tasks[task_id].stop_time = dateUtils.now()
        logger.info(
            '--%s-%s success - stopped', task.name, task.task_id)

    def on_fail(self, task, error):
        tasks[task.task_id].status = V2Task.STATUS_ERROR
        tasks[task.task_id].stop_time = dateUtils.now()
        logger.info(
            '-- %s-%s failed: %s', task.name, task.task_id, error)

    def on_cleanup(self, task):
        logger.info(
            '-- %s-%s cleaning up... ', task.name, task.task_id)

    def on_progress(self, task, percent):
        tasks[task.task_id].perc = percent
        logger.info('-- %s-%s failed: %.1f%%',
                    task.name, task.task_id, percent)

    def cache_put(self, key, value, **kwargs):
        if (key not in store) and value is not None:
            store[key] = value
        return True

    def cache_get(self, key):
        return store.get(key, None)

    def cache_exists(self, key):
        return True if store.get(key, None) else False

    def run_worker(self, task, deps_workers, weight=0):
        deps_keys = {k: TestTaskManager.get_key(
            w) for k, w in deps_workers.iteritems()}
        # run execute function directly
        worker = LocalWorker(target=self.execute,
                             kwargs={'task': task, 'deps_keys': deps_keys})
        worker.start()
        return worker

    @staticmethod
    def get_task(task):
        return tasks[task.task_id]

    @staticmethod
    def get_key(worker):
        logger.info('TTM WORKER type %s - ' % worker)
        return CeleryTaskManager.get_key(worker)

    def is_ready(self, key=None):
        return self.cache_exists(key)

    def get_result(self, key=None):
        return self.cache_get(key)


class LocalTaskManager(CeleryTaskManager):

    """
    Local task manager. Thread-based workers + MongoDB, AWS S3 cache
    """

    @staticmethod
    def get_key(worker):
        logger.info('LTM WORKER type %s - ' % worker)
        return CeleryTaskManager.get_key(worker)

    def run_worker(self, task, deps_workers, weight=0):
        deps_keys = {k: LocalTaskManager.get_key(
            w) for k, w in deps_workers.iteritems()}
        # run execute function directly
        worker = LocalWorker(target=self.execute,
                             kwargs={'task': task, 'deps_keys': deps_keys})
        worker.start()
        return worker

    def get_result(self, key=None):
        return self.cache_get(key)

    def is_ready(self, key=None):
        if self.cache_get(key):
            return True
        else:
            return False

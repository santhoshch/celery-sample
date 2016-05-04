"""
Tasker is a task execution framework. Provides an ability to execute
"tasks" in multithreaded, multiserver, multiprocess environments.
Supports Celery, Thread-based workers.

__init__ - common code, base task
gtasks - celery tasks
models - models
workers - worker implementations
managers - task manager implementations
"""
import copy
import hashlib
import inspect
import logging
import os
from subprocess import Popen, PIPE
import uuid

from django.utils.functional import cached_property

from celery_sample.settings import CNAME, SITE_ROOT
logger = logging.getLogger(__name__)


def get_file_git_version(site_root, path=None):
    cmd = "git log -1 --pretty=format:%h --no-merges"
    if path:
        cmd += " {}".format(path)
    try:
        process = Popen(
            cmd, stdout=PIPE, cwd=site_root, shell=True, env=os.environ)
        output = process.communicate()[0]
        return output.strip()
    except Exception as e:
        logger.exception(e.message)
        return 'unknown'


class VersionManager(object):
    _versions = {}

    def get_class_version(self, inst):
        cls = inst.__class__
        try:
            return self._versions[cls]
        except KeyError:
            version = hashlib.sha1(VersionManager.get_source(inst)).hexdigest()
            self._versions[cls] = version
            return version

    @staticmethod
    def get_source(inst):
        return '\n'.join(
            [''.join(inspect.getsourcelines(f[1])[0])
                for f in inspect.getmembers(inst, predicate=inspect.ismethod)]
        )

VERSION_MANAGER = VersionManager()


def _make_hash(obj, debug=False):
    # memory optimized http://stackoverflow.com/a/8714242
    """
    make_hash({'x': 1, 'b': [3,1,2,'b'], 'c': {1:2}})
    make_hash({'x': 1, 'b': [3,1,2,'b'], 'c': {1:2}})
    make_hash({'x': 1, 'b': [3,1,2,'b']})
    make_hash({'x': 1, 'b': [3,1,2,'b']})
    make_hash({'x': 1, 'b': [3,1,2,'b']})
    """
    try:
        if isinstance(obj, (set, tuple, list)):
            if isinstance(obj, (set,)):
                obj = list(obj)
                obj.sort()
            return tuple(map(make_hash, obj))
        elif not isinstance(obj, dict):
            return hash(obj)
        try:
            new_o = copy.deepcopy(obj)
        except:
            if debug:
                logger.info('Deepcopy failing for: %s' % str(obj))
                import pickle
                pickle.dump(obj, open("deepcopy_failure.p", "wb"))
            raise
        nitems = new_o.items()
        nitems.sort(key=lambda x: x[0])
        for k, v in nitems:
            if v is not None:
                new_o[k] = make_hash(v)
            else:
                new_o[k] = make_hash("None")
        new_oo = new_o.items()
        new_oo.sort(key=lambda x: x[0])
        return hash(tuple(frozenset(new_oo)))
    except Exception as e:
        if not debug:
            _make_hash(obj, debug=True)
        raise Exception(e)


def make_hash(params):
    """
    Calculate sha1 of a dictionary

    :param params: dict - a dictionary
    :return: str - calculated hash id of params
    """
    return hashlib.sha1(str(_make_hash(params))).hexdigest()


class V2Task(object):

    """
    Base Task class

    :param params: dict - task parameters. You are not allowed to mess with params
        after creation.
    :param context: dict - task context

    trace_id - id shared between all members of a run tree
    task_id - globally unique id for a task
    dependencies - task dependencies (must be implemented as cached_property)
    res_id - globally unique id for a task result (multiple tasks may have same res_id)
    parent_task - parent task object
    parent_id - parent task id
    """
    STATUS_CREATED = 0
    STATUS_STARTED = 1
    STATUS_FINISHED = 2
    STATUS_ERROR = 3
    STATUS_SUBMITTED = 4

    LIFETIME_LONG = 'long'  # 4 months
    LIFETIME_MEDIUM = 'medium'  # 1 month
    LIFETIME_SHORT = 'short'  # 1 w

    def __init__(self, params=None, variables=None, context=None, **kw):
        self.tmanager = None
        self.parent_task = None
        self.parent_id = None
        self.lifetime = V2Task.LIFETIME_LONG
        self.task_id = str(uuid.uuid1())
        self.trace_id = str(uuid.uuid1())
        self.main_id = None
        self._name = self.__class__.__name__
        self._path = '{}.{}'.format(self.__module__, self.name)
        self._params = params if isinstance(params, dict) else {}
        self._variables = variables if isinstance(params, dict) else {}
        self._context = {}
        self._context = self.update_context(context)
        self.version = self._get_version()
        self._res_id = None
        self._kw = kw

    def register_res_id(self):
        self._res_id = self.generate_res_id()

    def generate_res_id(self):
        def mkhash(name, value):
            return make_hash({'name': name, 'value': value})
        k = [mkhash(self.name, self.params), self.version, CNAME]
        h = make_hash(k)
        return h

    @property
    def params(self):
        return self._params

    @property
    def variables(self):
        return self._variables

    @property
    def res_id(self):
        return self._res_id

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        return self._path

    @property
    def metadata(self):
        return {'name': self.name,
                'res_id': self.res_id,
                'version': self.version,
                'params': self.params}

    @property
    def context(self):
        return self._context

    @cached_property
    def dependencies(self):
        return {}

    def update_context(self, ctx):
        if ctx:
            self._context.update(ctx)
            if "trace" in ctx:
                self.trace_id = ctx["trace"]
            elif "trace_id" in ctx:
                self.trace_id = ctx["trace_id"]
            if "lifetime" in ctx:
                self.lifetime = ctx["lifetime"]
        return self._context

    def _get_version(self):
        if self.context.get('versioning', True):
            return 'native'
        try:
            return VERSION_MANAGER.get_class_version(self)
        except TypeError as e:
            logger.warning("Could not get file version for task: %s and class: %s, because of %s",
                           self.name, self.__class__.__name__, str(e))
            return 'native'

    def on_progress(self, percent):
        if self.tmanager:
            self.tmanager.on_progress(self, percent)

    def execute(self, dep_results, *args, **kwargs):
        raise NotImplementedError('Must be implemented in a child class')

from domainmodel import Model
import datetime
from Crypto.Hash import SHA512, SHA
from Crypto.PublicKey import RSA
from celery import current_task
import pytz
import logging
from Crypto.Signature import PKCS1_PSS
import base64
import time
import re
from utils import dateUtils


logger = logging.getLogger('gnana.%s' % __name__)


class Task(Model):

    """Stores information about a task"""
    CSV_RESULT = 1
    JSON_RESULT = 2
    BINARY_RESULT = 3
    mime_types = {CSV_RESULT: "application/csv",
                  JSON_RESULT: "application/json",
                  BINARY_RESULT: "application/binary"}
    collection_name = 'task'
    tenant_aware = False
    kind = 'domainmodel.task.Task'
    version = 1
    index_list = {
        'expires': {
            'expireAfterSeconds': 7200
        },
        'extid': {},
        'main_id': {},
        'res_id': {},
        'parent_id': {},
        'createdby': {},
        'tasktype': {},
        'created_datetime': {},
        'trace': {},
    }
    STATUS_CREATED = 0
    STATUS_STARTED = 1
    STATUS_FINISHED = 2
    STATUS_ERROR = 3
    STATUS_SUBMITTED = 4

    def __init__(self, attrs=None):
        self.type = None
        self.createdby = None  # Should be the ObjectID
        self.progress = -1
        self.steps = []
        self.current_step = None
        self.expire()
        self.extid = None
        self.status = None
        self.current_status_msg = ""
        # Stored as epoch, so we can sort on it easily.
        # datetime2epoch(dateUtils.now())
        self.created_datetime = dateUtils.now()
        self.task_meta = {}
        self.trace = None
        self.main_id = None
        self.res_id = None
        self.celery_id = None
        self.parent_id = []
        self.perc = 0.0
        self.tenant = None
        self.submit_time = None
        self.start_time = None  # when task started
        self.stop_time = None  # when task stopped (completed of failed)
        # did it have to do processing or was result already cached
        self.did_work = None
        # make sure to use string. new framework will be "2"
        self.framework_version = "1"
        super(Task, self).__init__(attrs)

    def encode(self, attrs):
        attrs['extid'] = self.extid
        if not self.type or not self.createdby:
            raise TaskError("Task type and created by or mandatory")
        attrs['tasktype'] = self.type
        attrs['createdby'] = self.createdby
        attrs['status'] = self.status
        attrs["expires"] = self.expires
        attrs['created_datetime'] = self.created_datetime
        attrs['current_status_msg'] = self.current_status_msg
        attrs['parent_id'] = self.parent_id
        attrs['perc'] = self.perc
        attrs['did_work'] = self.did_work
        attrs['main_id'] = self.main_id
        attrs['res_id'] = self.res_id
        attrs['celery_id'] = self.celery_id
        attrs['task_meta'] = self.task_meta
        attrs['tenant'] = self.tenant
        attrs['trace'] = self.trace
        attrs['submit_time'] = self.submit_time
        attrs['start_time'] = self.start_time
        attrs['stop_time'] = self.stop_time
        attrs['framework_version'] = str(self.framework_version)
        return super(Task, self).encode(attrs)

    def decode(self, attrs):
        self.type = attrs['tasktype']
        self.createdby = attrs["createdby"]
        self.extid = attrs["extid"]
        self.parent_id = attrs['parent_id']
        self.perc = attrs['perc']
        self.main_id = attrs['main_id']
        self.res_id = attrs.get('res_id', None)
        self.celery_id = attrs['celery_id']
        self.status = attrs.get("status", None)
        self.trace = attrs.get('trace', None)
        self.expires = attrs.get("expires", None)
        self.tenant = attrs.get('tenant', None)
        self.did_work = attrs.get('did_work', None)
        self.created_datetime = attrs.get('created_datetime', None)
        self.task_meta = attrs.get('task_meta', None)
        self.current_status_msg = attrs.get('current_status_msg', None)
        self.submit_time = attrs.get('submit_time', None)
        self.start_time = attrs.get('start_time', None)
        self.stop_time = attrs.get('stop_time', None)
        self.framework_version = attrs.get('framework_version', "1")
        return super(Task, self).decode(attrs)

    def expire(self):
        task_validity = 500
        self.expires = dateUtils.now() + datetime.timedelta(task_validity)

    @classmethod
    def updateCurrentStatus(cls, status_as_string, task_id=None):
        """ For a running task, updates a running comment on where we are.
        The 'current_status_msg' field is updated with whatever has been
        provided. Tasks such as dataload, answers, etc. can update what
        they are doing at various stages, so when the task status is queried
        we can get a hint of where we are. This is especially useful when
        running models for large datasets. For example, answers.py can
        update the task status as, "Processing records 500-600 of 25234", etc.
        The API has been added here, so other tasks can simply call it on Task.
        """
        task_id = task_id if task_id else current_task.request.id
        t = cls.getByFieldValue('extid', task_id)
        if t:
            try:
                t.current_status_msg = status_as_string
                t.save()
            except:
                # We are not going to stop the world because of this failure.
                pass

    @classmethod
    def getATask(cls, task_id=None):
        task_id = task_id if task_id else current_task.request.id
        return cls.getByFieldValue('extid', task_id)

    @classmethod
    def get_mainid(cls, trid):
        p = cls.getByFieldValue('trace', trid)
        return p.main_id

    @classmethod
    def set_task(cls, trid, mid, tid, pid, stat, di, na):
        p = cls.getByFieldValue('extid', tid)
        if not p:
            p = Task()
            q = cls.getByFieldValue('trace', trid)
            p.trace = trid
            p.main_id = mid
            p.extid = tid
            p.parent_id = pid
            if di:
                p.task_meta = di
            p.type = na
            p.createdby = q.createdby
            p.status = stat
            p.tenant = 'default_user'
        return p.save()

    @classmethod
    def get_progress(cls, mid):
        count = 0
        l = [i for i in cls.getAll({'object.main_id': mid})]

        for i in l:
            if i.status in (Task.STATUS_FINISHED, Task.STATUS_ERROR):
                count += 1

        ln = len(l)
        p = cls.getByFieldValue('extid', mid)
        if p is not None:
            if ln != 0:
                p.perc = float(count) / ln
            p.save()
            return int(p.perc * 100)


class TaskError(Exception):

    def __init__(self, error):
        self.error = error
# 
# 
# class Session(Model):
#     collection_name = 'session'
#     tenant_aware = False
#     kind = 'domainmodel.session.Session'
#     version = 1
#     index_list = {
#         'key': {'unique': True},
#         'expire_on': {
#             'expireAfterSeconds': 120   # 2 mins after expiration time
#         }
#     }
# 
#     def __init__(self, attrs=None):
#         self.config = {}
#         self.data = None
#         self.key = None
#         super(Session, self).__init__(attrs)
# 
#     def encode(self, attrs):
#         if self.key:
#             attrs['key'] = self.key
#         else:
#             raise SessionError('Missing session key')
#         if self.data:
#             attrs['data'] = self.data
#         attrs['expire_on'] = datetime.datetime.now(
#         ) + datetime.timedelta(minutes=30)
#         return super(Session, self).encode(attrs)
# 
#     def decode(self, attrs):
#         self.data = attrs.get('data')
#         self.key = attrs.get('key')
#         super(Session, self).decode(attrs)
# 
#     @classmethod
#     def getSessionByKey(cls, session_key):
#         return cls.getByFieldValue('key', session_key)

 
class TaskCache(Model):
    collection_name = 'task_cache'
 
    tenant_aware = True
    encrypted = False
 
    kind = 'domainmodel.app.TaskCache'
    version = 1
    index_list = {
        'expires': {
            'expireAfterSeconds': 7200
        },
        'extid': {}
    }
 
    def __init__(self, attrs=None):
        self.extid = None
        self.expires = None
        self.metadata = {}
        self.lifetime = 'short'
        super(TaskCache, self).__init__(attrs)
 
    def encode(self, attrs):
        attrs['extid'] = self.extid
        attrs['metadata'] = self.metadata
        attrs["expires"] = self.expires
        attrs['lifetime'] = self.lifetime
 
    def decode(self, attrs):
        self.extid = attrs['extid']
        self.metadata = attrs['metadata']
        self.lifetime = attrs.get('lifetime', 'short')
        if 'expires' in attrs:
            self.expires = attrs["expires"]
        else:
            self.expires = None
 
    @classmethod
    def getByKey(cls, extid):
        return cls.getByFieldValue('extid', extid)
 
    def expire(self):
        self.expires = dateUtils.now() + datetime.timedelta(2)
 
# 
# class SessionError(Exception):
# 
#     def __init__(self, error):
#         self.error = error

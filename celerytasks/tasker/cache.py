from cStringIO import StringIO
import logging
import os
import shutil
import tempfile
import zipfile
import sys
from os.path import basename

from domainmodel.app import TaskCache
from celery_sample.settings import gnana_storage, CNAME

logger = logging.getLogger(__name__)


__all__ = ['TasksCache']

__author__ = 'romangladkov'


class TemporaryDirectory(object):
    def __enter__(self):
        self.name = tempfile.mkdtemp()
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.name)


class TasksCache(object):
    """
    Tasks cache service.
    """
    s3_bucket = 'aviso-tasks-cache'
    expire = 60

    def _get_file(self, key, lifetime):
        from sklearn.externals import joblib
        logger.info('Loading {} from storage'.format(key))

        path = gnana_storage.get_file_path('/'.join([CNAME, lifetime, key]))
        tfile = gnana_storage.open(path, self.s3_bucket)

        with TemporaryDirectory() as d:
            with zipfile.ZipFile(tfile, 'r', compression=zipfile.ZIP_DEFLATED) as tzip:
                tzip.extractall(path=d)
            data = joblib.load('{}/{}.pkl'.format(d, key))

        logger.info('{} loaded from storage'.format(key))
        return data

    def _save_file(self, key, data, replace, lifetime):
        from sklearn.externals import joblib
        mem_size = sys.getsizeof(data)
        tfile = StringIO()
        with TemporaryDirectory() as d:
            files = joblib.dump(data, '{}/{}.pkl'.format(d, key), compress=0)
            with zipfile.ZipFile(tfile, 'w', compression=zipfile.ZIP_DEFLATED) as tzip:
                for f in files:
                    tzip.write(f, basename(f))
            filesize = tfile.tell() / 1024
            tfile.seek(0)

            logger.info('Saving {} to storage (size: {} kb, memory size: {} b)'.format(key, filesize, mem_size))
            if gnana_storage.add_file_object(tfile, '/'.join([CNAME, lifetime]),
                                             filename=key,
                                             replace=replace,
                                             bucket=self.s3_bucket):
                logger.info('{} saved to storage'.format(key))
                return True
        return False

    def _remove_file(self, key, lifetime):
        try:
            path = gnana_storage.get_file_path('/'.join([CNAME, lifetime, key]))
            gnana_storage.delete_adhoc_file(fullpath=path, bucket=self.s3_bucket)
            return True
        except Exception as e:
            logger.exception(e.message)
            logger.info('purging res_id ={} from task_cache'.format(key))

    def _get_object(self, key):
        obj = TaskCache.getByKey(key)
        if obj:
            return self._get_file(key, obj.lifetime)
        return None

    def _save_object(self, key, data, metadata, overwrite, lifetime):
        metadata = metadata or {}
        obj = TaskCache.getByKey(key)
        if obj and not overwrite:
            return True

        if not obj:
            obj = TaskCache()
            obj.extid = key
            obj.metadata = metadata
            obj.lifetime = lifetime
        if not self._save_file(key, data, overwrite, lifetime):
            return False
        obj.save()
        return self.exists(key)

    def remove(self, key):
        obj = TaskCache.getByKey(key)
        if obj:
            self._remove_file(key, obj.lifetime)
            obj._remove()
            return True
        return False

    def exists(self, key):
        """
        Check if key in cache
        :param key:
        :return:
        """
        if isinstance(key, dict):
            query = {'object.metadata.{}'.format(k): v for (k, v) in key.items()}
            obj = TaskCache.getBySpecifiedCriteria(query)
            if obj:
                return True
        else:
            obj = TaskCache.getByKey(key)
            if obj:
                return gnana_storage.if_exists(path='/'.join([CNAME, obj.lifetime, key]),
                                               bucket=self.s3_bucket)
        return False

    def get(self, key):
        """
        Get value from cache
        :param key:
        :return:
        """
        if isinstance(key, dict):
            return self.get_by_criteria(key)
        return self._get_object(key)

    def put(self, key, data, metadata=None, overwrite=False, lifetime='short'):
        """
        Put value to cache
        :param key:
        :param data:
        :param overwrite:
        :param lifetime:
        :param metadata:
        :return:
        """
        return self._save_object(key, data, metadata, overwrite, lifetime)

    def get_or_create(self, key, data, overwrite=False, lifetime='short'):
        """
        Get or create key
        :param key:
        :param data:
        :param overwrite:
        :param lifetime:
        :return:
        """
        obj = self._get_object(key)
        if obj:
            return obj
        return self.put(key, data, overwrite, lifetime)

    def _wipe(self):
        """
        Wipe the cache. Use at your own risk
        """
        deleted = {}
        for item in TaskCache.getAll():
            deleted[item.extid] = self.remove(item.extid)
        return deleted

    def get_by_criteria(self, criteria):
        query = {'object.metadata.{}'.format(k): v for (k, v) in criteria.items()}
        obj = TaskCache.getBySpecifiedCriteria(query)
        if obj:
            return self._get_file(obj.extid, obj.lifetime)
        return None

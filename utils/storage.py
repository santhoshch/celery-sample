'''
Created on 02-May-2016

@author: ravi
'''
import errno
import logging
import tempfile
import time
import ssl
import sys
import shutil
from shutil import copyfile
import os

import boto
import boto.s3.key

from utils import mkdir_p
from utils import FileInfo
# from gnana.framework import GnanaStorage, FileInfo
from celery_sample.settings import pathprefix

CNAME = 'Test'


logger = logging.getLogger(__name__)

class GnanaFileStorage(object):
    """
    An implementation of storage that simply utilizes the home directory.

    This can be in production if we ever use NTFS mounted directories.  S3
    is not necessarily safe due to eventual consistency.
    """

    def get_file_path(self, path):
        return '/'.join([pathprefix, path])

    def add_file_object(self, fobject, fullpath, filename, replace=False, bucket=None):
        fullpath = self.get_file_path(fullpath)
        mkdir_p(fullpath)
        file_path = "/".join([fullpath, filename])
        if not replace:
            if os.path.exists(file_path):
                return
        with open(file_path, 'w') as f:
            f.write(fobject.read())
        return file_path

    def addFile(self, tenant, inbox, dataset,
                file_type, file_name, local_file_path, replace, stackspecific=False):
        s3list = [pathprefix, tenant, dataset, "_files", inbox,
                  file_type]
        if stackspecific and CNAME:
            s3list = [pathprefix, tenant, CNAME, dataset, "_files", inbox, file_type]
        dirname = "/".join(s3list)
        mkdir_p(dirname)
        full_path = "/".join([dirname, file_name])
        copyfile(local_file_path, full_path)
        return full_path

    def add_adhoc_file(self, tenant, file_name, local_file_path, dataset=None, stackspecific=False):
        dirname = os.path.join(pathprefix, tenant)
        if stackspecific and CNAME:
            dirname = os.path.join(dirname, CNAME)
        if dataset:
            dirname = os.path.join(dirname, dataset)
        mkdir_p(dirname)
        full_path = os.path.join(dirname, file_name)
        copyfile(local_file_path, full_path)
        return full_path

    def get_full_path_for_adhoc_file(self, tenant, file_name, dataset=None, stackspecific=False):
        dirname = os.path.join(pathprefix, tenant)
        if stackspecific and CNAME:
            dirname = os.path.join(dirname, CNAME)
        if dataset:
            dirname = os.path.join(dirname, dataset)
        full_path = os.path.join(dirname, file_name)
        return full_path

    def move_file(self, tenant, inbox, dataset, file_type, file_name,
                  old_full_path, stackspecific=False):

        # Make sure required destination directory is present
        s3list = [pathprefix, tenant, dataset, "_files", inbox, file_type]
        if stackspecific and CNAME:
            s3list = [pathprefix, tenant, CNAME, dataset, "_files", inbox,
                      file_type]
        dirname = "/".join(s3list)
        mkdir_p(dirname)
        new_full_path = "/".join([dirname, file_name])
        os.rename(old_full_path, new_full_path)
        return new_full_path

    def get_known_stages(self, tenant, dataset, stackspecific=False):
        dirname = os.path.join(pathprefix, tenant, dataset, "_files")
        if stackspecific and CNAME:
            dirname = os.path.join(pathprefix, tenant, CNAME, dataset, "_files")
        return set(os.listdir(dirname))

    def adhocfilelist(self, tenant, path=None):
        """
        This methods returns the file info for any path for a given
        tenant. It is not tied to a datset or stage. If no path is
        specified, the top level info is returned. This will not recurse by default.
        """
        def yield_this(tenant, path):
            dirname = os.path.join(pathprefix, tenant, path) if path else os.path.join(pathprefix, tenant)
            try:
                for ft in os.listdir(dirname):
                    fullpath = os.path.join(dirname, ft)
                    if not ft.startswith('.'):
                        yield {"fullpath": fullpath, "name": ft, "isdir": os.path.isdir(fullpath)}
            except OSError as err:
                if err.errno == errno.ENOENT:
                    pass
                else:
                    raise

        for x in yield_this(tenant, path):
            yield x

    def filelist(self, tenant, inbox=None, dataset=None, file_type=None, stackspecific=False):
        inbox = inbox or '_data'
        if dataset:
            for x in self.yield_files(tenant, inbox, dataset, file_type, stackspecific=stackspecific):
                yield x
        else:
            dirname = os.path.join(pathprefix, tenant)
            try:
                for dataset in os.listdir(dirname):
                    full_dataset_path = os.path.join(dirname, dataset)
                    if os.path.isdir(full_dataset_path):
                        for x in self.yield_files(tenant, inbox, dataset, file_type):
                            yield x
            except OSError as err:
                if err.errno == errno.ENOENT:
                    pass
                else:
                    raise

    def yield_files(self, tenant, inbox, dataset, file_type, files=None, stackspecific=False):
        dirname = os.path.join(pathprefix, tenant, dataset, "_files", inbox)
        if stackspecific and CNAME:
            dirname = os.path.join(pathprefix, tenant, CNAME, dataset, "_files", inbox)
        try:
            for ft in os.listdir(dirname):
                full_type_dir = os.path.join(dirname, ft)
                if (os.path.isdir(full_type_dir) and (not file_type or file_type == ft)):
                    for f in (files or os.listdir(full_type_dir)):
                        if not f.startswith('.'):
                            yield FileInfo(dataset, inbox, ft,
                                           f, os.path.join(full_type_dir, f))
        except OSError as err:
            if err.errno == errno.ENOENT:
                pass
            else:
                logger.error("OS Error (%s) while yielding files", err.errno,
                             exc_info=sys.exc_info())
                raise
        except:
            logger.error("Error while yielding files", exc_info=sys.exc_info())

    def open(self, fullpath, bucket=None):
        return file(fullpath)

    def deleteFile(self, tenant, inbox, dataset, file_type, file_name, stackspecific=False):
        fullpath = os.path.join(pathprefix, tenant, dataset,
                                "_files", inbox, file_type, file_name)
        if stackspecific and CNAME:
            fullpath = os.path.join(pathprefix, tenant, CNAME, dataset,
                                    "_files", inbox, file_type, file_name)
        os.remove(fullpath)

    def delete_adhoc_file(self, fullpath, bucket=None):
        try:
            os.remove(fullpath)
        except:
            pass

    def file_exists(self, tenant, inbox, dataset, file_type, file_name, stackspecific=False):
        fullPath = os.path.join(pathprefix, tenant, dataset, "_files",
                                inbox, file_type, file_name)
        if stackspecific and CNAME:
            fullPath = os.path.join(pathprefix, tenant, CNAME, dataset, "_files",
                                    inbox, file_type, file_name)
        return os.path.isfile(fullPath)

    def if_exists(self, path, *args, **kwargs):
        return os.path.isfile(self.get_file_path(path))

    def remove_tenant(self, tenant):
        tenant_path = os.path.join(pathprefix, tenant)
        try:
            shutil.rmtree(tenant_path)
        except OSError as ose:
            if ose.errno != errno.ENOENT:
                raise

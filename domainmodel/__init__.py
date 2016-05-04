
from datetime import datetime
from pandas import json

from celery_sample import sec_context, crypto_utils
from celery_sample.settings import gnana_db
import logging
from bson import BSON
from pymongo import ASCENDING
import re
import time


logger = logging.getLogger('gnana.%s' % __name__)

gnana_db2 = gnana_db


class Model(object):

    """Base class for all domainmodel objects and provides the basic ORM
    capabilities and conventions.

    In addition, this class also provides a
    few class level methods that can be used to retrieve objects by keys
    and names.

    This base class adds the following into the collection:

    _id
        Database ID of the object

    _kind
        Type of the object stored in the collection.  Most of the time, a
        collection holds homogeneous kind of objects, however this is not
        guaranteed.

    _version
        Each object will store a schema version used to store the object.
        In the future it is expected that, an upgrade method is called to
        progressively upgrade the data in the collection.

        .. NOTE:: As of now we are storing the version, but no upgrade logic
            is implemented.

    object
        All the attributes of the object are encoded by calling the encode
        method and saved with object key

        During encoding each domainobject should ensure there are no periods
        in any dictionaries.  This is a limitation in document databases.

    Following Class level variables to be defined by each domain object:

    kind
        Kind of the object

    tenant_aware
        If the tenant aware is set to true, when saving the objects,
        collection name is prefixed with the tenant name from security context
        object

    version
        Current object schema version

    index_list
        A dictionary of indexes to be ensured when saving.  When specifying
        the index, additional options can be provided as a subdictionary.

        Examples::

            # Just an index
            'probeid': {}

            # Unique index on object.username field in DB
            'username': {'unique': True}

            # Index to expire documents after certain age. In this example
            # document expires after 900 sec + value in object.timestamp
            # field
            'timestamp': {
                'expireAfterSeconds': 900
            }

    query_list
        A dictionary in the same format as indexes, that must be searchable
        but not actually created in the DB as indexes.

    collection_name
        Name of the collection in which to store the values.

        .. WARNING: Never use ModelClass.collection_name directly, instead
            use ModelClass.getCollectionName() method.  Using the method ensures
            that tenant prefixing is done properly.
    """
    kind = None
    tenant_aware = True
    collection_name = None
    index_list = {}
    version = None
    index_checked = {}
    encrypted = True
    query_list = {}
    check_sum = None # Used in UIP prepare to compare stage_uip & main_uip records
    typed_fields = {}

    def __init__(self, attrs):
        if attrs is not None and (u'_id' in attrs or '_id' in attrs):
            self.id = attrs[u'_id']
            if u'_kind' not in attrs:
                raise ModelError("Kind of object is not defined by subclass")
            if attrs[u'_kind'] != self.kind:
                raise ModelError(
                    "Loading incorrect type of object, DB: %s, Required: %s" %
                    (attrs[u'_kind'], self.kind))
            # Ideally this should not be here.
            if self.encrypted and self.tenant_aware and u'_encdata' in attrs:
                decdata = BSON(
                    sec_context.decrypt(attrs[u'_encdata'])).decode()
                if decdata:
                    attrs[u'object'] = decdata
            if attrs['_version'] != self.version:
                self.upgrade_attrs(attrs)
            self.last_modified_time = attrs.get('last_modified_time', None)
            self.check_sum = attrs.get('check_sum', None)
            self.decode(attrs[u'object'])
        else:
            self.id = None
            self.last_modified_time = None
            self.check_sum = None

    def save(self, is_partial=False, bulk_list=None, id_field=None):
        """Save the object into the database.
        If the object is initialized with attributes and an _id is found,
        during the object creation, it is overwritten.  If no _id is present
        it is created.
        id_field: Is used to update records based on the given field
        """
        collection_name = self.getCollectionName()
        postgres = getattr(self, "postgres", None)

        self.create_index()

        localattrs = {}
        self.last_modified_time = time.time()
        self.encode(localattrs)
        localattrs.pop('last_modified_time', None)
        check_sum_value = localattrs.pop('check_sum', None)
        object_doc = {'object': localattrs,
                      '_kind': self.kind,
                      '_version': self.version,
                      'last_modified_time': self.last_modified_time}
        if check_sum_value:
            object_doc['check_sum'] = check_sum_value
        if self.tenant_aware and self.encrypted and sec_context.details.is_encrypted:
            encdata = sec_context.encrypt(BSON.encode(localattrs))
            if encdata:
                self.encdata = encdata
                object_doc.update({
                    "_encdata": encdata,
                    "object": crypto_utils.extract_index(self.index_list, localattrs)
                })
                object_doc["object"].update(
                    crypto_utils.extract_index(self.query_list, localattrs)
                )

        if bulk_list is not None:
            if not self.id and not id_field:
                bulk_list.insert(object_doc)
            else:
                if self.id:
                    object_doc['_id'] = self.id
                id_field = '_id' if not id_field else id_field

                if id_field and '.' in id_field:
                    keys = id_field.split('.')
                    id_field_val = object_doc[keys[0]][keys[1]]
                else:
                    id_field_val = object_doc[id_field]
                match_val = self.id if not id_field else id_field_val

                if is_partial:
                    bulk_list.find({id_field: match_val}).upsert().update_one(object_doc)
                else:
                    bulk_list.find({id_field: match_val}).upsert().replace_one(object_doc)
            return None
        else:
            if getattr(self, "postgres", None):
                self.id = gnana_db2.saveDocument(
                    collection_name, object_doc, self.id, is_partial=is_partial)
            else:
                self.id = gnana_db.saveDocument(
                    collection_name, object_doc, self.id, is_partial=is_partial)

            return self.id

    @classmethod
    def bulk_ops(cls):

        class Bulk_Operations():

            def __init__(self, postgres, col):
                self.postgres = postgres
                self.col = col
                if postgres:
                    self.bulk_list = []
                    self.bulk_update = []
                    self.find_objs = {}
                else:
                    self.bulk_list = gnana_db.db[
                        col].initialize_unordered_bulk_op()

            def execute(self):
                if self.postgres:
                    #                     pass
                    if self.bulk_list:
                        logger.info("Using postgres bulk insert")
                        gnana_db2.postgres_bulk_execute(
                            self.bulk_list, self.col)
                    if self.bulk_update:
                        logger.info("Using postgres bulk update")
                        gnana_db2.postgres_bulk_update(self.bulk_update)
                else:
                    self.bulk_list.execute()

            def insert(self, doc):
                if self.postgres:
                    self.bulk_list.append(doc)
#                     gnana_db2.insert(self.col, [doc])
                else:
                    self.bulk_list.insert(doc)

            def upsert(self):
                if self.postgres:
                    return self
                else:
                    return self.bulk_list.upsert()

            def _replace(self, doc):
                if self.postgres:
                    self.bulk_update.append(
                        gnana_db2.postgres_bulk_update_string(self.col, doc))
#                     gnana_db2.saveDocument(self.col, doc, doc["_id"])
                else:
                    raise Exception('replace is not supported for Mongo, this is specific to postgres')

            def find(self, criteria):
                if self.postgres:
                    if criteria:
                        obj = gnana_db2.findDocument(self.col, criteria)
                        if obj:
                            self.find_objs[sec_context.encrypt(obj['object']['extid'])] = obj
                    return self
                else:
                    #                     self.bulk_list.find({'_id': doc['_id']}).replace_one(doc)
                    return self.bulk_list.find(criteria)

            def replace_one(self, doc):
                if self.postgres:
                    if (doc['object']['extid'] in self.find_objs):
                        saved_rec = self.find_objs.pop(doc['object']['extid'])
                        doc['_id'] = saved_rec['_id']
                        self._replace(doc)
                    else:
                        self.insert(doc)
                else:
                    self.bulk_list.replace_one(doc)

            def update_one(self, doc):
                if self.postgres:
                    saved_rec = self.find_objs.pop(doc['object']['extid'])
                    saved_rec.update(doc)
                    self._replace(saved_rec)
#                     gnana_db2.saveDocument(self.col, doc,doc["_id"])
                else:
                    return self.bulk_list.update_one(doc)

        return Bulk_Operations(getattr(cls, "postgres", False), cls.getCollectionName())

    @classmethod
    def bulk_insert(cls, rec_list):
        cls.create_index()
        tenant_details = sec_context.details
        is_tenant_encrypted = tenant_details.is_encrypted
        docs_to_insert = []
        for rec in rec_list:
            localattrs = {}
            rec.last_modified_time = time.time()
            rec.encode(localattrs)
            localattrs.pop('last_modified_time', None)
            check_sum_value = localattrs.pop('check_sum', None)
            query_fields = []
            postgres = False
            if getattr(cls, "postgres", None):
                query_fields = cls.all_known_fields
                postgres = True
                if cls.typed_fields:
                    localattrs = cls.check_postgres_typed_fields(localattrs)
            object_doc = {'object': localattrs,
                          '_kind': cls.kind,
                          '_version': cls.version,
                          'last_modified_time': rec.last_modified_time}
            if check_sum_value:
                object_doc['check_sum'] = check_sum_value
            docs_to_insert.append(object_doc)
        if docs_to_insert:
            if getattr(cls, "postgres", None):
                gnana_db2.insert(cls.getCollectionName(), docs_to_insert)
            else:
                gnana_db.insert(cls.getCollectionName(), docs_to_insert)

    @classmethod
    def copy_collection(cls, newcls, criteria={}, batch_size=5000):
        if getattr(cls, "postgres", None):
            cur = gnana_db2.findDocuments(
                cls.getCollectionName(), criteria, auto_decrypt=True)
        else:
            cur = gnana_db.findDocuments(
                cls.getCollectionName(), criteria, auto_decrypt=True)
        count = cur.count()
        start_size = 0
        while start_size < count:
            to_process = min(count - start_size, batch_size)
            newcls.bulk_insert(cur[start_size:start_size + to_process])
            start_size += to_process

    @classmethod
    def truncate_or_drop(cls, criteria=None):
        if criteria is None:
            if getattr(cls, "postgres", None):
                return gnana_db2.dropCollection(cls.getCollectionName())
            else:
                return gnana_db.dropCollection(cls.getCollectionName())
        else:
            if getattr(cls, "postgres", None):
                return gnana_db2.truncateCollection(cls.getCollectionName(), criteria, cls.typed_fields)
            else:
                return gnana_db.truncateCollection(cls.getCollectionName(), criteria)['n']

    @classmethod
    def renameCollection(cls, new_col_name, overwrite=False):
        if getattr(cls, "postgres", None):
            gnana_db2.renameCollection(
                cls.getCollectionName(), new_col_name, overwrite)
        else:
            gnana_db.renameCollection(
                cls.getCollectionName(), new_col_name, overwrite)

    @classmethod
    def create_index(cls):
        postgres = getattr(cls, "postgres", None)
        collection_name = cls.getCollectionName()
        if not cls.index_checked.get(collection_name, False):
            for x in cls.index_list:
                options = cls.index_list[x]
                if 'index_spec' in cls.index_list[x]:
                    options = options.copy()
                    index_spec = options.pop('index_spec')
                else:
                    index_field_list = x.split('~')
                    index_spec = []
                    for f in index_field_list:
                        index_fld = ("object.%s" % f, ASCENDING)
                        index_spec.append(index_fld)
                if postgres:
                    gnana_db2.ensureIndex(collection_name, index_spec, options)
                else:
                    gnana_db.ensureIndex(collection_name, index_spec, options)

            # Special indexes that need to be added for all models are to be
            # defined here
            if postgres:
                gnana_db2.ensureIndex(
                    collection_name, "last_modified_time", {})
            else:
                gnana_db.ensureIndex(collection_name, "last_modified_time", {})
            cls.index_checked[collection_name] = True
            all_query_fields_new = {
            'index_list': loaded_index_list(cls.index_list),
            'query_list': loaded_index_list(cls.query_list)
            }
#             all_query_fields_old = None
#             try:
#                 all_query_fields_old = gnana_db.findDocument(
#                         sec_context.name+'.saved_index_info',
#                         {'collection': collection_name}
#                 )
#             except:
#                 pass
#             if (all_query_fields_old is None or
#                     diff_rec(all_query_fields_new, all_query_fields_old['index_info'])):
#                 if not all_query_fields_old:
#                     all_query_fields_old = {'collection': collection_name}
#                 all_query_fields_old['index_info'] = all_query_fields_new
#                 try:
#                     gnana_db.saveDocument(
#                         sec_context.name+'.saved_index_info',
#                         all_query_fields_old
#                     )
#                 except Exception as e:
#                     logger.exception("Got Exception while saving index info %s" % (e.message))

    @classmethod
    def getCollectionName(cls):
        """
        Return the collection name to be used for the class.

        .. Warning:: Do not cache this value, as it will change based on
            the context.
        """
        if(cls.tenant_aware):
            return sec_context.name + "." + cls.collection_name
        else:
            return cls.collection_name

    @classmethod
    def getBySpecifiedCriteria(cls, criteria, check_unique=False):
        """Find an object by given criteria. The caller can specify any
        MongoDB-blessed criteria to find a specific document.

        check_unique
            When multiple objects match the field value, passing check_unique as
            True will explicitly checking nothing else matched.  Otherwise it
            returns the first matching object

            .. NOTE::

                Generally you should use the unique_indexes and not depend
                on this mechanism.
        """
        if getattr(cls, "postgres", None):
            attrs = gnana_db2.findDocument(cls.getCollectionName(),
                                           criteria,
                                           check_unique)
        else:
            attrs = gnana_db.findDocument(cls.getCollectionName(),
                                          criteria,
                                          check_unique)
        if(attrs):
            return cls(attrs)
        else:
            return None

    @classmethod
    def getByFieldValue(cls, field, value, check_unique=False):
        """Find an object by given field value.  ``object.`` is automatically
        prepended to the field name provided.  An object of the class on which
        this method is called will be created, hence this method will work for
        all domainmodel objects.

        forgive
            When multiple objects match the field value, passing forgive as
            True will return the first matching object.  If it is False, an
            exception is raised.

            TODO: Refactor this - use getBySpecifiedCriteria once the tests pass
        """
        if(getattr(cls, "postgres", None)):
            attrs = gnana_db2.findDocument(
                cls.getCollectionName(),
                {'object.' + field:
                    sec_context.encrypt(value) if cls.tenant_aware and cls.encrypted else value},
                check_unique
            )
        else:
            attrs = gnana_db.findDocument(
                cls.getCollectionName(),
                {'object.' + field:
                    sec_context.encrypt(value) if cls.tenant_aware and cls.encrypted else value},
                check_unique
            )
        return cls(attrs) if attrs else None

    @classmethod
    def getAllByFieldValue(cls, field, value):
        """Find all objects by given field value.  ``object.`` is automatically
        prepended to the field name provided.  An object of the class on which
        this method is called will be created, hence this method will work for
        all domainmodel objects.


            TODO: Refactor this - use getBySpecifiedCriteria once the tests pass
        """
        try:
            if(getattr(cls, "postgres", None)):
                my_iter = gnana_db2.findAllDocuments(cls.getCollectionName(),
                                                     {'object.' + field: sec_context.encrypt(value)})
            else:
                my_iter = gnana_db.findAllDocuments(cls.getCollectionName(),
                                                    {'object.' + field: sec_context.encrypt(value)})
            for attrs in my_iter:
                yield cls(attrs)
        except:
            logger.exception("Exception raised while finding values %s-%s-%s" %
                             (cls.getCollectionName(), field, value))

    @classmethod
    def getAll(cls, criteria={}):
        """Find all objects.  An object of the class on which
        this method is called will be created, hence this method will work for
        all domainmodel objects.
        """
        try:
            if getattr(cls, "postgres", None):
                my_iter = gnana_db2.findAllDocuments(
                    cls.getCollectionName(), criteria, cls.typed_fields)
            else:
                my_iter = gnana_db.findAllDocuments(
                    cls.getCollectionName(), criteria)
            for attrs in my_iter:
                yield cls(attrs)
        except Exception as e:
            a = e
            logger.exception(a)

    @classmethod
    def getByKey(cls, key):
        """Find an object by key
        """
        if getattr(cls, "postgres", None):
            attrs = gnana_db2.retrieve(cls.getCollectionName(), key)
        else:
            attrs = gnana_db.retrieve(cls.getCollectionName(), key)
        return attrs and cls(attrs) or None

    @classmethod
    def getByName(cls, name):
        """Find an object by name.  This is shorthand to getByFieldValue
        """
        return cls.getByFieldValue('name', name)

    # Nothing to encode, id is used automatically
    def encode(self, attrs):
        """Called before saving to get a dictionary representation of the
        object suitable for saving into a document database.  Make sure to
        call the super class method in the implementations.  No need to return
        anything, just updating the attrs is enough.
        """
        attrs['last_modified_time'] = self.last_modified_time
        attrs['check_sum'] = self.check_sum
        return attrs

    # Nothing to decode
    def decode(self, attrs):
        """Called to reconstruct the object from the dictionary.  Initialize
        any required variables and make sure to call the super class
        """
        self.last_modified_time = attrs.get(u'last_modified_time', None)
        self.check_sum = attrs.get(u'check_sum', None)
        pass

    @classmethod
    def remove(cls, objid):
        """Remove the object from the database
        """
        if(objid):
            if getattr(cls, "postgres", None):
                gnana_db2.removeDocument(cls.getCollectionName(), objid)
            else:
                gnana_db.removeDocument(cls.getCollectionName(), objid)
        else:
            raise ModelError("Can't remove unbound db object")

    def _remove(self):
        self.remove(self.id)

    @classmethod
    def list_all_collections(cls, prefix):
        return gnana_db.collection_names(prefix)

    @classmethod
    def check_postgres_typed_fields(cls,localattrs):
        for key in localattrs:
            if key in cls.typed_fields.keys():
                typed_field_type = cls.typed_fields[key]['type']
                is_array = re.search("(ARRA\w+)", typed_field_type) or re.search("[\[]*", typed_field_type)
                if is_array and not isinstance(localattrs[key], list):
                    localattrs[key] = [localattrs[key]]
        return localattrs

    @classmethod
    def get_db(cls):
        return gnana_db


class ModelError(Exception):
    def __init__(self, error):
        logger.error(error)
        self.error = error


def loaded_index_list(index_list_class):
    index_list_db = {}
    for k, v in index_list_class.iteritems():
        name = k.replace('.', '~')
        index_list_db[name] = v.copy()
        if 'index_spec' in v:
            index_list_db[name]['key'] = v.get('index_spec')
        else:
            index_field_list = k.split('~')
            index_spec = []
            for f in index_field_list:
                index_fld = ("object.%s" % f, ASCENDING)
                index_spec.append(index_fld)
            index_list_db[name]['key'] = index_spec
    return index_list_db


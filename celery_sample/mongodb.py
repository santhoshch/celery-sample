from bson.objectid import ObjectId
import logging
from bson import BSON
from __builtin__ import isinstance
from pymongo.read_preferences import ReadPreference
import time
import pymongo


logger = logging.getLogger('gnana.%s' % __name__)


class GnanaMongoDB(object):
    is_prod = False
    sec_context = None
    def __init__(self, db):
        class DbSwitcher:
            def __init__(self,parent):
                self.parent = parent
            def __getattr__(self, attr):
                if attr is not '__getitem__':
                    return getattr(db,attr)

            def __getitem__(self,collection):
                if self.parent.is_prod:
                    if collection.startswith(self.parent.sec_context.name):
                        tenant_db = self.parent.sec_context.tenant_db
                        # If there is no tenant_db we are defaulting to the standard DB
                        # Just to prevent internal server errors
                        return tenant_db[collection] if tenant_db else self.db[collection]
                return db[collection]
        self.db = DbSwitcher(self)

    def saveDocument(self, collection, document, objid=None, is_partial=False):
        if(objid):
            doc = document.copy()
            doc['_id'] = objid
        else:
            doc = document
        identity = doc.get('_id',None)
        if is_partial and identity:
            doc.pop('_id')
            try:
                self.db[collection].update({'_id':identity}, {'$set':doc})
                return identity
            except Exception as ex:
                logger.exception('Exception in updating a document %s', str(doc))
                raise
        else:
            try:
                return self.db[collection].save(doc)
            except Exception as ex:
                logger.exception('Exception in saving a document %s', str(doc))
                raise

    def findDocument(self, collection, criteria, check_unique=False):
        collection.split('.')
        matches = self.db[collection].find(criteria)
        try:
            ret = matches.next()
        except StopIteration:
            return None
        
        if check_unique:
            try:
                next_val = matches.next()
                raise Exception()
            except StopIteration:
                pass
        if '_encdata' in ret:
            ret[u'object'] = BSON(ret.pop('_encdata')).decode()
        return ret
    
    def findAll(self, collection):
        matches = self.db[collection].find({})
        for c in matches:
            yield c

    def find_count(self, collection, criteria={}):
        return self.db[collection].find(criteria).count()        
    
    def dropCollection(self, name):
        self.db[name].drop()

    def renameCollection(self, name, new_name, overwrite=False):
        self.db[name].rename(new_name, dropTarget=overwrite)

    def renameCollectionsInNamespace(self, name, new_name, overwrite=False):
        namespace = name + "."
        for col_name in (self.sec_context.tenant_db if self.is_prod else self.db).collection_names():
            if(col_name.startswith(namespace)):
                new_col_name = new_name + col_name[len(namespace)-1:]
                self.db[col_name].rename(new_col_name, dropTarget=overwrite)

    def removeDocument(self, collection, objid):
        if(type(objid) == str or type(objid) == unicode):
            objid = ObjectId(objid)
        self.db[collection].remove({'_id': objid})
        
    def retrieve(self, collection, objid):
        if(type(objid) == str or type(objid) == unicode):
            objid = ObjectId(objid)
        return self.db[collection].find_one({'_id': objid})

    def ensureIndex(self, collection, index_field, options):
        try:
            if isinstance(index_field,list):
                # When index_spec is a list, ensure that the sort direction is
                # integer and not float
                for i in range(len(index_field)):
                    if isinstance(index_field[i][1],float) :
                        index_field[i][1]=int(index_field[i][1])
            self.db[collection].ensure_index(index_field, **options)
        except Exception as ex:
            logger.exception('Failed to ensure index on %s for %s with options %s' % (collection, str(index_field), str(options)))
            raise ex

    def truncateCollection(self, name, criteria=None):
        if criteria is None:
            criteria = {}
        return self.db[name].remove(criteria)

    def insert(self, name, rec_list):
        retry = False
        for i in range(3):
            try:
                if not retry:
                    return self.db[name].insert(rec_list)
                else:
                    if isinstance(rec_list, list):
                        for d in rec_list:
                            try:
                                self.db[name].insert(d)
                            except Exception as e:
                                logger.info(
                                    'retry failed  for a particular insert - %s extid - %s' % (e, getattr(d, 'extid', 'No Extid')))
                        return len(rec_list)
                    else:
                        return self.db[name].insert(rec_list)
            except pymongo.errors.AutoReconnect as e:
                retry = True
                time.sleep(5)
                logger.info('Reconnecting .... %s' % i)

    def findDocuments(self, name, criteria,
                      fieldlist=None,
                      sort=None,
                      batch_size=None, 
                      hint=None,
                      auto_decrypt=True,
                      read_from_tertiary=False):
        if read_from_tertiary:
            if fieldlist:
                ret_cursor = self.db[name].find(criteria, fieldlist, sort=sort,tag_sets = [{'node': 'tertiary'},{'node': 'secondary'}],read_preference=ReadPreference.SECONDARY)
            else:
                ret_cursor = self.db[name].find(criteria,read_preference=ReadPreference.SECONDARY ,sort=sort,tag_sets = [{'node': 'tertiary'},{'node': 'secondary'}])
        else:
            if fieldlist:
                ret_cursor = self.db[name].find(criteria, fieldlist, sort=sort)
            else:
                ret_cursor = self.db[name].find(criteria, sort=sort)
        if hint:
            ret_cursor = ret_cursor.hint(hint)
        
        if batch_size:
            ret_cursor = ret_cursor.batch_size(max(2, batch_size))
    
        return MongoDBCursorIterator(ret_cursor, auto_decrypt)

    findAllDocuments = findDocuments

    def getDistinctValues(self, name, key, criteria=None):
        if criteria is None:
            criteria = {}
        try:
            if criteria:
                return [val for val in self.db[name].find(criteria).distinct(key)]
            else:
                return [val for val in self.db[name].distinct(key)]
        except Exception as ex:
            if 'distinct too big' in ex.message:
                def get_nested_value(d, key):
                    x = d
                    for c in key.split():
                        x=x.get(c,{})
                    return x if x else None
    
                def None_filter(l):
                    for d in l:
                        v = get_nested_value(d, key)
                        if v is not None:
                            yield v
    
                all_docs = self.findDocuments(name, criteria, {key:1})
                return list(set(x for x in None_filter(all_docs)))
            else:
                raise ex

    def dropCollectionsInNamespace(self, name):
        namespace = name + "."
        for col_name in (self.sec_context.tenant_db if self.is_prod else self.db).collection_names():
            if(col_name.startswith(namespace)):
                self.db[col_name].drop()

    def collection_names(self, prefix):
        for col_name in (self.sec_context.tenant_db if self.is_prod else self.db).collection_names():
            if(col_name.startswith(prefix)):
                yield col_name

    def indexInformation(self, collection):
        return self.db[collection].index_information()

    def getAggregateValues(self, collection, pipeline):
        result = self.db[collection].aggregate(pipeline)
        return result


class MongoDBCursorIterator(object):
    def __init__(self, cursor, auto_decrypt):
        self.cursor = cursor
        self.auto_decrypt = auto_decrypt
        
    def hint(self, hint):
        self.cursor.hint(hint)
        return self
    
    def limit(self, limit):
        self.cursor.limit(limit)
        return self
    
    def skip(self, limit):
        self.cursor.skip(limit)
        return self
    
    def count(self):
        return self.cursor.count()
    
    def batch_size(self, size):
        self.cursor.batch_size(size)
        return self
    
    def next(self):
        x = self.cursor.next()
        if self.auto_decrypt and '_encdata' in x:
            x['object'] = BSON(x.pop('_encdata')).decode()
        return x
    
    def __iter__(self):
        for x in self.cursor:
            if self.auto_decrypt and '_encdata' in x:
                x['object'] = BSON(x.pop('_encdata')).decode()
            yield x
            
    def close(self):
        self.cursor.close()

    

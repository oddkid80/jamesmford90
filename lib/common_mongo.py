from common import common
from pymongo import MongoClient
import logging

class common_mongo():
    def __init__(self,mongo_connection_string,mongo_create=False):
        self.mongo_connection_string = mongo_connection_string
        self.mongo_create = mongo_create      
    
    def _define_cursor(self,db,collection):
        try:
            logging.info('Attempting to create mongo cursor.')
            mongo_conn = MongoClient(self.mongo_connection_string)

            if not self.mongo_create:
                list_of_dbs = [dbs['name'] for dbs in mongo_conn.list_databases()]
                if db not in list_of_dbs:
                    raise Exception(f"Database '{db}' does not exist in the mongodb. Either set mongo_create = True, or pick one of existing dbs. List of dbs: [{', '.join(list_of_dbs)}]")        
            
                db = mongo_conn[f'{db}']
                list_of_colls = [colls['name'] for colls in db.list_collections()]
                if collection not in list_of_colls:
                    raise Exception(f"Collection '{collection}' does not exist in mongodb. Either set mongo_create = True, or pick one of existing collections. List of collections: [{', '.join(list_of_colls)}]")
            
            else:
                db = mongo_conn[f'{db}']
                cursor = db[f'{collection}']
            logging.info('Mongo cursor created.')
            return cursor
        except Exception as ex:
            logging.error(f'Failed to create mongo cursor. Error: {ex}')
            raise ex

    def fetch(self,db,collection,query={},chunk=None):
        cursor = self._define_cursor(db,collection)        
        try:
            logging.info(f"Fetching from mongo, {db}.{collection}, with query: {query}")
            if chunk:
                return cursor.find(query).batch_size(chunk)
            else:
                return cursor.find(query)
        except Exception as ex:
            logging.error(f'Mongo fetch failed, error: {ex}')
        
    def insert(self,db,collection,list_of_dicts):
        cursor = self._define_cursor(db,collection)
        logging.info('Inserting records into mongodb.')
        try:
            if len(list_of_dicts) == 1:
                cursor.insert_one(list_of_dicts[0])
            elif isinstance(list_of_dicts,dict):
                cursor.insert_one(list_of_dicts)
            else:
                cursor.insert_many(list_of_dicts)
            logging.info('Records inserted into mongodb!')
        except Exception as ex:
            logging.error(f'Insert failed with error: {ex}')
    
    def update():
        pass
    
    def delete(self,db,collection,query={"0":1},delete_many=False):
        cursor = self._define_cursor(db,collection)
        if query == {"0":1}:
            logging.warning(f"No query specified, will not delete any records. If you wish to delete all records, specify query appropriately.")
        logging.info(f"Deleting from mongo, {db}.{collection}, with query: {query}")
        try:
            if delete_many:
                mongo_del = cursor.delete_many(query)
            else:
                mongo_del = cursor.delete_one(query)
            logging.info(f'Records deleted from mongodb! Count of records deleted: {mongo_del.deleted_count}')
        except Exception as ex:
            logging.error(f'Delete failed with error: {ex}')
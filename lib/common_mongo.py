from common import common
from pymongo import MongoClient
import logging

class mongo_cursor():
    def __init__(self,mongo_connection_string,db,collection,**kwargs):
        #creating mongo connection with passed string
        try:
            logging.info('Attempting to establish mongo connection.')
            self.mongo_conn = MongoClient(mongo_connection_string)
        except Exception as ex:
            logging.error(f'Mongo connection failed, with error: {ex}')
            raise ex
        logging.info('Mongo connection created!')
        
        #pulling args and assigning
        self.mongo_create = kwargs.get('mongo_create',False)

        #pointing cursor to db and collection
        self._point_cursor(db,collection)
    
    def __enter__(self):
        return self
    def __exit__(self,exc_type,exc_value,traceback):
        del self
    
    def _point_cursor(self,db,collection):
        self.db = db
        self.collection = collection
        logging.info(f'Attempting to create mongo cursor pointing to {self.db}.{self.collection}.')
        try:
            if not self.mongo_create:
                list_of_dbs = [dbs['name'] for dbs in self.mongo_conn.list_databases()]
                if db not in list_of_dbs:
                    raise Exception(f"Database '{db}' does not exist in the mongodb. Either set mongo_create = True, or pick one of existing dbs. List of dbs: [{', '.join(list_of_dbs)}]")        
            
                db = self.mongo_conn[f'{db}']
                list_of_colls = [colls['name'] for colls in db.list_collections()]
                if collection not in list_of_colls:
                    raise Exception(f"Collection '{collection}' does not exist in mongodb. Either set mongo_create = True, or pick one of existing collections. List of collections: [{', '.join(list_of_colls)}]")

                cursor = db[f'{collection}']
            else:
                db = self.mongo_conn[f'{db}']
                cursor = db[f'{collection}']
            self.mongo_cursor = cursor
            logging.info('Mongo cursor created!')
        except Exception as ex:
            logging.error(f'Failed to create mongo cursor. Error: {ex}')
            raise ex

    def fetch(self,query={},chunk=None):             
        try:
            logging.info(f"Fetching from mongo, {self.db}.{self.collection}, with query: {query}")
            if chunk:
                return self.mongo_cursor.find(query).batch_size(chunk)
            else:
                return self.mongo_cursor.find(query)
        except Exception as ex:
            logging.error(f'Mongo fetch failed, error: {ex}')
        
    def insert(self,list_of_dicts):
        logging.info(f'Inserting records into mongodb, {self.db}.{self.collection}.')
        try:
            if len(list_of_dicts) == 1:
                self.mongo_cursor.insert_one(list_of_dicts[0])
                recs_inserted = 1
            elif isinstance(list_of_dicts,dict):
                self.mongo_cursor.insert_one(list_of_dicts)
                recs_inserted = 1
            else:
                insert = self.mongo_cursor.insert_many(list_of_dicts)
                recs_inserted = len(insert.inserted_ids)
            logging.info(f'Records inserted into mongodb! Count of records inserted: {recs_inserted}')
        except Exception as ex:
            logging.error(f'Insert failed with error: {ex}')
    
    def update(self,query={"0":1},update_query={},update_many=False,**kwargs):
        if query == {"0":1}:
            logging.warning(f"No query specified, will not update any records. If you wish to update records, specify query. If you wish to add new field, specify empty query.")
        logging.info(f"Updating mongo, {self.db}.{self.collection}, with filter: {query}, update_query: {update_query}")
        try:
            if update_many:
                updated = self.mongo_cursor.update_many(query,update_query,**kwargs)
                updated_recs = updated.modified_count
            else:
                self.mongo_cursor.update_one(query,update_query,**kwargs)
                updated_recs = 1
            logging.info(f'Records updated! Count of records updated: {updated_recs}')           

        except Exception as ex:
            logging.error(f'Update failed with error: {ex}')
            raise ex
    
    def delete(self,query={"0":1},delete_many=False):
        if query == {"0":1}:
            logging.warning(f"No query specified, will not delete any records. If you wish to delete records, specify query.")
        logging.info(f"Deleting from mongo, {self.db}.{self.collection}, with query: {query}")
        try:
            if delete_many:
                mongo_del = self.mongo_cursor.delete_many(query)
            else:
                mongo_del = self.mongo_cursor.delete_one(query)
            logging.info(f'Records deleted from mongodb! Count of records deleted: {mongo_del.deleted_count}')
        except Exception as ex:
            logging.error(f'Delete failed with error: {ex}')
            raise ex
from common import common
from lib import common_mongo
import pandas as pd
import psycopg2 as pgconn
import datetime

if __name__ == '__main__':
    mongo_string = common.fetch_config(section='mongo,localhost',subsection='connection_string')
    pg_conn_info = common.config_postgres('postgres,postgres,sandbox,local')
    
    #defining mongo cursor
    mongo_worker = common_mongo.mongo_cursor(mongo_string,'sample_credit','member',mongo_create=True)
    #loading data
    with pgconn.connect(pg_conn_info) as con:
        for members in pd.read_sql('select * from credit.member',con,chunksize=50000):
            members = members.astype(str)
            dict_to_load = members.to_dict(orient='records')
            mongo_worker.insert(dict_to_load)
        
        #repointing mongo worker
        mongo_worker._point_cursor('sample_credit','charge')
        for charge in pd.read_sql('select * from credit.charge',con,chunksize=50000):
            charge = charge.astype(str)
            dict_to_load = charge.to_dict(orient='records')
            mongo_worker.insert(dict_to_load)
        
        #repointing mongo worker
        mongo_worker._point_cursor('sample_credit','payment')
        for payment in pd.read_sql('select * from credit.payment',con,chunksize=50000):
            payment = payment.astype(str)
            dict_to_load = payment.to_dict(orient='records')
            mongo_worker.insert(dict_to_load)

    del mongo_worker

    #updating records
    with common_mongo.mongo_cursor(mongo_string,'sample_credit','payment',mongo_create=True) as mongo_worker:
        mongo_worker.update(query={},update_query={'$set':{'updated_date':datetime.datetime.now()}},update_many=True,upsert=True)
    
    #deleting records
    with common_mongo.mongo_cursor(mongo_string,'sample_credit','charge',mongo_create=True) as mongo_worker:
        mongo_worker.delete(query={'charge_category':'Groceries'},delete_many=True)

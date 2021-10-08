import sys
import os
from scripts.sql.execute_sql import sql_executor

import pandas as pd
import csv
import pyodbc
import mysql.connector as mysql
import psycopg2
from common import common

executor = sql_executor()

folder_location = os.path.dirname(os.path.abspath(__file__))

#postgres
postgres_connection = common.config_postgres(section='postgres,postgres,sandbox')
con = psycopg2.connect(postgres_connection)
executor.setup_connection_and_cursor(con)

executor.execute_sql_file(folder_location+'/build_payments_postgres.sql')

#mssql
msql_connection = common.config_sql_server(section='mssql,dbadmin')
con = pyodbc.connect(msql_connection)
executor.setup_connection_and_cursor(con)

executor.execute_sql_file(folder_location+'/build_payments_sqlserver.sql')

#mysql
#pulling data from postgres
with psycopg2.connect(postgres_connection) as con:
    for i, chunk in enumerate(pd.read_sql_query('select * from credit.payment',con,chunksize=50000)):
        if i==0:
            chunk.to_csv(path_or_buf=folder_location+'/payments_temp.txt',mode='w',sep='|',index=False,quoting=csv.QUOTE_ALL,quotechar='"')
        else:
            chunk.to_csv(path_or_buf=folder_location+'/payments_temp.txt',mode='a',sep='|',index=False,quoting=csv.QUOTE_ALL,quotechar='"',header=False)

mysql_connection = common.config_mysql(section='mysql,dbadmin')
mysql_conn = mysql.connect(**mysql_connection, allow_local_infile=True)
mysql_cursor = mysql_conn.cursor()

mysql_payment_ddl = """
drop table if exists credit.payment;
create table credit.payment
(
member_id int
, pay_date date
, paid_amount numeric(20,2)
)
;
"""
for line in mysql_payment_ddl.split(';'):
    if line.strip() != '':
        mysql_cursor.execute(line)
mysql_conn.commit()

for chunk in pd.read_csv(folder_location+'/payments_temp.txt', delimiter='|', chunksize=50000, quoting=csv.QUOTE_ALL, quotechar='"'):
    data = chunk.values.tolist()
    mysql_cursor.executemany('insert into credit.payment values (%s,%s,%s)',data)
    mysql_conn.commit()

os.remove(folder_location+'/payments_temp.txt')

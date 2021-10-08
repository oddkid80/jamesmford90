import sys
import os

import pandas as pd
import csv
import pyodbc
import mysql.connector as mysql
import psycopg2
from common import common

file_location = os.path.dirname(os.path.abspath(__file__))
charge_file = file_location+'/charge.txt'
member_file = file_location+'/member.txt'

#mysql -- mysql too slow on transactions/creating tables forgoing loading to mysql
mysql_connection = common.config_mysql(section='mysql,dbadmin')
mysql_conn = mysql.connect(**mysql_connection, allow_local_infile=True)
mysql_cursor = mysql_conn.cursor()

create_member = """
    drop table if exists credit.member;
    create table credit.member
        (
        member_id int
        ,first_name varchar(100)
        ,last_name varchar(100)
        ,gender varchar(100)
        ,dob date
        ,country varchar(100)
        )
    ;
    """
create_charge = """
    drop table if exists credit.charge;
    create table credit.charge
        (
        member_id int
        , charge_category varchar(100)
        , charge_amount numeric(20,2)
        , charge_date date
        )
    ;
    """
for line in create_member.split(';'):
    if line.strip() != '':
        mysql_cursor.execute(line)
for line in create_charge.split(';'):
    if line.strip() != '':
        mysql_cursor.execute(line)
mysql_conn.commit()

for chunk in pd.read_csv(member_file, delimiter='|', chunksize=50000, quoting=csv.QUOTE_ALL, quotechar='"'):
    data = chunk.values.tolist()
    mysql_cursor.executemany('insert into credit.member values (%s,%s,%s,%s,%s,%s)',data)
    mysql_conn.commit()
    print('success')

for chunk in pd.read_csv(charge_file, delimiter='|', chunksize=50000, quoting=csv.QUOTE_ALL, quotechar='"'):
    data = chunk.values.tolist()
    mysql_cursor.executemany('insert into credit.charge values (%s,%s,%s,%s)',data)
    mysql_conn.commit()
    print('success')

mysql_conn.close()

#postgres
postgres_connection = common.config_postgres(section='postgres,postgres,sandbox')
with psycopg2.connect(postgres_connection) as con:
    cursor = con.cursor()
    create_member = """
        drop table if exists credit.member;
        create table credit.member
            (
            member_id int
            ,first_name varchar(100)
            ,last_name varchar(100)
            ,gender varchar(100)
            ,dob date
            ,country varchar(100)
            )
        ;
        """
    create_charge = """
        drop table if exists credit.charge;
        create table credit.charge
            (
            member_id int
            , charge_category varchar(100)
            , charge_amount numeric(20,2)
            , charge_date date
            )
        ;
        """
    cursor.execute(create_member)
    cursor.execute(create_charge)

    with open(member_file,'r') as fo:
        cursor.copy_expert("COPY credit.member from STDIN DELIMITER '|' HEADER CSV QUOTE '"+'"'+"'",fo)
    with open(charge_file,'r') as fo:
        cursor.copy_expert("COPY credit.charge from STDIN DELIMITER '|' HEADER CSV QUOTE '"+'"'+"'",fo)

#mssql
mssql_table_ddl = """
drop table if exists credit.dbo.member;
create table credit.dbo.member
	(
    member_id int
    ,first_name varchar(100)
    ,last_name varchar(100)
    ,gender varchar(100)
    ,dob date
    ,country varchar(100)
	)
;
drop table if exists credit.dbo.charge;
create table credit.dbo.charge
	(
    member_id int
    , charge_category varchar(100)
    , charge_amount numeric(20,2)
    , charge_date date
	)
;
"""

msql_connection = common.config_sql_server(section='mssql,dbadmin')

with pyodbc.connect(msql_connection) as con:
    cursor = con.cursor()
    cursor.execute('begin tran credit_ddl;')
    cursor.execute(mssql_table_ddl)
    cursor.execute('commit tran credit_ddl;')

    cursor.fast_executemany = True

    for chunk in pd.read_csv(member_file, delimiter='|', chunksize=50000, quoting=csv.QUOTE_ALL, quotechar='"'):
        data = chunk.values.tolist()
        cursor.executemany('insert into credit.dbo.member values (?,?,?,?,?,?)',data)
        print('success')
    con.commit()

    for chunk in pd.read_csv(charge_file, delimiter='|', chunksize=50000, quoting=csv.QUOTE_ALL, quotechar='"'):
        data = chunk.values.tolist()
        cursor.executemany('insert into credit.dbo.charge values (?,?,?,?)',data)
        print('success')
    con.commit()

os.remove(member_file)
os.remove(charge_file)

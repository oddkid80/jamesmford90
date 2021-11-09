from common import common
import logging
import argparse
import json

class sql_executor():
    
    def __init__(self,connection_type=None,config_section=None):
        
        #postgres configuration/initialization
        if connection_type == 'postgres':
            #trying import of modules
            try:
                import psycopg2 as connector
            except:
                logging.error('Failed to import psycopg2 - please check installed packages.')
                raise Exception('Failed to import psycopg2 - please check installed packages.')
            
            #trying fetching config and creating connection
            try:
                connection_info = common.config_postgres(section=config_section)              
                self.connection = connector.connect(connection_info)
                self.cursor = self.connection.cursor()
            except Exception as ex:
                logging.error(f'{ex}')
                raise ex 
                
        #mssql configuration/initialization
        elif connection_type == 'mssql':
            #trying import of modules
            try:
                import pyodbc as connector
            except:
                logging.error('Failed to import pyodbc - please check installed packages.')
                raise Exception('Failed to import pyodbc - please check installed packages.')
            
            #trying fetching config and creation connection
            try:
                connection_info = common.config_sql_server(section=config_section)
                self.connection = connector.connect(connection_info)
                self.cursor = self.connection.cursor()
            except Exception as ex:
                logging.error(f'{ex}')
                raise ex            
        
        #mysql configuration/initialization
        elif connection_type == 'mysql':
            #trying import of modules
            try:                
                import mysql.connector as connector
            except:      
                logging.error('Failed to import mysql.connector - please check installed packages.')
                raise Exception('Failed to import mysql.connector - please check installed packages.') 
            
            #trying fetching config and creation connection
            try:
                connection_info = common.config_mysql(section=config_section)
                self.connection = connector.connect(**connection_info)
                self.cursor = self.connection.cursor()
            except Exception as ex:
                logging.error(f'{ex}')
                raise ex
                        
        #other variables
        self.param_identifier = ':'      
    
    def setup_connection_and_cursor(self,connection):
        self.connection = connection
        self.cursor = connection.cursor()
    
    def execute_sql_statement(self,sql_statement,parameters=None):
        if parameters != None:
            sql_statement = common.param_query(sql_statement,parameters,param_identifier=self.param_identifier)
        try:
            logging.info(f'Executing sql statement: {sql_statement}')
            self.cursor.execute(sql_statement)
            try:
                rows_affected = self.cursor.rowcount
            except:
                rows_affected = None
            
            if sql_statement.strip().lower().startswith('insert into'):
                logging.info(f'Insert statement succeeded. Rows affected: {rows_affected}')
                
            if sql_statement.strip().lower().startswith('delete from'):
                logging.info(f'Delete statement succeeded. Rows affected: {rows_affected}')
                
            if sql_statement.strip().lower().startswith('create'):
                logging.info('Create statement succeeded.')
            
            if sql_statement.strip().lower().startswith('drop table'):
                logging.info('Drop table statement succeeded.')
                
            if sql_statement.strip().lower().startswith('truncate'):
                logging.info('Truncate statement succeeded.')
                
            if sql_statement.strip().lower().startswith('alter'):
                logging.info('Alter statement succeeded.')
                    
        except Exception as ex:
            raise ex
        
    def execute_sql_file(self,file_name,parameters=None):
        queries = common.split_query(file_name)
        for query in queries:
            try:
                logging.info(f'SQL file: {file_name} executing statement: {query[0]} starting line: {query[2]}')
                self.execute_sql_statement(query[1],parameters=parameters)
            except Exception as ex:
                self.connection.rollback()
                logging.error(f'SQL file: {file_name} failed on line: {query[2]} with error: {str(ex)}')
                raise ex
        self.connection.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--file_name',help='String of location to file.',required=True)
    parser.add_argument('-c','--connection_type',help='Connection type.',required=True)
    parser.add_argument('-cf','--config_section',help='Config section.',required=True)
    parser.add_argument('-param','--parameters',help='SQL script parameters - will replace in sql script.',type=json.loads,required=False)
    
    parse_args, empty = parser.parse_known_args()
    args = vars(parse_args)    
    
    file_name = args['file_name']
    connection_type = args['connection_type']
    config_section = args['config_section']
    parameters = args['parameters'] or None    
    
    connections_supported = ('postgres','mssql','mysql')    
    if connection_type not in connections_supported:
        raise Exception(f'Connection type not supported. Try again. Currently supported connection types: {", ".join(connections_supported)}')
    
    executor = sql_executor(connection_type=connection_type,config_section=config_section)
    executor.execute_sql_file(file_name=file_name,parameters=parameters)
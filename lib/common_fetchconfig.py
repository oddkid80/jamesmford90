from configparser import ConfigParser
import os
from sys import platform

def fetch_config(section=None,config_file=None):
    """
    Fetches config in standard dictionary format
    """
    parser = ConfigParser(interpolation=None)

    filename = config_file

    try:
        open(filename,'r')
    except:
        raise Exception(f"Config file {filename} does not exist.")

    parser.read(filename)
    # get section
    config = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            config[item[0]] = item[1]
    else:
        raise Exception(f"{section} not found in the {filename} file")
    
    return config


class common_fetchconfig():    

    #define core config location
    config_location = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/.config/config.cfg'

    def config_postgres(section=None,config_file=config_location):
        """
        Fetches postgres config - will use class level config location which is located in the main repo ~/.config/config.cfg file
            or can be specified.
        
        Will then take the config and transform it into a standard postgres connection string
        """
        db = fetch_config(section=section,config_file=config_file)
        
        string = ''
        for i in db:
            string+=' '+i+'='+db[i]
        return string
    
    def config_sql_server(section=None,config_file=config_location):
        """
        Fetches SQL server config and puts it in string format
        """        
        db = fetch_config(section=section,config_file=config_file)

        import pyodbc
        driver = pyodbc.drivers()[0]
        if db['driver'] != driver:
            db['driver'] = '{'+driver+'}'
        del pyodbc

        connection_string = ''
        for key in db:
            if connection_string == '':
                connection_string+=key.replace('_',' ')+'='+db[key]
            else:
                connection_string+=';'+key.replace('_',' ')+'='+db[key]

        return connection_string
    
    def config_mysql(section=None,config_file=config_location):
        """
        Fetches mysql server config and returns it in dictionary format
        """

        return fetch_config(section=section,config_file=config_file)


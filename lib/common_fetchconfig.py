from configparser import ConfigParser
import os
from sys import platform

def fetch_config(section=None,subsection=None,config_file=None):
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
    
    if subsection:   
        try:     
            return config[subsection]
        except:
            raise Exception(f'{subsection} not found in config')
    else:
        return config

def format_config(format_type,config):
    """
    Formats a config dict to required string format - available types are:
        postgres
        sql_server
    """
    if isinstance(config,dict):
        if format_type == 'postgres':
            string = ''
            for i in config:
                string+=' '+i+'='+config[i]
            return string

        if format_type == 'sql_server':            
            import pyodbc
            driver = pyodbc.drivers()[0]
            if config['driver'] != driver:
                config['driver'] = '{'+driver+'}'
            del pyodbc

            string = ''
            for key in config:
                if string == '':
                    string+=key.replace('_',' ')+'='+config[key]
                else:
                    string+=';'+key.replace('_',' ')+'='+config[key]
            return string
        
        else:
            return config
    
    else:
        return config


class common_fetchconfig():    

    #define core config location
    config_location = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/.config/config.cfg'
    
    def fetch_config(section=None,subsection=None,config_file=config_location,format_type=None):
        config = fetch_config(section=section,subsection=subsection,config_file=config_file)
        return format_config(format_type,config)
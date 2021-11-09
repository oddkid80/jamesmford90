import os, sys, psutil
import time
import logging

path = os.path.abspath(__file__)
path = os.path.dirname(os.path.dirname(os.path.dirname(path)))
sys.path.append(path)
os.chdir(path)

from scripts import *

class background_daemon():
    
    def __init__(self):
        self.daemon_process = os.path.basename(sys.argv[0])
        
        self._canitrun = True
        self._daemon_ping_interval = 5   
        self._daemon_close_delay = 2   
        
        self.daemon_wd = os.getcwd()        
    
    def _find_procs(self):
        daemons_running = []
        
        for process in psutil.process_iter():
            for part in process.cmdline():
                if self.daemon_process in part.split('/')[-1]:
                    if process.pid != os.getpid():
                        daemons_running.append(process)
        
        return daemons_running
    
    def _forever_loop(self):
        
        while self._canitrun:
            try:
                logging.info('Daemon pinging.')
                self.daemon_task()
                time.sleep(self._daemon_ping_interval)    
            except Exception as ex:
                logging.error('Daemon task failed. Error: {ex}')        
                raise ex
    
    def _create_daemon_instance(self):
        
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
            
            logging.info(f'Daemon created.')
        
        except OSError as e:
            logging.error('Daemon failed to split, error: {e}')
            raise e 
            
        
        os.chdir(self.daemon_wd)
        os.setsid()
        os.umask(0)   
    
    def _kill_daemon(self):
        
        running_daemons = self._find_procs()
        
        if running_daemons:
            for daemon in running_daemons:
                logging.info(f'Terminating daemon: {daemon}.')
                daemon.terminate()
                time.sleep(self._daemon_close_delay)
        else:
            logging.info('No daemons to terminate.') 
    
    def start_daemon(self):
        
        self._create_daemon_instance()
        self._forever_loop()
        
    def daemon_task(self):
        pass        
        
        
if __name__ == '__main__':
    daemon = background_daemon()
    
    #daemon.start_daemon()
    #daemon._find_procs()
    #daemon._kill_daemon()
    
    
    
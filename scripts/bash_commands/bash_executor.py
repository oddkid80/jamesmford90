from subprocess import Popen, PIPE
from tempfile import NamedTemporaryFile, TemporaryDirectory
import datetime
import logging

class bash_executor():
    
    def execute_bash(self,bash_command):        
        tmp_file_prefix = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        with TemporaryDirectory(prefix='bash_executor_tmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=tmp_file_prefix) as tmp_file:
                tmp_file.write(bytes(bash_command, 'utf_8'))                
                tmp_file.flush()
                tmp_file_name = tmp_file.name
                
                logging.info(f'Executing bash command "{bash_command}"')
                sub_proc = Popen(['bash',tmp_file_name],cwd=tmp_dir,stdout=PIPE,stderr=PIPE) 
                out,err = sub_proc.communicate()                     
                if err:
                    logging.error(f'Bash command failed with error {err.decode().strip()}') 
                    raise Exception(f'Bash command failed with error {err.decode().strip()}') 
                sub_proc.wait()
        logging.info(f'Sucessfully executed bash command "{bash_command}"')
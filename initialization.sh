#for pyodbc need 'sudo apt-get install unixodbc'
"""
sudo su
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

#Download appropriate package for the OS version
#Choose only ONE of the following, corresponding to your OS version

#Ubuntu 16.04
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

#Ubuntu 18.04
curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

#Ubuntu 20.04
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

#Ubuntu 21.04
curl https://packages.microsoft.com/config/ubuntu/21.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

exit
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17
# optional: for bcp and sqlcmd
sudo ACCEPT_EULA=Y apt-get install -y mssql-tools
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
source ~/.bashrc
# optional: for unixODBC development headers
sudo apt-get install -y unixodbc-dev
"""


import subprocess
import os
subprocess.run('sudo apt-get update',shell=True)
subprocess.run('sudo apt install python3-pip',shell=True)

import sys
from sys import platform

if 'linux' in platform:
    subprocess.run([f"sudo su -c 'curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -'"],shell=True)
    subprocess.run([f"sudo su -c 'curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -'"],shell=True)
    subprocess.run([f"sudo su -c 'curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list'"],shell=True)
    subprocess.run('sudo apt-get update',shell=True)
    subprocess.run('sudo apt install libodbc1',shell=True)
    subprocess.run('sudo apt-get install -y unixodbc',shell=True)
    subprocess.run('sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17',shell=True)
    subprocess.run('sudo ACCEPT_EULA=Y apt-get install -y mssql-tools',shell=True)

    command = """echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc"""
    subprocess.run(command,shell=True)

    subprocess.run('source ~/.bashrc',shell=True)
    subprocess.run('sudo apt-get install -y unixodbc-dev',shell=True)


import subprocess

packages = [
    ['openpyxl','na']
    ,['pywin32','windows'] #windows only
    ,['mysql-connector-python','na']
    ,['psycopg2-binary','na']
    ,['pandas','na']
    ,['pyodbc','na']
    ,['odo','na']
    ,['sqlalchemy','na']
]
for package in packages:
    if package[1] == 'na':
        subprocess.check_call([sys.executable, '-m', 'pip', 'install',package[0]])
    if package[1] in platform:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install',package[0]])

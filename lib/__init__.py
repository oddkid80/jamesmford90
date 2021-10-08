import logging

#log config
logfile = './'+'standard_log.log'
logger = logging.getLogger()
handler = logging.FileHandler(logfile,mode='a')
formatter = logging.Formatter('%(lineno)s|%(asctime)s.%(msecs)03d|%(levelname)s|%(pathname)s|%(funcName)s|%(message)s',datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
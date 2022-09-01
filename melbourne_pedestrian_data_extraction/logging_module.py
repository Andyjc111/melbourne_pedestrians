import datetime
import logging
import os


# SIMPLE SCRIPT TO INTIALIZE LOGGING IN PYTHON 
run_time  = datetime.datetime.now().strftime('%Y%m%d_%H%M')
file_name = (f'{os.path.basename(__file__)}_{run_time}').replace('.','_')
open(f'{file_name}.log', 'a').close()

log_filename = f'{file_name}.log'
log_format   = '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(message)s'

def startlogging(filename=log_filename, format=log_format):
    logging.basicConfig(filename=log_filename,
                       format=log_format,
                       level=logging.INFO,
                       #encoding='utf-8',
                       datefmt='%Y-%m-%d %H:%M.%SS'
                       )
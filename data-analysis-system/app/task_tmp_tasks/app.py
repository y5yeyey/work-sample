# -*- coding: utf-8 -*-

"""  config  """
from db_client import *
from conf import *
import os
from excel_writer import *
import re

"""  packages  """
import pandas as pd
from channel_tasks import * 
from datetime import datetime, timedelta
from user import *

readPath = lambda(filename): "{0}\\{1}".format(os.path.dirname(os.path.realpath(__file__)), filename)

def main(dt, data_path):
    import example_jobs
    dt_start = datetime.strptime('2016-08-22', '%Y-%m-%d')
	dt_end = datetime.strptime('2016-08-29', '%Y-%m-%d')
	conn, cr = mssql_client(CONF_155['DataAnalyze'])	
	with mssql_data(conn, cr):
		example_jobs.tmp_job1(cr, dt_start, dt_end)
		example_jobs.tmp_job2(cr, dt_start, dt_end)
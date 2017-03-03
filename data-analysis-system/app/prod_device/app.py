# -*- coding: utf-8 -*-

import os
from conf import *
from db_client import *
from datetime import datetime, timedelta

class putRecords(object):
	def __init__(self, cr, begin_dt, end_dt):
		self.cr = cr
		self.begin_dt = begin_dt
		self.end_dt = end_dt
		self.path = os.path.dirname(os.path.realpath(__file__))
	def insert(self, filename):
		update_var = "DECLARE @BEGIN_DT date = '{0}'; DECLARE @END_DT date = '{1}'; ".format(self.begin_dt, self.end_dt)
		self.sql = '{0}{1}'.format( update_var, parseSQL('{0}\\{1}'.format(self.path, filename)) )
	def exe(self):
		self.cr.execute(self.sql)
		self.cr.commit()

def main(dt, data_path):
	begin_dt, end_dt = dt + timedelta(days=-1), dt
	conn, cr = mssql_client(CONF_NAME['Example'])
	with mssql_data(conn, cr):
		sql_writer = putRecords(cr, begin_dt, end_dt)
		from event import updateEventRecord
		updateEventRecord(cr, begin_dt, end_dt)
		
		
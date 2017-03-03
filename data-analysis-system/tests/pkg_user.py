# -*- coding: utf-8 -*-

from unittest import TestCase
from datetime import datetime, timedelta
from conf import CONF_EXAMPLE
from db_client import mssql_client, mssql_data
import functools

DIM_VARIABLES = {'platform': None, 'version': None, 'area': None, 'city': None, '1': None, 'tracker': None, 'operator': None, 'brand': None, 'model': None, 'network': None}

from user import *

def SQLTester(fn):
	@functools.wraps(fn)
	def wrapper(*args, **kwargs):
		sql = "SELECT * FROM ({input_sql}) sql4test WHERE 1=0".format(
			input_sql=fn(*args, **kwargs)
		)
		return sql
	return wrapper

	
class TestUser(TestCase):
	
	def __init__(self, *args, **kwargs):
		super(self.__class__, self).__init__(*args, **kwargs)
		self.conn, self.cr = mssql_client(CONF_155['DataAnalyze'])
		self.dt_start = datetime.now().date() + timedelta(days=-1)
		self.dt_end = self.dt_start + timedelta(days = 1)

	def _toTest(self, test_object, test_fns, *args):
		with mssql_data(self.conn, self.cr):
			for test_fn in test_fns:
				getattr(test_object, test_fn)(*args)
	
	def test_StartDefault(self):
		test_object = Start(self.cr, self.dt_start, self.dt_end)
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'old', 'actual', 'cumulative')
	def test_StartChannel(self):
		test_object = Start(self.cr, self.dt_start, self.dt_end, {"channel": ["jrtt1", "jrtt2"]})
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'old', 'actual', 'cumulative')
	def test_StartVersion(self):
		test_object = Start(self.cr, self.dt_start, self.dt_end, {"version": []})
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'old', 'actual', 'cumulative')
	def test_StartMix(self):
		test_object = Start(self.cr, self.dt_start, self.dt_end, {"channel": [], "version": []})
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'old', 'actual', 'cumulative')
	
	def test_ActivateDefault(self):
		test_object = Activate(self.cr, self.dt_start, self.dt_end)
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'cumulative')
	def test_ActivateChannel(self):
		test_object = Activate(self.cr, self.dt_start, self.dt_end, {"channel": []})
		self._toTest(test_object, ['sql', 'deviceid'], 'new','cumulative')
	def test_ActivateVersion(self):		
		test_object = Activate(self.cr, self.dt_start, self.dt_end, {"version": []})
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'cumulative')
	def test_ActivateMix(self):
		test_object = Activate(self.cr, self.dt_start, self.dt_end, {"channel": ["jrtt1"], "version": []})
		self._toTest(test_object, ['sql', 'deviceid'], 'new', 'cumulative')
			
	def test_RegisterDefault(self):
		test_object = Register(self.cr, self.dt_start, self.dt_end)
		self._toTest(test_object, ['sql', 'deviceid', 'userkey'], 'new', 'cumulative')
	def test_RegisterChannel(self):
		test_object = Register(self.cr, self.dt_start, self.dt_end, {"channel": []})
		self._toTest(test_object, ['sql', 'deviceid', 'userkey'], 'new', 'cumulative')

	def test_OrderDefault(self):
		test_object = Order(self.cr, self.dt_start, self.dt_end)
		self._toTest(test_object, ['sql', 'deviceid', 'userkey'], 'new', 'old', 'actual', 'cumulative')
	def test_OrderChannel(self):
		test_object = Order(self.cr, self.dt_start, self.dt_end, {"channel": []})
		self._toTest(test_object, ['sql', 'deviceid', 'userkey'], 'new', 'old', 'actual', 'cumulative')

			
class TestMetrics(TestCase):
	
	def __init__(self, *args, **kwargs):
		super(self.__class__, self).__init__(*args, **kwargs)
		self.conn, self.cr = mssql_client(CONF_155['DataAnalyze'])
		self.dt_start = datetime.now().date() + timedelta(days=-1)
		self.dt_end = self.dt_start + timedelta(days = 1)
				
	def test_DeviceSQL(self):
		with mssql_data(self.conn, self.cr):	
			test_object = Start(self.cr, self.dt_start, self.dt_end, {"channel": []})
			sql = test_object.sql('new')['new']['deviceid']
			test_object = Metrics(self.cr, self.dt_start, self.dt_end, sql)
			test_object.get('deviceSQL')
			test_object = Start(self.cr, self.dt_start, self.dt_end)
			sql = test_object.sql('new')['new']['deviceid']
			test_object = Metrics(self.cr, self.dt_start, self.dt_end, sql)
			test_object.get('deviceSQL')			
			

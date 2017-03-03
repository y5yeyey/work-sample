# -*- coding: utf-8 -*-


from db_client import *

class Statistics(object):
	def __init__(self, cr, dt_start, dt_end, userkey_list=[0], device_list=['0'], sql=False):
		self.cr = cr
		self.dt_start = dt_start
		self.dt_end = dt_end
		self.userkey_list = userkey_list if len(userkey_list) > 0 else [0]
		self.device_list = device_list if len(device_list) > 0 else ['0']
		self._val = {}
		self._sql = {} if sql else None
		self.sqlquery = sql if sql else None
		self.fact_hui_mai_che_order_par_order_type_key = [1, 999]
	@property
	def val(self):
		return self._val
	@property
	def sql(self):
		return self._sql
	def get(self, *args):
		for method in args:
			sql = getattr(self, method)()
			if self._sql is None:
				self._val.update(self.getResult(sql, 'val', method))
			else:
				self._sql[method] = sql
	def getResult(self, sql, key, label_of_key):
		self.cr.execute(sql)
		all = fetch_all(self.cr)
		return {label_of_key: all[0][key] if all else 0}
	def device(self):
		if self.sqlquery:
			sql = "SELECT COUNT(DISTINCT TableStats_0.FDeviceID) AS val FROM ({sqlquery}) TableStats_0".format(
				sqlquery = self.sqlquery
			)
		return sql
	def user(self):
		if self.sqlquery:
			sql = "SELECT COUNT(DISTINCT TableStats_0.UserKey) as val FROM ({sqlquery}) TableStats_0".format(
				sqlquery = self.sqlquery
			)
		return sql
	def order(self):
		if self.userkey_list != [0]:
			sql = "select count(distinct OrderKey) as val from Example.Example_Table where UserKey in ({id}) and OrderCreateTime >= '{start}' and OrderCreateTime < '{end}';".format(
				id = ','.join(map(str, self.userkey_list)),
				start = self.dt_start,
				end = self.dt_end
			)
		elif self.device_list != ['0']:
			sql = "select count(distinct OrderKey) as val from Example.Example_Table where DeviceID in ('{id}') and OrderCreateTime >= '{start}' and OrderCreateTime < '{end}';".format(
				id = "','".join(map(str, self.device_list)),
				start = self.dt_start,
				end = self.dt_end
			)
		return sql
	def pay(self):
		if self.userkey_list != [0]:
			sql = "select count(distinct OrderKey) as val from Example.Example_Table where UserKey in ({id}) and PayTimeDateKey >= '{start}' and PayTimeDateKey < '{end}';".format(
				id = ','.join(map(str, self.userkey_list)),
				start = self.dt_start,
				end = self.dt_end
			)
		elif self.device_list != ['0']:
			sql = "select count(distinct OrderKey) as val from Example.Example_Table where DeviceID in ('{id}') and PayTimeDateKey >= '{start}' and PayTimeDateKey < '{end}';".format(
				id = "','".join(map(str, self.device_list)),
				start = self.dt_start,
				end = self.dt_end
			)
		return sql
	def deal(self):
		if self.userkey_list != [0]:
			sql = "select count(distinct OrderKey) as val from Example.Example_Table where UserKey in ({id}) and DealDateKey >= '{start}' and DealDateKey < '{end}';".format(
				id = ','.join(map(str, self.userkey_list)),
				start = self.dt_start,
				end = self.dt_end
			)
		elif self.device_list != ['0']:
			sql = "select count(distinct OrderKey) as val from Example.Example_Table where DeviceID in ('{id}') and DealDateKey >= '{start}' and DealDateKey < '{end}';".format(
				id = "','".join(map(str, self.device_list)),
				start = self.dt_start,
				end = self.dt_end
			)
		return sql
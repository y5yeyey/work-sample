# -*- coding: utf-8 -*-

import re
from db_client import *
import functools

class Cubes(object):
	"""
	@Cubes('<handler>')
	def new(self):
		pass
    """
	def __init__(self, *dimensions):
		self.dimensions = dimensions
		self.handlers = {
			"channel": self._channel,
			"version": self._version,
			"location": self._location
		}
	def _default(self, id, table):
		pass
	def _channel(self, id, table):
		channel_map = {"table": "Example.DimChannel", "field": "Code", "join": ["DeviceID", "DeviceID"]}
		if id == "userkey":
			channel_map["join"] = ["UserKey", "UserKey"]
			if table == "Example.Table":
				channel_map["join"] = ["UserKey", "OrderUserKey"]
		return channel_map
	def _version(self, id, table):
		return {"table": "[1.1.1.1].Example", "field": "VersionName", "join": ["VersionKey", "VersionKey"]}
	def _location(self, id, table):
		pass
	def __call__(self, fn):
		@functools.wraps(fn)
		def wrapper(cls, *args, **kw):
			def _input_clauses(input_sql):
				select_parts = [m.span() for m in re.finditer("SELECT", input_sql)]
				from_parts = [m.span() for m in re.finditer("FROM", input_sql)]
				where_parts = [m.span() for m in re.finditer("WHERE", input_sql)]
				cluase_parts = [select_parts[0][1], from_parts[0][0], from_parts[0][1], where_parts[0][0], where_parts[0][1]]
				select_clause = input_sql[cluase_parts[0] : cluase_parts[1]].strip(' ')
				from_clause = input_sql[cluase_parts[2] : cluase_parts[3]].strip(' ')
				table_name = from_clause.split(' ')[0]
				where_clause = input_sql[cluase_parts[4]: ].strip(' ')
				return {
					"select": [select_clause],
					"from": [from_clause],
					"join": [],
					"where": [where_clause],
					"table": table_name
				}
				
			def _update_clauses(input_clauses, id, dim, dim_vals, alias_cnt):
				if dim not in self.dimensions:
					raise NameError("'Cubes' has no handler named '%s', please check dim_vars" % dim)
				tb_info = self.handlers.get(dim, self._default)(id, input_clauses['table'])
				tb_info['alias'] = "u" + str(alias_cnt)
				input_clauses['select'].append(", {alias}.{field}".format(alias=tb_info['alias'], field=tb_info['field']))
				input_clauses['join'].append(" LEFT JOIN {tb} {alias} ON {alias}.{join} = u0.{u0_join}".format(tb=tb_info['table'], alias=tb_info['alias'], join=tb_info['join'][0], u0_join=tb_info['join'][1]))
				if dim_vals:
					input_clauses['where'].append(" AND u0.{u0_join} IN ('{vals}') ".format(u0_join=tb_info['join'][1], vals="','".join(dim_vals)))
				return input_clauses
				
			def _final_sql(id, dim_vars, sql):
				input_clauses = _input_clauses(sql)
				input_clauses = map(
					lambda (dim, val), alias_cnt:
					_update_clauses(input_clauses, id, dim, val, alias_cnt),
					dim_vars.items(),
					range(1, 1+len(dim_vars))
				)[0]
				return "{slt} {frm} {jon} {whe}".format(
					slt = "SELECT " + " ".join(input_clauses['select']),
					frm = "FROM " + " ".join(input_clauses['from']),
					jon = " ".join(input_clauses['join']),
					whe = "WHERE " + " ".join(input_clauses['where'])
				)
			
			def _output_sql_dict(input_sql_dict, dim_vars):
				return {
					id: _final_sql(id, dim_vars, sql) 
					for id, sql in input_sql_dict.items()
				} if dim_vars else input_sql_dict
				
			input_sql_dict = fn(cls, *args, **kw)
			return _output_sql_dict(input_sql_dict, cls.dim_vars)
			
		return wrapper
		
	
class Action(object):
	"""
	class newAction(Action):
		@Cubes('example_cube')
		def new(self):
			return {'deviceid': sql}		
    """
	def __init__(self, cr, dt_start, dt_end, dim_vars=None):
		self.cr, self.dt_start, self.dt_end = cr, dt_start, dt_end
		self.dim_vars = dim_vars
	def sql(self, *args):
		return {id: getattr(self, id)() for id in args}
	def _get(self, fn, *args):
		result = {}
		for method in args:
			for id, sql in getattr(self, method)().items():
				result[method] = fn(id, sql)
		return result
	def userkey(self, *args):
		def __addUserKey(id, sql):
			if id == 'userkey':
				try:
					self.cr.execute(sql)
					all = fetch_all(self.cr)
					return all if all else []
				except:
					raise Exception(sql)
		return self._get(__addUserKey, *args)
	def deviceid(self, *args):
		def _addDeviceID(id, sql):
			def _getDeviceSet(all):
				device_set = set([])
				if all:
					if len(all[0]) > 1:
						device_set = {}
						for line in all:
							key = tuple([val for field, val in line.items() if field != "DeviceID"])
							key = key[0] if len(key) == 1 else key
							val = line['DeviceID']
							if key in device_set:
								device_set[key].add(val)
							else:
								device_set[key] = set([val])
					else:
						device_set = set([line['DeviceID'] for line in all])
				return device_set
			if id == 'deviceid':
				try:
					self.cr.execute(sql)
					return _getDeviceSet(fetch_all(self.cr))
				except:
					raise Exception(sql)
		return self._get(_addDeviceID, *args)
		

class Activate(Action):
	@Cubes('channel', 'version')
	def new(self):
		sql = "SELECT u0.DeviceID FROM Example.DimChannel u0 WHERE u0.ActiveTime >= '{start}' AND u0.ActiveTime < '{end}'".format(start=self.dt_start, end=self.dt_end)
		return {"deviceid": sql}
	@Cubes('channel', 'version')
	def cumulative(self):
		sql = "SELECT u0.DeviceID FROM Example.DimChannel u0 WHERE u0.ActiveTime < '{end}'".format(end=self.dt_end)
		return {"deviceid": sql}
		

class Start(Action):
	@Cubes('channel', 'version')
	def new(self):
		sql = "SELECT DISTINCT u0.DeviceID FROM Example.FactDeviceStartRecord u0 WHERE u0.StartTime >= '{start}' AND u0.StartTime < '{end}' AND u0.DeviceID NOT IN (SELECT DISTINCT DeviceID FROM Example.FactDeviceStartRecord WHERE StartTime < '{start}')".format(start = self.dt_start, end = self.dt_end)
		return {"deviceid": sql}
	@Cubes('channel', 'version')
	def old(self):
		sql = "SELECT DISTINCT u0.DeviceID FROM Example.FactDeviceStartRecord u0 WHERE u0.StartTime >= '{start}' AND u0.StartTime < '{end}' AND u0.DeviceID IN (SELECT DISTINCT DeviceID FROM Example.FactDeviceStartRecord WHERE StartTime < '{start}')".format(start = self.dt_start, end = self.dt_end)
		return {"deviceid": sql}
	@Cubes('channel', 'version')
	def actual(self):
		sql = "SELECT DISTINCT u0.DeviceID FROM Example.FactDeviceStartRecord u0 WHERE u0.StartTime >= '{start}' AND u0.StartTime < '{end}'".format(start = self.dt_start, end = self.dt_end)
		return {"deviceid": sql}
	@Cubes('channel', 'version')
	def cumulative(self):
		sql = "SELECT DISTINCT u0.DeviceID FROM Example.FactDeviceStartRecord u0 WHERE u0.StartTime < '{end}'".format(end = self.dt_end)
		return {"deviceid": sql}

		
class Register(Action):
	@Cubes('channel')
	def new(self):
		sql_userkey = "SELECT u0.UserKey FROM [DataAnalyze].[dbo].[DimUserInfor_2] u0 WHERE u0.CreateTime >= '{0}' AND u0.CreateTime < '{1}' ".format(self.dt_start, self.dt_end)
		sql_deviceid = "SELECT DISTINCT u0.DeviceID FROM Example.DimChannel u0 WHERE u0.UserKey IN (SELECT UserKey FROM Example.DimUserInfor_2 WHERE CreateTime >= '{0}' AND CreateTime < '{1}')".format(self.dt_start, self.dt_end)
		return {"deviceid": sql_deviceid, "userkey": sql_userkey}
	@Cubes('channel')
	def cumulative(self):
		sql_userkey = "SELECT u0.UserKey FROM [DataAnalyze].[dbo].[DimUserInfor_2] u0 WHERE u0.CreateTime < '{0}'".format(self.dt_end)
		sql_deviceid = "SELECT DISTINCT u0.DeviceID FROM Example.DimChannel u0 WHERE u0.UserKey IN (SELECT UserKey FROM Example.DimUserInfor_2 WHERE CreateTime < '{0}')".format(self.dt_end)
		return {"deviceid": sql_deviceid, "userkey": sql_userkey}
		

class Order(Action):
	Action.C2B_order_type_key = 1111
	Action.example_key = [1, 1111]
	Action.order_type_key = ','.join(map(str, [Action.C2B_order_type_key]))
	@Cubes('channel')
	def new(self):
		sql_userkey = """
			SELECT DISTINCT u0.OrderUserKey AS UserKey 
			FROM Example.Table u0
			WHERE u0.OrderCreateTime >= '{start}' AND u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par})
			AND u0.OrderUserKey NOT IN (
				SELECT DISTINCT OrderUserKey 
				FROM Example.Table 
				WHERE OrderCreateTime < '{start}' 
				AND OrderUserKey IS NOT NULL 
				AND OrderTypeKey IN ({par})
			)
		""".format(
			start = self.dt_start, 
			end = self.dt_end, 
			par = self.order_type_key
		)
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID AS DeviceID 
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime >= '{start}' AND u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par}) 
			AND u0.DeviceID NOT IN (
				SELECT DISTINCT DeviceID 
				FROM Example.Table 
				WHERE OrderCreateTime < '{start}'  
				AND OrderTypeKey IN ({par})
			)
		""".format(
			start = self.dt_start, 
			end = self.dt_end, 
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
 	def old(self):
		sql_userkey = """
			SELECT DISTINCT u0.OrderUserKey AS UserKey 
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime >= '{start}' AND u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par}) 
			AND u0.OrderUserKey IN (
				SELECT DISTINCT OrderUserKey 
				FROM Example.Table 
				WHERE OrderCreateTime < '{start}' 
				AND OrderUserKey IS NOT NULL 
				AND OrderTypeKey IN ({par})
			)
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime >= '{start}' AND u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par}) 
			AND u0.DeviceID IN (
				SELECT DISTINCT DeviceID
				FROM Example.Table 
				WHERE OrderCreateTime < '{start}' 
				AND OrderTypeKey IN ({par})
			)
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def actual(self):
		sql_userkey = """
			SELECT DISTINCT u0.OrderUserKey AS UserKey 
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime >= '{start}' AND u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par})
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime >= '{start}' AND u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par})
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def cumulative(self):
		sql_userkey = """
			SELECT DISTINCT u0.OrderUserKey AS UserKey 
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par}) 
			AND u0.OrderUserKey IS NOT NULL
		""".format(
			end = self.dt_end,
			par = self.order_type_key
		)
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID AS DeviceID 
			FROM Example.Table u0 
			WHERE u0.OrderCreateTime < '{end}' 
			AND u0.ParOrderTypeKey IN ({par}) 
			AND u0.OrderUserKey IS NOT NULL
		""".format(
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
		

class Pay(Action):
	Action.C2B_order_type_key = 0
	Action.order_type_key = ','.join(map(str, [Action.C2B_order_type_key]))
	@Cubes('channel')
	def new(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.PayTimeDateKey >= '{start}' AND u0.PayTimeDateKey < '{end}'
			AND u0.DeviceID NOT IN (
				SELECT DISTINCT u0.DeviceID
				FORM Example.Table
				WHERE ParOrderTypeKey IN ({par})
				AND PayTimeDateKey < '{start}'
			)
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def old(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.PayTimeDateKey >= '{start}' AND u0.PayTimeDateKey < '{end}'
			AND u0.DeviceID IN (
				SELECT DISTINCT u0.DeviceID
				FORM Example.Table
				WHERE ParOrderTypeKey IN ({par}) 
				AND PayTimeDateKey < '{start}'
			)
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def actual(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.PayTimeDateKey >= '{start}' AND u0.PayTimeDateKey < '{end}'
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def cumulative(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.PayTimeDateKey < '{end}'
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
		
		
class Deal(Action):
	Action.C2B_order_type_key = 0
	Action.order_type_key = ','.join(map(str, [Action.C2B_order_type_key]))
	@Cubes('channel')
	def new(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.IfDeal = 1 
			AND u0.DealDateKey >= '{start}' AND u0.DealDateKey < '{end}'
			AND u0.DeviceID NOT IN (
				SELECT DISTINCT u0.DeviceID
				FORM Example.Table
				WHERE ParOrderTypeKey IN ({par}) 
				AND IfDeal = 1
				AND DealDateKey < '{start}'
			)
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def old(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.IfDeal = 1 
			AND u0.DealDateKey >= '{start}' AND u0.DealDateKey < '{end}'
			AND u0.DeviceID IN (
				SELECT DISTINCT u0.DeviceID
				FORM Example.Table
				WHERE ParOrderTypeKey IN ({par}) 
				AND IfDeal = 1
				AND DealDateKey < '{start}'
			)
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def actual(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.IfDeal = 1 
			AND u0.DealDateKey >= '{start}' AND u0.DealDateKey < '{end}'
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	@Cubes('channel')
	def cumulative(self):
		sql_userkey = ""
		sql_deviceid = """
			SELECT DISTINCT u0.DeviceID
			FROM Example.Table u0 
			WHERE u0.ParOrderTypeKey IN ({par}) 
			AND u0.IfDeal = 1 
			AND u0.DealDateKey < '{end}'
		""".format(
			start = self.dt_start,
			end = self.dt_end,
			par = self.order_type_key
		)
		return {'userkey': sql_userkey, 'deviceid': sql_deviceid}
	
def Group(fn):
	"""
	@Group
	def example():
		return sql
	"""
	def wrapper(self, *args, **kw):		
		
		def _format_sql(*args):
			return tuple([sql.replace("\n", " ").replace("\t", " ") for sql in args])
		
		def _getSQLFromID(sql):
			return sql.format(
				dt_start = self.dt_start,
				dt_end = self.dt_end,
				input = self.input
			)
		
		def _getSQLFromSQL(stat_sql, user_sql):
			def _get_not_id_keys(user_sql):
				select_keywords = user_sql.replace("DISTINCT", "").split(' FROM ')[0][6:].replace(" ", "").split(",")
				return [
					field.strip(' ').split('.')[1] \
					for field in select_keywords \
					if not re.compile(r"(UserKey|DeviceID)$").search(field)
				]
			def _get_groupby_items(not_id_keys):
				if not_id_keys:
					groupby_keys = ", s0.{0}".format(", s0.".join(not_id_keys))
					groupby_clause = "GROUP BY s0.{0}".format(", s0.".join(not_id_keys)) if len(groupby_keys) > 1 else ""
					return groupby_keys, groupby_clause
				return "", ""
			stat_sql, user_sql = _format_sql(stat_sql, user_sql)
			not_id_keys = _get_not_id_keys(user_sql)
			groupby_keys, groupby_clause = _get_groupby_items(not_id_keys)
			return sql.format(
				dt_start = self.dt_start,
				dt_end = self.dt_end,
				input = user_sql,
				groupby_keys = groupby_keys,
				groupby_clause = groupby_clause
			)
	
		sql = fn(self, *args, **kw)
		return _getSQLFromID(sql) if self.input else _getSQLFromSQL(sql, self.query)
	
	return wrapper	

	
class Metrics(object):
	"""
	test_object = Metrics(cr, dt_start, dt_end, sql)
	sql = test_object.sql('deviceSQL')
	data = test_object.get('deviceSQL')
	"""
	def __init__(self, cr, dt_start, dt_end, input):
		def _getInput(input):
			output, sql = None, None
			if type(input) == str:
				sql = input  # SQL
			elif type(input) == list:
				output = "'" + "','".join(map(str, input)) + "'"
				if output.isdigit():
					output = ",".join(map(str, input))  # ID
			return output, sql
		self.cr, self.dt_start, self.dt_end = cr, dt_start, dt_end
		self.input, self.query = _getInput(input)
		self.example_key = [1, 10000]
	
	def _get_sql(self, *args):
		return {method: getattr(self, method)() for method in args}
	
	def sql(self, *args):
		return self._get_sql(*args) if self.query else {}
	
	def get(self, *args):
		self.__val = {}		
		for method, sql in self._get_sql(*args).items():
			try:
				self.cr.execute(sql)
				all = fetch_all(self.cr)
				self.__val[method] = all if all else []
			except:
				raise Exception(sql)
		return self.__val
	
	@Group
	def deviceSQL(self):
		sql = "SELECT COUNT(DISTINCT s0.DeviceID) AS Metric {groupby_keys} FROM ({input}) s0 {groupby_clause}"
		return sql
	@Group
	def deviceID(self):
		sql = "SELECT COUNT(DISTINCT s0.DeviceID) AS Metric FROM Example.DimChannel s0 WHERE s0.DeviceID in ({input})"
		return sql
	@Group
	def userSQL(self):
		sql = "SELECT COUNT(DISTINCT s0.UserKey) AS Metric {groupby_keys} FROM ({input}) s0 {groupby_clause}"
		return sql
	@Group
	def userID(self):
		pass
	@Group
	def orderUserSQL(self):
		sql = "SELECT COUNT(DISTINCT s1.OrderKey) AS Metric {groupby_keys} FROM ({input}) s0 INNER JOIN Example.Table s1 ON s0.OrderUserKey = s1.UserKey WHERE s1.OrderCreateTime >= '{dt_start}' AND s1.OrderCreateTime < '{dt_end}' AND ParOrderTypeKey in (0)  {groupby_clause}"
		return sql
	@Group
	def orderUserID(self):
		sql = "SELECT COUNT(DISTINCT"
	@Group
	def orderDeviceSQL(self):
		sql = "SELECT COUNT(DISTINCT s1.OrderKey) AS Metric {groupby_keys} FROM ({input}) s0 INNER JOIN Example.Table s1 ON s0.DeviceID = s1.DeviceID WHERE s1.OrderCreateTime >= '{dt_start}' AND s1.OrderCreateTime < '{dt_end}' AND ParOrderTypeKey in (0) {groupby_clause}"
		return sql
	@Group
	def orderDeviceID(self):
		sql = "SELECT COUNT(DISTINCT s0.OrderKey) AS Metric FROM Example.Table s0 WHERE s0.OrderCreateTime >= '{dt_start}' AND s0.OrderCreateTime < '{dt_end}' AND ParOrderTypeKey in (0) AND s0.DeviceID IN ({input})"
		return sql
	@Group
	def payUserSQL(self):
		sql = "SELECT COUNT(DISTINCT s1.OrderKey) AS Metric {groupby_keys} FROM ({input}) s0 INNER JOIN Example.Table s1 ON s0.UserKey = s1.OrderUserKey WHERE s1.PayTimeDateKey >= '{dt_start}' AND s1.PayTimeDateKey < '{dt_end}' AND ParOrderTypeKey in (0) {groupby_clause}"
		return sql
	@Group
	def payUserID(self):
		pass
	@Group
	def payDeviceSQL(self):
		sql = "SELECT COUNT(DISTINCT s1.OrderKey) AS Metric {groupby_keys} FROM ({input}) s0 INNER JOIN Example.Table s1 ON s0.DeviceID = s1.DeviceID WHERE s1.PayTimeDateKey >= '{dt_start}' AND s1.PayTimeDateKey < '{dt_end}' AND ParOrderTypeKey in (0) {groupby_clause}"
		return sql
	@Group
	def payDeviceID(self):
		sql = "SELECT COUNT(DISTINCT s0.OrderKey) AS Metric FROM Example.Table s0 WHERE s0.PayTimeDateKey >= '{dt_start}' AND s0.PayTimeDateKey < '{dt_end}' AND ParOrderTypeKey in (0) AND s0.DeviceID IN ({input})"
		return sql
	@Group
	def dealUserSQL(self):
		sql = "SELECT COUNT(DISTINCT s1.OrderKey) AS Metric {groupby_keys} FROM ({input}) s0 INNER JOIN Example.Table s1 ON s0.UserKey = s1.OrderUserKey WHERE s1.IfDeal = 1 AND  s1.DealDateKey >= '{dt_start}' AND s1.DealDateKey < '{dt_end}' AND ParOrderTypeKey in (0) {groupby_clause}"
		return sql
	@Group
	def dealUserID(self):
		pass
	@Group
	def dealDeviceSQL(self):
		sql = "SELECT COUNT(DISTINCT s1.OrderKey) AS Metric {groupby_keys} FROM ({input}) s0 INNER JOIN Example.Table s1 ON s0.DeviceID = s1.DeviceID WHERE s1.IfDeal = 1 AND s1.DealDateKey >= '{dt_start}' AND s1.DealDateKey < '{dt_end}' AND ParOrderTypeKey in (0) {groupby_clause}"
		return sql
	@Group
	def dealDeviceID(self):
		sql = "SELECT COUNT(DISTINCT s0.OrderKey) AS Metric FROM Example.Table s0 WHERE s0.IfDeal = 1 AND s0.DealDateKey >= '{dt_start}' AND s0.DealDateKey < '{dt_end}' AND ParOrderTypeKey in (0) AND s0.DeviceID IN ({input})"
		return sql		


# -*- coding: utf-8 -*-

from db_client import *
from datetime import timedelta
import simplejson as json

class EventRecord(object):

	def __init__(self, cr, dt_start, dt_end):
		self.cr = cr
		self.dt_start = dt_start
		self.dt_end = dt_end
	
	def _parseParaValue(self, line):
		log_id = line['LogId']
		event_name = line['EventName']
		event_time = line['EventTime']
		event_count = line['EventCount']
		event_tag = line['EventTag'] if line['EventTag'] else "{}"
		event_tag = json.loads( event_tag.replace("\\n", "").replace(" ", "") )
		return log_id, event_name, event_time, event_count, event_tag

	def _filterEventVal(self, event_name, event_key, event_val):
		if event_name == u"car-param-comp-detail" and event_key == u'carIds':
			event_val = event_val.split(',')
		if event_key == u'userId':
			event_val = None if event_val == "-1" else event_val
		elif event_key == u'trackerId':
			event_val = None if event_val == "" else event_val
		return event_val
		
	def _getSQLVal(self, log_id, event_name, event_time, event_count, event_key, event_val):
		return u"""
			(
				'{log_id}',
				'{event_name}',
				'{event_time}',
				{event_count},
				{event_key},
				{event_val},
				GETDATE()
			)
		""".format(
			log_id = log_id,
			event_name = event_name,
			event_time = event_time,
			event_count = event_count,
			event_key = "'" + event_key + "'" if event_key else "NULL",
			event_val = "'" + event_val + "'" if event_val else "NULL"
		)
	
	def _getSQLHead(self):
		return u"""
			INSERT INTO [Example].[dbo].[FactEventRecord](
				[LogID],
				[EventName],
				[EventTime],
				[EventCount],
				[EventKey],
				[EventVal],
				[LoadTime]
			) VALUES 
		"""
		
	def _getRecord(self):
		sql = """
			SELECT
				tb2.LogId,
				tb2.ParamValue AS EventName,
				CONVERT(
					DATETIME, 
					SUBSTRING(tb3.ParamValue, 1, 4) + '-' + 
					SUBSTRING(tb3.ParamValue, 5, 2) + '-' + 
					SUBSTRING(tb3.ParamValue, 7, 2) + ' ' + 
					SUBSTRING(tb3.ParamValue, 9, 2) + ':' + 
					SUBSTRING(tb3.ParamValue, 11, 2) + ':' + 
					SUBSTRING(tb3.ParamValue, 13, 2)
				) AS EventTime,
				tb4.ParamValue AS EventCount,
				tb5.ParamValue AS EventTag
			FROM [1.1.1.1].[LogDB].[LGC].[AppLog] tb1
			LEFT JOIN [1.1.1.1].[LogDB].[HIS].AppLogParam tb2
				ON tb1.LogId = tb2.LogId AND tb2.ParamKey = 'EventName'
			LEFT JOIN [1.1.1.1].[LogDB].[HIS].AppLogParam tb3
				ON tb1.LogId = tb3.LogId AND tb3.ParamKey = 'EventTime'
			LEFT JOIN [1.1.1.1].[LogDB].[HIS].AppLogParam tb4
				ON tb1.LogId = tb4.LogId AND tb4.ParamKey = 'EventCount'
			LEFT JOIN [1.1.1.1].[LogDB].[HIS].AppLogParam tb5
				ON tb1.LogId = tb5.LogId AND tb5.ParamKey = 'EventTag'
			WHERE tb1.OpTypeId = 3 
			  AND tb1.CreateTime >= '{dt_start}' AND tb1.CreateTime < '{dt_end}'
			  AND tb2.ParamValue IS NOT NULL
			  AND tb3.ParamValue IS NOT NULL
			  AND tb4.ParamValue IS NOT NULL
			ORDER BY tb1.LogTime
		""".format(
			dt_start = self.dt_start,
			dt_end = self.dt_end
		)
		self.cr.execute(sql)
		all = fetch_all(self.cr)
		return (line for line in all) if all else None
			
	def insertRecord(self):
		all = self._getRecord()
		if not all:
			return None
		sql = []
		for line in all:
			log_id, event_name, event_time, event_count, event_tag = self._parseParaValue(line)
			if event_tag:
				for event_key, event_val in event_tag.items():
					event_val = self._filterEventVal(event_name, event_key, event_val)
					if type(event_val) == list:
						for each_event_val in event_val:
							sql.append(self._getSQLVal(log_id, event_name, event_time, event_count, event_key, each_event_val))
					else:
						sql.append(self._getSQLVal(log_id, event_name, event_time, event_count, event_key, event_val))
			else:
				event_key, event_val = None, None
				sql.append(self._getSQLVal(log_id, event_name, event_time, event_count, event_key, event_val))
			if len(sql) > 1000:
				self.cr.execute(self._getSQLHead() + ",".join(sql[0:1000]))
				self.cr.commit()
				sql = sql[1000:]
		if sql:
			self.cr.execute(self._getSQLHead() + ",".join(sql))
			self.cr.commit()
							

def updateEventRecord(cr, dt_start, dt_end):
	for i in range(0, (dt_end - dt_start).days):
		dt_left = dt_start + timedelta(days=i)
		dt_right = dt_left + timedelta(days=1)
		FactEventRecord = EventRecord(cr, dt_left, dt_right)
		FactEventRecord.insertRecord()
		
	
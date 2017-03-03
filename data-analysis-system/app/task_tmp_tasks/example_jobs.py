# -*- coding: utf-8 -*-

from db_client import *
from user import *
from datetime import datetime, timedelta

def tmp_job1(cr, dt_start, dt_end):
	print decode_line([u'date', u'channel', u'additive_user', u'order_number', u'payment_number', u'deals_number'])
	for _ in range(0, (dt_end - dt_start).days):
		dt_left = dt_start + timedelta(days=i)
		dt_right = dt_left + timedelta(days=1)
		obj_activate = Activate(cr, dt_left, dt_right, DIM_VARIABLES)
		obj_activate.get('new')
		deviceid_channel = {}
		sql = """
			select tb1.DeviceID, tb2.Code
			from (
				select DeviceID, min(StartTime) as ActivateTime
				from TableName group by DeviceID
			) tb1 left join TableName2 tb2 on tb1.DeviceID = tb2.DeviceID
			where tb1.DeviceID in ('{device_list}');
			""".format(
			device_list = "','".join(map(str, obj_activate.deviceid['new']))
		)
		cr.execute(sql)
		all = fetch_all(cr)
		if all:
			for e in all:
				channel = e['Code']
				device = e['DeviceID']
				if channel in deviceid_channel:
					deviceid_channel[channel].append(device)
				else:
					deviceid_channel[channel] = [device]
			for channel, deviceid_list in deviceid_channel.items():
				echo_list = [dt_left.date(), channel, len(deviceid_list)]
				ss = Metrics(cr, dt_left, dt_end, device_list=deviceid_list)
				ss.get('order', 'pay', 'deal')
				echo_list += [ss.val['order'], ss.val['pay'], ss.val['deal']]
				print decode_line(echo_list)

def tmp_job2(cr, dt_start, dt_end):
	print decode_line([u'date', u'new_user', u'new_user_order_number', u'new_user_deal_number'])
	for _ in range(0, (dt_end - dt_start).days):
		dt_left = dt_start + timedelta(days=i)
		dt_right = dt_left + timedelta(days=1)
		new_user = Activate(cr, dt_left, dt_right)
		device_set = new_user.deviceid('new')['new']
		order_user_set =  device_set & Order(cr, dt_left, dt_end).deviceid('actual')['actual']
		pay_user_set = device_set & Pay(cr, dt_left, dt_end).deviceid('actual')['actual']
		deal_user_set = device_set & Deal(cr, dt_start, dt_end).deviceid('actual')['actual']
		echo_list = [dt_left.date(), len(device_set), len(order_user_set), len(pay_user_set), len(deal_user_set)]
		print decode_line(echo_list)
		
		
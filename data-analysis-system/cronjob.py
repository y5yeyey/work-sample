# -*- coding: utf-8 -*-

import os
import sys
import logging
import subprocess
from datetime import datetime
import argparse

LOGS = os.path.dirname(os.path.realpath(__file__)) + "\\logs"

def job_pipeline(log_file, app_name_list, dt_list):
	logging.basicConfig(
		filename = log_file,
		format = '%(asctime)s [%(levelname)s] %(message)s',
		level = logging.DEBUG
	)
	logging.info('[START PIPELINE] Started [{0}] Jobs'.format(len(app_name_list)))
	suc_job_cnt = 0
	for index, app_name in enumerate(app_name_list):
		dt = None if dt_list[index] == 'NULL' else dt_list[index]
		cmd = "python run.py --app \"{0}\"".format(app_name)
		if dt:
			cmd = "python run.py --app \"{0}\" --dt {1}".format(app_name, dt)
		app_order = index + 1
		logging.info('Began #[{0}] {1}'.format(app_order, app_name))
		logging.debug('Executed Command: {0}'.format(cmd))
		try:
			subprocess.check_output(
				cmd,
				shell = True,
				stderr = subprocess.STDOUT
			)
			suc_job_cnt += 1
			logging.info('Finished #[{0}]'.format(app_order))
		except subprocess.CalledProcessError, ex:
			logging.error('\n{0}'.format(ex.output.strip('\n')))
			logging.info('Finished #[{0}] With Error'.format(app_order))
	logging.info(
		'[END PIPELINE] Ended With [{0}/{1}] Succeeded'.format(
			suc_job_cnt, len(app_name_list)
		)
	)
		
def arg_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument("--bat", help="Example: daily.bat. Ran from which batch script.")
	parser.add_argument("--jobs", help="Example: \"app_1 app_2 app_3\". A list of app names.")
	parser.add_argument("--dt", help="Example: \"dt_1 dt_2 dt_3\". A list of dt's which app names will use.")
	args = parser.parse_args()
	bat_file = args.bat if args.bat else None
	job_list = args.jobs.split(' ')
	dt_list = args.dt.split(' ')
	return bat_file, job_list, dt_list
	
def cron():
	bat_file, job_list, dt_list = arg_parser()
	log_file = "{0}\\{1}.log".format(LOGS, bat_file)
	job_pipeline(log_file, job_list, dt_list)

if __name__ == "__main__":
	try:
		cron()
	except:
		raise
		sys.exit(1)
		
		
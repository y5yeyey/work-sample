# -*- coding: utf-8 -*-

import os
import sys
import argparse
import imp
from datetime import datetime

GLOBAL = os.path.dirname(os.path.realpath(__file__))
CONF = GLOBAL + "\\conf"
PKG = GLOBAL + "\\pkg"
APP = GLOBAL + "\\app"
TEST = GLOBAL + "\\tests"
DATA = GLOBAL + "\\data"

def add_sys_path():
	# Windows - sys.path.append(";".join([GLOBAL, CONF, PKG]))
	sys.path.insert(1, GLOBAL)
	sys.path.insert(1, CONF)
	sys.path.insert(1, PKG)
	sys.path.insert(1, TEST)
	
def arg_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument("--app")
	parser.add_argument("--dt")
	args = parser.parse_args()
	app = args.app if args.app else '.'
	dt = datetime.strptime(args.dt, "%Y-%m-%d") if args.dt else datetime.today().date()
	return app, dt

def run():
	add_sys_path()
	app, dt = arg_parser()
	sys.path.insert(1, '{0}\\{1}'.format(APP, app))
	data_path = '{0}\\{1}'.format(DATA, app)
	module = imp.load_source('app', '{0}\\{1}\\app.py'.format(APP, app))
	if not os.path.exists(data_path):
		os.makedirs(data_path)
	module.main(dt, data_path)
			
		
if __name__ == "__main__":
	try:
		run()
	except:
		raise
		sys.exit(1)
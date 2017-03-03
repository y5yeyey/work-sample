# -*- coding: utf-8 -*-

import unittest
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
	parser.add_argument("--pkg")
	args = parser.parse_args()
	pkg = args.pkg if args.pkg else None
	return pkg


if __name__ == "__main__":
	try:
		# config test cases which you would like to run unittests
		# from tests.pkg_channel_tasks import *
		add_sys_path()
		from tests.pkg_user import *
		unittest.main()
	except:
		raise
		sys.exit(1)
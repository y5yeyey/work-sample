# -*- coding: utf-8 -*-

import pyodbc  # Python SQL Server Client
import pymongo  # Python MongoDB Client
import pandas as pd
import codecs
import os
import re

UTF_CODE = "utf-8"
GB_CODE = "gb2312"

def decode_line(data_list):
    return "\t".join(map(str, data_list)).decode(UTF_CODE).encode(GB_CODE)

def parseSQL(sql_file):
    with codecs.open(sql_file, 'r', "UFT_CODE") as f:
        sql = []
        locker = None
        for line in f.readlines():
            if locker:
                continue
            each_line = line.replace('\n', '').replace('\t', '').replace('\r', '')
            if "--" in each_line:
                each_line = each_line[0:each_line.index('--')]
            if len(each_line) > 0:
                sql.append(each_line)
    return " ".join([s.encode(UTF_CODE) for s in sql]).decode(UTF_CODE).encode(GB_CODE)

def joinSQL(sql_context):
    declare_lines = []
    sql = [sql_context]
    if not re.search('.sql$', sql_context):
        return declare_lines, sql
    with codecs.open(sql_context, 'r', UTF_CODE) as f:
        sql = []
        declare_lines = []
        for line in f.readlines():
            each_line = line.replace('\n', '').replace('\t', '')
            if "--" in each_line:
                each_line = each_line[0:each_line.index('--')]
            elif "/*" and "*/" in each_line:
                each_line = " "
            elif re.search('^DECLARE', each_line.strip(' ')):
                declare_lines.append(each_line)
                each_line = " "
            sql.append(each_line)
    return declare_lines, sql

def exec_sql_programs(cr, current_path, *args):
    declare, query = [], []
    for index, each_sql_file in enumerate(args):
        declare_part, sql = joinSQL(current_path + '\\' + each_sql_file)
        if index + 1 < len(args):
            if index == 0:
                sql.insert(0, 'WITH tb{0} AS ( '.format(index+1))
            else:
                sql.insert(0, 'tb{0} AS ( '.format(index+1))
            if index + 2 == len(args):
                sql.append(') ')
            else:
                sql.append('), ')
        declare += declare_part
        query += sql
    cmd = "{0} {1}".format(" ".join(declare), " ".join(query)).decode(UTF_CODE)
    cr.execute(cmd)
    return fetch_all(cr)
	
def mssql_client(db_config):
    connect_msg = "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" \
                  %(db_config['HOST'], db_config['DB'], db_config['USER'], db_config['PW'])
    conn = pyodbc.connect(connect_msg, autocommit=True)
    cr = conn.cursor()
    return conn, cr
	
class mssql_data(object):
    def __init__(self, conn, cr):
        self.conn = conn
        self.cr = cr
    def __enter__(self):
        return self.cr
    def __exit__(self, type, value, traceback):
        self.cr.close()
       self.conn.close()

def fetch_all(cr):
    return  ( dict(zip(zip(*cr.description)[0], row)) for row in cr.fetchall() )
	
def mongo_client(db_config):
    mongo_user, mongo_pw, mongo_host, mongo_db = db_config['USER'], db_config['PW'], db_config['HOST'], db_config['DB']
    client = pymongo.MongoClient(mongo_host, 27017)
    db_auth = client['admin']
    db_auth.authenticate(mongo_user, mongo_pw)
    db = client[mongo_db]
    return client, db
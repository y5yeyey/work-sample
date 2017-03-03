# -*- coding: utf-8 -*-

def ConfigSetting(user, pw, host, *dbs):
	return {db: dict({'USER': user, 'PW': pw, 'HOST': host}.items() + {'DB': db}.items()) for db in dbs}


"""
Database Config
"""

CONF_DB = ConfigSetting(
	"DB_NAME1",
	"DB_NAME2",
	"DB_NAME3",
	"DB_NAME4"
)


"""
MongoDB Config
"""

CONF_MONGO = {
	'MONGODB': {
		'USER': 'username',
		'PW': 'password',
		'HOST': "1.1.1.1",
		'DB': 'dbname'
	}
}
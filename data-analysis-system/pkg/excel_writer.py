# -*- coding: utf-8 -*-

import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def writeExcel(filename, sheetname, df):
	writer = pd.ExcelWriter(filename)
	df.to_excel(writer, sheetname, index=False, encoding="utf-8")
	writer.save()
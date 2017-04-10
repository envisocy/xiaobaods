#!usr/bin/env python3
#! -*- coding: gbk -*-
import xiaobaods_function as fc
if __name__ == "__main__":
	try:
		import sys
		argv = sys.argv[1]
		argv = eval(argv)
	except:
		argv ={}
print(argv)
fc.xiaobaods_s01(date=argv.get("date",""),category=argv.get("category","牛仔裤"),length=argv.get("length",7),SQL=argv.get("SQL","xiaobaods"),table=argv.get("table","bc_searchwords_hotwords"),variable=argv.get("variable",""),debug=argv.get("debug",0),fillna=argv.get("fillna",""),path=argv.get("path",""))
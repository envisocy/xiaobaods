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
fc.xiaobaods_c(date=argv.get("date",""),category=argv.get("category","牛仔裤"),classification=argv.get("classification","款式"),attributes=argv.get("attributes","铅笔裤"),length=argv.get("length",7),SQL=argv.get("SQL","xiaobaods"),variable=argv.get("variable","热销排名"),debug=argv.get("debug",0),fillna=argv.get("fillna",""),path=argv.get("path",""),keyword=argv.get("keyword","日期："))
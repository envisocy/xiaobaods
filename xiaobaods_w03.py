#!usr/bin/env python3
#! -*- coding: utf-8 -*-
import xiaobaods_function as fc
if __name__ == "__main__":
    try:
        import sys
        argv = sys.argv[1]
        argv = eval(argv)
    except:
        argv ={}
fc.xiaobaods_w03(date=argv.get("date",""),category=argv.get("category","牛仔裤"),length=argv.get("length",7),SQL=argv.get("SQL","xiaobaods"),choice=argv.get("choice","热搜核心词"),variable=argv.get("variable","排名"),fillna=argv.get("fillna",""),debug=argv.get("debug",0),path=argv.get("path",""))
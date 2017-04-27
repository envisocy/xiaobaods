#!usr/bin/env python3
#! -*- coding: gbk -*-
# SQL_msg
from dateutil.parser import parse
import numpy as np
import pymysql
import datetime
import pandas as pd
import configparser
import time
import sys,os
def conftodict(filename,path=""):
    # 2017-04-11 添加脚本路径锁定
    if path=="":
        path = os.path.dirname(__file__)+"/"
    dic={}
    cp = configparser.SafeConfigParser()
    cp.read(path+filename)
    s = cp.sections()
    for i in s:
        dic[i]={}
        for j in cp.options(i):
            dic[i][j]=cp.get(i,j)
    return dic
SQL_msg = conftodict("xiaobaods_SQL.conf")
def xiaobaods_a(date="",category="牛仔裤",length=7,SQL="xiaobaods",table="bc_attribute_granularity_sales",variable="热销排名",fillna="",debug=0,path="",keyword="日期:"):
    # 2017-04-11 添加keyword隐藏参数：'日期:'
    # 2017-04-12 修复可能引起数据库检索合并重复值的BUG
    time_s = time.time()
    latest_date=datetime.datetime.today().date()-datetime.timedelta(1)
    if category not in ["牛仔裤","打底裤","休闲裤"]:
        category="牛仔裤"
    if length>14 or length<3:
        length=7
    if SQL not in SQL_msg:
        SQL=SQL_msg[0]
    if table not in ["bc_attribute_granularity_sales","bc_attribute_granularity_visitor"]:
        table="bc_attribute_granularity_sales"
    if date=="":
        date = latest_date
    else:
        date=parse(date).date()  # 修改日期格式
    if table=="bc_attribute_granularity_sales":
        sql_select_f = "SELECT CT.`主图缩略图`,CT.`热销排名`,CT.`商品信息`,CT.`所属店铺`,CT.`支付子订单数`,CT.`交易增长幅度`,CT.`支付转化率指数`,CT.`宝贝链接`,CT.`店铺链接`,CT.`查看详情`,CT.`同款货源`"
        if variable not in ["热销排名","支付子订单数","交易增长幅度","支付转化率指数"]:
            variable="热销排名"
    elif table=="bc_attribute_granularity_visitor":
        sql_select_f = "SELECT CT.`主图缩略图`,CT.`热销排名`,CT.`商品信息`,CT.`所属店铺`,CT.`流量指数`,CT.`搜索人气`,CT.`支付子订单数`,CT.`宝贝链接`,CT.`店铺链接`,CT.`查看详情`,CT.`同款货源`"
        if variable not in ["热销排名","流量指数","搜索人气","支付子订单数"]:
            variable="热销排名"
    # Try to connect with the mysql and back a date which minimum.
    try:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        cursor = conn.cursor()
        cursor.execute("SELECT min(`日期`),max(`日期`) from "+table+" where `类目`='"+category+"';")
        date_limit = cursor.fetchall()
        date_floor = date_limit[0][0]
        date_ceiling = date_limit[0][1] 
        cursor.close()
        conn.close()
    except Exception as e:
        return e
    if date > latest_date:
        date = latest_date
    if date > date_ceiling:
        date = date_ceiling
    if date < date_floor + datetime.timedelta(length-1):
        date = date_floor + datetime.timedelta(length-1)
    # Main program.
    sql_select_m=""
    for i in range(length):
        sql_select_m += ",MAX(CASE ST.日期 WHEN "+str(date - datetime.timedelta(length-i-1)).replace("-","")+" THEN ST."+variable+" ELSE NULL END) AS `日期："+str(date - datetime.timedelta(length-i-1)).replace("-","")+"` "
    sql_select_e="FROM "+table+" AS CT LEFT JOIN "+table+" AS ST ON SUBSTRING(CT.`宝贝链接` ,- 12) = SUBSTRING(ST.`宝贝链接` ,- 12) WHERE CT.`日期` = "+str(date).replace("-","")+" AND CT.类目 = '"+category+"' AND ST.日期 >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.类目 = '"+category+"' GROUP BY CT.`热销排名`,CT.`"+variable+"` ORDER BY CT.`热销排名`;"
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    conn.close()
    if fillna != "drop":
        df = df.fillna(fillna)
    else:
        df.dropna(inplace=True)
    if debug not in [1,2,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 8:
        return df
    elif debug== 2:
        print ("- Running time：%.4f s"%(time.time()-time_s))
        print("- date：%r \n- category：%r \n- length：%r \n- SQL：%r \n- table: %r \n- variable：%r \n- debug：%r \n- path: %r"%(str(date),category,str(length),SQL,table,variable,debug,path))
    elif debug== 1:
        print ("- Running time：%.4f s"%(time.time()-time_s))
        print( "  SQL_choice: %r \n- category: %r \n- length: %r \n- date: %r \n- SQL: %r"%(SQL,category,str(length),str(date),sql_select_f+sql_select_m+sql_select_e))
    elif debug==9:
        import os
        print ("- Running time：%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="【数据组】["+table.split("_")[-1]+"_"+category+"_Top500"+"]"+variable+"_"+datetime.datetime.strftime(date,"%m%d")+"-"+str(length)+"_"+SQL+".csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> 输出CSV文件：",path,",",csv_filename)
        except Exception as e:
            print("> 输出CSV文件失败，错误原因：",e)
def xiaobaods_w(date="",category="牛仔裤",length=7,SQL="xiaobaods",choice="热搜核心词",variable="排名",fillna="",debug=0,path="",keyword="日期:"):
    # 2017-04-11 修补fillna的BUG，添加keyword隐藏参数：'日期:'
    # 2017-04-12 修复可能引起数据库检索合并重复值的BUG
    # 2017-04-13 Add dubug=7 Return paramter
    time_s = time.time()
    latest_date=datetime.datetime.today().date()-datetime.timedelta(1)
    choice_list = {"热搜修饰词":{"table":"bc_searchwords_hotwords","variable":("搜索人气","相关搜索词数","点击率","点击人气","支付转化率","直通车参考价")},
                  "热搜品牌词":{"table":"bc_searchwords_hotwords","variable":("搜索人气","相关搜索词数","点击率","点击人气","支付转化率","直通车参考价")},
                  "热搜搜索词":{"table":"bc_searchwords_hotwords","variable":("搜索人气","商城点击占比","点击率","点击人气","支付转化率","直通车参考价")},
                  "热搜核心词":{"table":"bc_searchwords_hotwords","variable":("搜索人气","相关搜索词数","点击率","点击人气","支付转化率","直通车参考价")},
                  "热搜长尾词":{"table":"bc_searchwords_hotwords","variable":("搜索人气","商城点击占比","点击率","点击人气","支付转化率","直通车参考价")},
                  "飙升修饰词":{"table":"bc_searchwords_risewords",
                           "variable":("相关搜索词数","搜索人气","词均搜索增长幅度","点击人气","支付转化率","直通车参考价")},
                  "飙升品牌词":{"table":"bc_searchwords_risewords",
                           "variable":("相关搜索词数","搜索人气","词均搜索增长幅度","点击人气","支付转化率","直通车参考价")},
                  "飙升搜索词":{"table":"bc_searchwords_risewords",
                           "variable":("搜索增长幅度","搜索人气","点击率","点击人气","支付转化率","直通车参考价")},
                  "飙升核心词":{"table":"bc_searchwords_risewords",
                           "variable":("相关搜索词数","搜索人气","词均搜索增长幅度","点击人气","支付转化率","直通车参考价")},
                  "飙升长尾词":{"table":"bc_searchwords_risewords",
                           "variable":("搜索增长幅度","搜索人气","点击率","点击人气","支付转化率","直通车参考价")},}
    if category not in ["牛仔裤","打底裤","休闲裤"]:
        category="牛仔裤"
    if length>30 or length<3:
        length=7
    if SQL not in SQL_msg:
        SQL=SQL_msg[0]
    if choice not in choice_list:
        choice = "热搜修饰词"
    if variable not in choice_list[choice]["variable"]:
        variable="排名"
    if date=="":
        date = latest_date
    else:
        date=parse(date).date()  # 修改日期格式
    # Try to connect with the mysql and back a date which minimum.
    try:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"],passwd=SQL_msg[SQL]["passwd"],
                               charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        cursor = conn.cursor()
        cursor.execute("SELECT min(`日期`),max(`日期`) from "+choice_list[choice]["table"]+" where `类目`='"+category+"' and `字段`='"+choice+"';")
        date_limit = cursor.fetchall()
        date_floor = date_limit[0][0]
        date_ceiling = date_limit[0][1] 
        cursor.close()
        conn.close()
    except Exception as e:
        return e
    if date > latest_date:
        date = latest_date
    if date > date_ceiling:
        date = date_ceiling
    if date < date_floor + datetime.timedelta(length-1):
        date = date_floor + datetime.timedelta(length-1)
    # Main program.
    sql_select_f="SELECT CT.`排名`,CT.`搜索词`"
    for i in range(len(choice_list[choice]["variable"])):
        sql_select_f += ",CT.`"+choice_list[choice]["variable"][i]+"`"
    sql_select_m=""
    for i in range(length):
        sql_select_m += ",MAX(CASE ST.日期 WHEN "+str(date - datetime.timedelta(length-i-1)).replace("-","")+" THEN ST."+variable+" ELSE NULL END) AS `"+keyword+str(date - datetime.timedelta(length-i-1)).replace("-","")+"` "
    sql_select_e="FROM "+choice_list[choice]["table"]+" AS CT LEFT JOIN "+choice_list[choice]["table"]+" AS ST ON CT.搜索词 = ST.搜索词 WHERE CT.`日期` = "+str(date).replace("-","")+" AND CT.类目 = '"+category+"' AND CT.字段='"+choice+"' AND ST.字段='"+choice+"' AND ST.日期 >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.类目 = '"+category+"' GROUP BY CT.`排名`,CT.`"+variable+"` ORDER BY CT.`排名`;"
    # Return parameter
    if debug == 7:
        return {"SQL_choice":SQL,"category":category,"length":str(length),"date":str(date),"SQL":sql_select_f+sql_select_m+sql_select_e,"choice":choice,"table":choice_list[choice]["table"],"variable":variable,"fillna":fillna,"debug":debug,"path":path}    
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    conn.close()
    if fillna != "drop":
        df = df.fillna(fillna)
    else:
        df.dropna(inplace=True)
    # Debug
    if debug not in [1,2,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 1:
        print ("- Running time：%.4f s"%(time.time()-time_s))
        print( "  SQL_choice: %r \n- category: %r \n- length: %r \n- date: %r \n- SQL: %r"%(SQL,category,str(length),str(date),sql_select_f+sql_select_m+sql_select_e))
    elif debug== 2:
        print ("- Running time：%.4f s"%(time.time()-time_s))
        print("- date：%r \n- category：%r \n- length：%r \n- SQL：%r \n- choice: %r \n- table: %r \n- variable：%r \n- fillna: %r \n- debug：%r \n- path: %r"%(str(date),category,str(length),SQL,choice,choice_list[choice]["table"],variable,fillna,debug,path))
    elif debug== 8:
        return df
    elif debug== 9:
        import os
        print ("- Running time：%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="【数据组】["+choice_list[choice]["table"].split("_")[-1]+"_"+category+"_"+choice+"]"+variable+"_"+datetime.datetime.strftime(date,"%m%d")+"-"+str(length)+"_"+SQL+".csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> 输出CSV文件：",path,",",csv_filename)
        except Exception as e:
            print("> 输出CSV文件失败，错误原因：",e)
def xiaobaods_ws(df_raw,df_sort,algorithm=0,lbd=0,head=5,debug=0,path=""):
    # 2017-04-12 Algorithm EDT.
    # Algorithm Choice
    if len(df_raw) > len(df_sort):
        df_raw = df_raw.ix[:len(df_sort),:]
    elif len(df_raw) < len(df_sort):
        df_sort = df_sort.ix[:len(df_raw),:]
    if head<3:
        head = 3
    elif head>len(df_raw):
        head = len(df_raw)
    if algorithm in [1,2]:
        # EDT. Algorithm
        if lbd < 0.01 or lbd>10:
            lbd = 0.18
        df_sort1 = df_sort.ix[:,8:]
        df_sort['alg'] = lbd*2.718**(-df_sort1[df_sort1.columns[len(df_sort1.columns)-1]]*lbd)*np.std(df_sort1,axis=1)*np.sign(df_sort1.quantile(0.7,axis=1)-df_sort1[df_sort1.columns[len(df_sort1.columns)-1]]+0.001)*np.sign(algorithm*2-3)
        df_sort.sort_values(['alg'],inplace=True)
        df_raw = df_raw.iloc[df_sort.index,:]
    # Output
    if debug not in [1,2,8,9]:
        print(df_raw[:head].to_json(orient="index"))
    elif debug == 1:
        print("排序数据：",df_sort1[:head])
        print("排序参数：",df_sort.ix[:,"alg"][:head])
    elif debug == 2:
        print("- df_raw：%r \n- df_sort：%r \n- algorithm：%r \n- lbd：%r \n- head：%r \n- debug：%r \n- List：%r \n"%(df_raw.shape,df_sort.shape,algorithm,lbd,head,debug,list(df_sort.index[:head])))
    elif debug == 8:
        return df_raw[:head]
    elif debug == 9:
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="【数据组】排序算法输出数据文档.csv"
        try:
            df_raw[:head].to_csv(path+"\\"+csv_filename)
            print("> 输出CSV文件：",path,",",csv_filename)
        except Exception as e:
            print("> 输出CSV文件失败，错误原因：",e)
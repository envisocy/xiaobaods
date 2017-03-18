#!usr/bin/env python3
#! -*- coding: utf-8 -*-
#xiaobaodas version_alpha_03 2017-03-17 修补SQL和debug传参可能导致的BUG，添加fillna参数
#Ex-Ch:V_a01->02:优化import方式，修补Visitor表的BUG，添加‘查看详情’、‘同款货源’变量，添加path参数，添加debug=9的CSV导出功能，加入运行时间反馈
def xiaobaods_a02(date="",category="牛仔裤",length=7,SQL="xiaobaods",table="bc_attribute_granularity_sales",variable="热销排名",fillna="",debug=0,path=""):
    import time
    time_s = time.time()
    import pandas as pd
    import datetime
    import pymysql
    from dateutil.parser import parse
    SQL_msg = {"local":{"host":"127.0.0.1","port":3306,"user":"root","charset":"utf8","passwd":"","db":"baoersqlbasic"},
               "xiaobaods":{"host":"101.201.237.58","port":3306,"user":"program_default","charset":"utf8","passwd":"KQPp5wwZJG33fwFs","db":"baoersqlbasic"}}
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
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=SQL_msg[SQL]["port"], user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
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
    sql_select_e="FROM "+table+" AS CT LEFT JOIN "+table+" AS ST ON SUBSTRING(CT.`宝贝链接` ,- 12) = SUBSTRING(ST.`宝贝链接` ,- 12) WHERE CT.`日期` = "+str(date).replace("-","")+" AND CT.类目 = '"+category+"' AND ST.日期 >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.类目 = '"+category+"' GROUP BY CT.`"+variable+"` ORDER BY CT.`热销排名`;"
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=SQL_msg[SQL]["port"], user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    conn.close()
    df = df.fillna(fillna)
    if debug not in [1,2,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 8:
        print ("- Running time：%.4f s"%(time.time()-time_s))
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
if __name__ == "__main__":
    try:
        import sys
        argv = sys.argv[1]
        argv = eval(argv)
    except:
        argv ={}
xiaobaods_a02(date=argv.get("date",""),category=argv.get("category","牛仔裤"),length=argv.get("length",7),SQL=argv.get("SQL","xiaobaods"),table=argv.get("table","bc_attribute_granularity_sales"),variable=argv.get("variable","热销排名"),debug=argv.get("debug",0),fillna=argv.get("fillna",""),path=argv.get("path",""))   
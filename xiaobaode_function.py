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
import codecs
import MySQLdb
from sqlalchemy import create_engine
def time_seconds(Timedelta):
    return datetime.time(Timedelta.seconds//3600,Timedelta.seconds//60%60,Timedelta.seconds%60)
# SQL msg
def conftodict(filename,path=""):
    #! -*- coding: utf8 -*-
    # 2017-04-11 添加脚本路径锁定
    # 2017-04-27 修补BUG，"/"
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
for i in SQL_msg:
    SQL_msg[i]["db"]="baoersqlexternal"
# Store Group

def items_brand_list(SQL="xiaobaods",table="items0",debug=8,path=""):
    '''
    查询items0中的品牌及日期范围
    '''
    time_s = time.time()
    if table not in ["items0","items_info"]:
        table = "items0"
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query("SELECT `品牌`,min(`日期`) as `起始`,max(`日期`) as `结束` from "+table+" GROUP BY `品牌`;",conn)
    conn.close()
    if debug not in [1,2,7,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 1:
        print("- Running time：%.4f s"%(time.time()-time_s))
        print("SELECT `品牌`,min(`日期`),max(`日期`) from "+table+" GROUP BY `品牌`;")
    elif debug== 2:
        print("- Running time：%.4f s"%(time.time()-time_s))
        print("- SQL：%r \n- table：%r \n- debug：%r \n- path：%r \n"%(SQL,table,debug,path))
    elif debug== 7:
        return df.to_dict()
    elif debug== 8:
        return df
    elif debug== 9:
        import os
        print ("- Running time：%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="【数据组】"+table+"品牌范围查询.csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> 输出CSV文件：",path,",",csv_filename)
        except Exception as e:
            print("> 输出CSV文件失败，错误原因：",e)
def items_info_create(SQL="xiaobaods",brand="new",debug=8,path=""):
    '''
    item0 -> item_info 添加初始宝贝信息
    '''
    brand_list=""
    df0 = items_brand_list(table="items0",debug=8)
    df_info = items_brand_list(table="items_info",debug=8)
    df0["品牌"] = df0["品牌"].map(lambda s:s.strip(" "))
    if brand=="new": # 只添加没有的品牌信息
        brand_list = list(df0["品牌"]).copy()
        for i in df_info["品牌"]:
            if i in df0["品牌"]:
                brand_list.pop(i)
    elif brand=="all": # 添加所有的品牌信息，慎用
        brand_list = list(df0["品牌"]).copy()
    elif brand in list(df0["品牌"]):
        brand_list = [brand]
    # 最早信息（覆盖式写入）
    print(df0["品牌"])
    print(brand_list)
    for i in brand_list:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        df = pd.io.sql.read_sql_query("SELECT * from items0 WHERE `品牌`='"+i+"' and `日期`="+datetime.datetime.strftime(df0.loc[df0["品牌"]==i,"起始"].values[0],"%Y%m%d")+";",conn)
        conn.close()
        df.rename(columns={"下架时间":"下架时间0"},inplace=True)
        for i in range(len(df)):
            df.loc[i,"下架时间"] = time_seconds(df.loc[i,"下架时间0"])
        df.drop("下架时间0",axis=1,inplace=True)
        # write to MySQL
        engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
        pd.io.sql.to_sql(df,"items_info",con=engine,if_exists="append",index=False)
    return df
def items_info_update(SQL="xiaobaods",brand="all",updatevarible="",debug=8,path=""):
    '''
    item0 -> item_info 更新宝贝信息
    '''
    brand_list=""
    updatelist = ["宝贝标题","销售价最高","销售价最低","一级类目","二级类目","三级类目","下架时间","星期","女裤裤型","款式","年份季节","厚薄","女裤腰高","材质成分","面料分类","裤长","上市年份季节","腰型","材质","尺寸","适用年龄"]
    if updatevarible =="":
        pass
    elif isinstance(updatevarible,str):
        updatelist.append(updatevarible)
    elif isinstance(updatevarible,int):
        updatelist.append(str(updatevarible))
    elif isinstance(updatevarible,list):
        updatelist = updatelist + updatevarible
    df0 = items_brand_list(table="items0",debug=8)
    df_info = items_brand_list(table="items_info",debug=8)
    df0["品牌"] = df0["品牌"].map(lambda s:s.strip(" "))
    df_info["品牌"] = df_info["品牌"].map(lambda s:s.strip(" "))
    if brand in df_info["品牌"].tolist():
        brand_list = [brand]
    elif brand=="all":
        brand_list = df_info["品牌"].tolist()
    def Datelist(start,end):
        if start > end:
            start,end=end,start
        datelist = []
        while start < end:
            datelist.append(start+datetime.timedelta(1))
            start += datetime.timedelta(1)
        return datelist
    for brand_x in brand_list:
        print("","*"*60,"\n","*** 品牌：",brand_x," 处理中... ***\n","*"*60)
        date_s = df_info.loc[df_info["品牌"]==brand_x,"结束"].values[0]
        date_e = df0.loc[df0["品牌"]==brand_x,"结束"].values[0]
        print(">>> 共需更新%s天数据(%s ~ %s)"%((date_e-date_s).days,datetime.datetime.strftime(date_s,"%Y-%m-%d"),datetime.datetime.strftime(date_e,"%Y-%m-%d")))
        datelist = Datelist(date_s,date_e)
        # 绕过未录入的日期记录
        cn=1
        date_x = datelist[0]
        while date_x != (datelist[-1]+datetime.timedelta(1)):
            # 准备数据
            print(" --- ",date_x," ---")        
            # 读取新一天数据
            conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
            df_ud =""
            while len(df_ud)==0:
                date_ud = datetime.datetime.strftime((date_x+datetime.timedelta(cn-1)),"%Y%m%d")
                df_ud = pd.io.sql.read_sql_query("SELECT * from items0 WHERE `品牌`='"+brand_x+"' and `日期`="+date_ud+";",conn)
                cn += 1
            date_x += datetime.timedelta(cn-2) # date_x通过cn-2越过未录入日期
            conn.close()
            # 读取参照数据
            conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
            date_vs = datetime.datetime.strftime(date_x-datetime.timedelta(cn-1),"%Y%m%d")
            # vs数据通过cn-1越过未录入日期
            df_vs = pd.io.sql.read_sql_query("SELECT * from items0 WHERE `品牌`='"+brand_x+"' and `日期`="+date_vs+";",conn)
            cn = 1
            conn.close()  
            print(" - [Msg] - ",len(df_vs),"[",date_vs,"] -> ",len(df_ud),"[",date_ud,"]")
            date_x += datetime.timedelta(1)
            
            # 处理数据
            id_ud = df_ud["宝贝ID"].tolist()
            id_vs = df_vs["宝贝ID"].tolist()
            new = list(set(id_ud).difference(set(id_vs)))
            down = list(set(id_vs).difference(set(id_ud)))
            update = id_ud.copy()
            for i in new:
                update.remove(i)
            
            # 删除：当天内容以防报错
            print(" * Step.1/4 * 初始化...")
            conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
            cursor = conn.cursor()
            cursor.execute("delete from items_info where 日期="+datetime.datetime.strftime(date_x,"%Y%m%d")+" AND 品牌='"+brand_x+"';")
            conn.commit()
            cursor.close()
            conn.close()
            # 新款：写入"上架日期"/"上架时间"
            print(" * Step.2/4 * 写入新上架宝贝信息，共",len(new),"款...")
            df_new = df_ud.loc[df_ud["宝贝ID"].isin(new),:]
            df_new.reset_index(drop=True,inplace=True)
            df_new.rename(columns={"下架时间":"下架时间0"},inplace=True)
            for length in range(len(df_new)):
                print(" - [List] - ",df_new.loc[length,"宝贝ID"])
                df_new.loc[length,"上架日期"] = df_new.loc[length,"下架日期"]-datetime.timedelta(7)
                df_new.loc[length,"下架时间"] = time_seconds(df_new.loc[length,"下架时间0"])
                df_new.loc[length,"上架时间"] = df_new.loc[length,"下架时间"]
            df_new.drop("下架时间0",axis=1,inplace=True)
            engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
            pd.io.sql.to_sql(df_new,"items_info",con=engine,if_exists="append",index=False)
            # 下架：写入"宝贝下架"
            print(" * Step.3/4 * 写入下架宝贝信息，共",len(down),"款...")
            df_down = df_vs.loc[df_vs["宝贝ID"].isin(down),:]
            df_down.reset_index(drop=True,inplace=True)
            df_down.rename(columns={"下架时间":"下架时间0"},inplace=True)
            for length in range(len(df_down)):
                print(" - [List] - ",df_down.loc[length,"宝贝ID"])
                df_down.loc[length,"下架时间"] = time_seconds(df_down.loc[length,"下架时间0"])
                df_down.loc[length,"宝贝下架"] = date_ud
                df_down.loc[length,"日期"] = date_ud
                ### 拷贝"上架日期"/"上架时间"信息
                conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
                df_push = pd.io.sql.read_sql_query("SELECT `上架日期`,`上架时间` from items_info where `宝贝ID`="+df_down.loc[length,"宝贝ID"].replace("\t","")+" and `日期`=(SELECT max(日期) from items_info where `宝贝ID`="+df_down.loc[length,"宝贝ID"].replace("\t","")+" and `日期`<"+datetime.datetime.strftime(date_x,"%Y%m%d")+");",conn)
                conn.close()
                if df_push.loc[0,"上架日期"]:
                    df_down.loc[length,"上架日期"]=df_push.loc[0,"上架日期"]
                if df_push.loc[0,"上架时间"]:
                    df_down.loc[length,"上架时间"]=time_seconds(df_push.loc[0,"上架时间"])
            df_down.drop("下架时间0",axis=1,inplace=True)
            engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
            pd.io.sql.to_sql(df_down,"items_info",con=engine,if_exists="append",index=False)
            # 更新：判断updatelist内容是否有变化，"上架日期"/"上架时间"数据传递
            print(" * Step.4/4 * 遍历常规宝贝信息，共",len(update),"款...")
            df_update = df_ud.loc[df_ud["宝贝ID"].isin(update),:]
            df_update.reset_index(drop=True,inplace=True)
            for length in range(len(df_update)):
                for i in updatelist:
                    if df_update.loc[length,i] != df_vs.loc[df_vs["宝贝ID"]==df_update.loc[length,"宝贝ID"],i].values[0]:
                        print(" - [List] - ",df_update.loc[length,"宝贝ID"],"  @ (",i," Updated )")
                        df_update_id = pd.DataFrame(df_update.loc[length,:]).T
                        df_update_id.reset_index(drop=True,inplace=True)
                        df_update_id.rename(columns={"下架时间":"下架时间0"},inplace=True)
                        df_update_id.loc[0,"下架时间"] = time_seconds(df_update_id.loc[0,"下架时间0"])
                        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
                        df_push = pd.io.sql.read_sql_query("SELECT `上架日期`,`上架时间` from items_info where `宝贝ID`="+df_update_id.loc[0,"宝贝ID"].replace("\t","")+" and `日期`=(SELECT max(日期) from items_info where `宝贝ID`="+df_update_id.loc[0,"宝贝ID"].replace("\t","")+" and `日期`<"+datetime.datetime.strftime(date_x,"%Y%m%d")+");",conn)
                        conn.close()
                        if df_push.loc[0,"上架日期"]:
                            df_update_id.loc[0,"上架日期"]=df_push.loc[0,"上架日期"]
                        if df_push.loc[0,"上架时间"]:
                            df_update_id.loc[0,"上架时间"]=time_seconds(df_push.loc[0,"上架时间"])
                        df_update_id.drop("下架时间0",axis=1,inplace=True)
                        engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
                        pd.io.sql.to_sql(df_update_id,"items_info",con=engine,if_exists="append",index=False)
                        break
        print("","*"*60,"\n","*** 【处理完毕！】 ***\n","*"*60)
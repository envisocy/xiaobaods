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
# SQL msg
def conftodict(filename,path=""):
    #! -*- coding: utf8 -*-
    # 2017-04-11 ��ӽű�·������
    # 2017-04-27 �޲�BUG��"/"
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
    ��ѯitems0�е�Ʒ�Ƽ����ڷ�Χ
    '''
    time_s = time.time()
    if table not in ["items0","items_info"]:
        table = "items0"
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query("SELECT `Ʒ��`,min(`����`) as `��ʼ`,max(`����`) as `����` from "+table+" GROUP BY `Ʒ��`;",conn)
    conn.close()
    if debug not in [1,2,7,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 1:
        print("- Running time��%.4f s"%(time.time()-time_s))
        print("SELECT `Ʒ��`,min(`����`),max(`����`) from "+table+" GROUP BY `Ʒ��`;")
    elif debug== 2:
        print("- Running time��%.4f s"%(time.time()-time_s))
        print("- SQL��%r \n- table��%r \n- debug��%r \n- path��%r \n"%(SQL,table,debug,path))
    elif debug== 7:
        return df.to_dict()
    elif debug== 8:
        return df
    elif debug== 9:
        import os
        print ("- Running time��%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="�������顿"+table+"Ʒ�Ʒ�Χ��ѯ.csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> ���CSV�ļ���",path,",",csv_filename)
        except Exception as e:
            print("> ���CSV�ļ�ʧ�ܣ�����ԭ��",e)
def items_info_create(SQL="xiaobaods",brand="new",debug=8,path=""):
    '''
    item0 -> item_info ��ӳ�ʼ������Ϣ
    '''
    brand_list=""
    df0 = items_brand_list(table="items0",debug=8)
    df_info = items_brand_list(table="items_info",debug=8)
    df0["Ʒ��"] = df0["Ʒ��"].map(lambda s:s.strip(" "))
    if brand=="new": # ֻ���û�е�Ʒ����Ϣ
        brand_list = list(df0["Ʒ��"]).copy()
        for i in df_info["Ʒ��"]:
            if i in df0["Ʒ��"]:
                brand_list.pop(i)
    elif brand=="all": # ������е�Ʒ����Ϣ������
        brand_list = list(df0["Ʒ��"]).copy()
    elif brand in list(df0["Ʒ��"]):
        brand_list = [brand]
    # ������Ϣ������ʽд�룩
    print(df0["Ʒ��"])
    print(brand_list)
    for i in brand_list:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        df = pd.io.sql.read_sql_query("SELECT * from items0 WHERE `Ʒ��`='"+i+"' and `����`="+datetime.datetime.strftime(df0.loc[df0["Ʒ��"]==i,"��ʼ"].values[0],"%Y%m%d")+";",conn)
        conn.close()
        df.rename(columns={"�¼�ʱ��":"�¼�ʱ��0"},inplace=True)
        def time_seconds(Timedelta):
            return datetime.time(Timedelta.seconds//3600,Timedelta.seconds//60%60,Timedelta.seconds%60)
        for i in range(len(df)):
            df.loc[i,"�¼�ʱ��"] = time_seconds(df.loc[i,"�¼�ʱ��0"])
        df.drop("�¼�ʱ��0",axis=1,inplace=True)
        # write to MySQL
        engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
        pd.io.sql.to_sql(df,"items_info",con=engine,if_exists="append",index=False)
    return df
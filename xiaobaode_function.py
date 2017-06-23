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
        for i in range(len(df)):
            df.loc[i,"�¼�ʱ��"] = time_seconds(df.loc[i,"�¼�ʱ��0"])
        df.drop("�¼�ʱ��0",axis=1,inplace=True)
        # write to MySQL
        engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
        pd.io.sql.to_sql(df,"items_info",con=engine,if_exists="append",index=False)
    return df
def items_info_update(SQL="xiaobaods",brand="all",updatevarible="",debug=8,path=""):
    '''
    item0 -> item_info ���±�����Ϣ
    '''
    brand_list=""
    updatelist = ["��������","���ۼ����","���ۼ����","һ����Ŀ","������Ŀ","������Ŀ","�¼�ʱ��","����","Ů�����","��ʽ","��ݼ���","��","Ů������","���ʳɷ�","���Ϸ���","�㳤","������ݼ���","����","����","�ߴ�","��������"]
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
    df0["Ʒ��"] = df0["Ʒ��"].map(lambda s:s.strip(" "))
    df_info["Ʒ��"] = df_info["Ʒ��"].map(lambda s:s.strip(" "))
    if brand in df_info["Ʒ��"].tolist():
        brand_list = [brand]
    elif brand=="all":
        brand_list = df_info["Ʒ��"].tolist()
    def Datelist(start,end):
        if start > end:
            start,end=end,start
        datelist = []
        while start < end:
            datelist.append(start+datetime.timedelta(1))
            start += datetime.timedelta(1)
        return datelist
    for brand_x in brand_list:
        print("","*"*60,"\n","*** Ʒ�ƣ�",brand_x," ������... ***\n","*"*60)
        date_s = df_info.loc[df_info["Ʒ��"]==brand_x,"����"].values[0]
        date_e = df0.loc[df0["Ʒ��"]==brand_x,"����"].values[0]
        print(">>> �������%s������(%s ~ %s)"%((date_e-date_s).days,datetime.datetime.strftime(date_s,"%Y-%m-%d"),datetime.datetime.strftime(date_e,"%Y-%m-%d")))
        datelist = Datelist(date_s,date_e)
        # �ƹ�δ¼������ڼ�¼
        cn=1
        date_x = datelist[0]
        while date_x != (datelist[-1]+datetime.timedelta(1)):
            # ׼������
            print(" --- ",date_x," ---")        
            # ��ȡ��һ������
            conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
            df_ud =""
            while len(df_ud)==0:
                date_ud = datetime.datetime.strftime((date_x+datetime.timedelta(cn-1)),"%Y%m%d")
                df_ud = pd.io.sql.read_sql_query("SELECT * from items0 WHERE `Ʒ��`='"+brand_x+"' and `����`="+date_ud+";",conn)
                cn += 1
            date_x += datetime.timedelta(cn-2) # date_xͨ��cn-2Խ��δ¼������
            conn.close()
            # ��ȡ��������
            conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
            date_vs = datetime.datetime.strftime(date_x-datetime.timedelta(cn-1),"%Y%m%d")
            # vs����ͨ��cn-1Խ��δ¼������
            df_vs = pd.io.sql.read_sql_query("SELECT * from items0 WHERE `Ʒ��`='"+brand_x+"' and `����`="+date_vs+";",conn)
            cn = 1
            conn.close()  
            print(" - [Msg] - ",len(df_vs),"[",date_vs,"] -> ",len(df_ud),"[",date_ud,"]")
            date_x += datetime.timedelta(1)
            
            # ��������
            id_ud = df_ud["����ID"].tolist()
            id_vs = df_vs["����ID"].tolist()
            new = list(set(id_ud).difference(set(id_vs)))
            down = list(set(id_vs).difference(set(id_ud)))
            update = id_ud.copy()
            for i in new:
                update.remove(i)
            
            # ɾ�������������Է�����
            print(" * Step.1/4 * ��ʼ��...")
            conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
            cursor = conn.cursor()
            cursor.execute("delete from items_info where ����="+datetime.datetime.strftime(date_x,"%Y%m%d")+" AND Ʒ��='"+brand_x+"';")
            conn.commit()
            cursor.close()
            conn.close()
            # �¿д��"�ϼ�����"/"�ϼ�ʱ��"
            print(" * Step.2/4 * д�����ϼܱ�����Ϣ����",len(new),"��...")
            df_new = df_ud.loc[df_ud["����ID"].isin(new),:]
            df_new.reset_index(drop=True,inplace=True)
            df_new.rename(columns={"�¼�ʱ��":"�¼�ʱ��0"},inplace=True)
            for length in range(len(df_new)):
                print(" - [List] - ",df_new.loc[length,"����ID"])
                df_new.loc[length,"�ϼ�����"] = df_new.loc[length,"�¼�����"]-datetime.timedelta(7)
                df_new.loc[length,"�¼�ʱ��"] = time_seconds(df_new.loc[length,"�¼�ʱ��0"])
                df_new.loc[length,"�ϼ�ʱ��"] = df_new.loc[length,"�¼�ʱ��"]
            df_new.drop("�¼�ʱ��0",axis=1,inplace=True)
            engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
            pd.io.sql.to_sql(df_new,"items_info",con=engine,if_exists="append",index=False)
            # �¼ܣ�д��"�����¼�"
            print(" * Step.3/4 * д���¼ܱ�����Ϣ����",len(down),"��...")
            df_down = df_vs.loc[df_vs["����ID"].isin(down),:]
            df_down.reset_index(drop=True,inplace=True)
            df_down.rename(columns={"�¼�ʱ��":"�¼�ʱ��0"},inplace=True)
            for length in range(len(df_down)):
                print(" - [List] - ",df_down.loc[length,"����ID"])
                df_down.loc[length,"�¼�ʱ��"] = time_seconds(df_down.loc[length,"�¼�ʱ��0"])
                df_down.loc[length,"�����¼�"] = date_ud
                df_down.loc[length,"����"] = date_ud
                ### ����"�ϼ�����"/"�ϼ�ʱ��"��Ϣ
                conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
                df_push = pd.io.sql.read_sql_query("SELECT `�ϼ�����`,`�ϼ�ʱ��` from items_info where `����ID`="+df_down.loc[length,"����ID"].replace("\t","")+" and `����`=(SELECT max(����) from items_info where `����ID`="+df_down.loc[length,"����ID"].replace("\t","")+" and `����`<"+datetime.datetime.strftime(date_x,"%Y%m%d")+");",conn)
                conn.close()
                if df_push.loc[0,"�ϼ�����"]:
                    df_down.loc[length,"�ϼ�����"]=df_push.loc[0,"�ϼ�����"]
                if df_push.loc[0,"�ϼ�ʱ��"]:
                    df_down.loc[length,"�ϼ�ʱ��"]=time_seconds(df_push.loc[0,"�ϼ�ʱ��"])
            df_down.drop("�¼�ʱ��0",axis=1,inplace=True)
            engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
            pd.io.sql.to_sql(df_down,"items_info",con=engine,if_exists="append",index=False)
            # ���£��ж�updatelist�����Ƿ��б仯��"�ϼ�����"/"�ϼ�ʱ��"���ݴ���
            print(" * Step.4/4 * �������汦����Ϣ����",len(update),"��...")
            df_update = df_ud.loc[df_ud["����ID"].isin(update),:]
            df_update.reset_index(drop=True,inplace=True)
            for length in range(len(df_update)):
                for i in updatelist:
                    if df_update.loc[length,i] != df_vs.loc[df_vs["����ID"]==df_update.loc[length,"����ID"],i].values[0]:
                        print(" - [List] - ",df_update.loc[length,"����ID"],"  @ (",i," Updated )")
                        df_update_id = pd.DataFrame(df_update.loc[length,:]).T
                        df_update_id.reset_index(drop=True,inplace=True)
                        df_update_id.rename(columns={"�¼�ʱ��":"�¼�ʱ��0"},inplace=True)
                        df_update_id.loc[0,"�¼�ʱ��"] = time_seconds(df_update_id.loc[0,"�¼�ʱ��0"])
                        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
                        df_push = pd.io.sql.read_sql_query("SELECT `�ϼ�����`,`�ϼ�ʱ��` from items_info where `����ID`="+df_update_id.loc[0,"����ID"].replace("\t","")+" and `����`=(SELECT max(����) from items_info where `����ID`="+df_update_id.loc[0,"����ID"].replace("\t","")+" and `����`<"+datetime.datetime.strftime(date_x,"%Y%m%d")+");",conn)
                        conn.close()
                        if df_push.loc[0,"�ϼ�����"]:
                            df_update_id.loc[0,"�ϼ�����"]=df_push.loc[0,"�ϼ�����"]
                        if df_push.loc[0,"�ϼ�ʱ��"]:
                            df_update_id.loc[0,"�ϼ�ʱ��"]=time_seconds(df_push.loc[0,"�ϼ�ʱ��"])
                        df_update_id.drop("�¼�ʱ��0",axis=1,inplace=True)
                        engine = create_engine(str(r"mysql+mysqldb://%s:" + '%s' + "@%s/%s?charset=utf8") % (SQL_msg[SQL]["user"], SQL_msg[SQL]["passwd"], SQL_msg[SQL]["host"], SQL_msg[SQL]["db"]),connect_args={'charset':'utf8'})
                        pd.io.sql.to_sql(df_update_id,"items_info",con=engine,if_exists="append",index=False)
                        break
        print("","*"*60,"\n","*** ��������ϣ��� ***\n","*"*60)
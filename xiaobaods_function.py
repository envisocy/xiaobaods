#!usr/bin/env python3
#! -*- coding: gbk -*-
# SQL_msg
from dateutil.parser import parse
import pymysql
import datetime
import pandas as pd
import configparser
import time
def conftodict(filename,path=""):
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
# 2017-03-20
def xiaobaods_a01(date="",category="ţ�п�",length=7,SQL="xiaobaods",table="bc_attribute_granularity_sales",variable="��������",debug=0):
    latest_date=datetime.datetime.today().date()-datetime.timedelta(1)
    if category not in ["ţ�п�","��׿�","���п�"]:
        category="ţ�п�"
    if length>14 or length<3:
        length=7
    if table not in ["bc_attribute_granularity_sales"]:
        table="bc_attribute_granularity_sales"
    if date=="":
        date = latest_date
    else:
        date=parse(date).date()  # �޸����ڸ�ʽ
    if variable not in ["��������","֧���Ӷ�����","֧��ת����ָ��"]:
        variable="��������"
    # Try to connect with the mysql and back a date which minimum.
    try:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        cursor = conn.cursor()
        cursor.execute("SELECT min(`����`),max(`����`) from "+table+" where `��Ŀ`='"+category+"';")
        date_limit = cursor.fetchall()
        date_floor = date_limit[0][0]
        date_ceiling = date_limit[0][1]
        cursor.close()
        conn.close()
    except Exception as e:
        return
    if date > latest_date:
        date = latest_date
    if date > date_ceiling:
        date = date_ceiling
    if date < date_floor + datetime.timedelta(length-1):
        date = date_floor + datetime.timedelta(length-1)
    # Main program.
    sql_select_f = "SELECT CT.`��ͼ����ͼ`,CT.`��������`,CT.`��Ʒ��Ϣ`,CT.`��������`,CT.`֧���Ӷ�����`,CT.`������������`,CT.`֧��ת����ָ��`,CT.`��������`,CT.`��������`"
    sql_select_m=""
    for i in range(length):
        sql_select_m += ",MAX(CASE ST.���� WHEN "+str(date - datetime.timedelta(length-i-1)).replace("-","")+" THEN ST."+variable+" ELSE NULL END) AS `���ڣ�"+str(date - datetime.timedelta(length-i-1)).replace("-","")+"` "
    sql_select_e="FROM "+table+" AS CT LEFT JOIN "+table+" AS ST ON SUBSTRING(CT.`��������` ,- 12) = SUBSTRING(ST.`��������` ,- 12) WHERE CT.`����` = "+str(date).replace("-","")+" AND CT.��Ŀ = '"+category+"' AND ST.���� >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.��Ŀ = '"+category+"' GROUP BY CT.`"+variable+"` ORDER BY CT.`��������`;"
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    cursor.close()
    conn.close()
    df = df.fillna("")
    if debug == 0:
        print(df.to_json(orient="index"))
    elif debug== 8:
        return df
    elif debug== 2:
        print("date=",date," category=",category," length=",length," SQL=",SQL," variable=",variable,"debug=",debug)
    elif debug== 1:
        print( "  SQL_choice: ",SQL,"  category: ",category,"  length: ",str(length),"  date: ",str(date),"  SQL: ",sql_select_f,sql_select_m,sql_select_e)
# 2017-03-20
def xiaobaods_a02(date="",category="ţ�п�",length=7,SQL="xiaobaods",table="bc_attribute_granularity_sales",variable="��������",fillna="",debug=0,path=""):
    time_s = time.time()
    latest_date=datetime.datetime.today().date()-datetime.timedelta(1)
    if category not in ["ţ�п�","��׿�","���п�"]:
        category="ţ�п�"
    if length>14 or length<3:
        length=7
    if SQL not in SQL_msg:
        SQL=SQL_msg[0]
    if table not in ["bc_attribute_granularity_sales","bc_attribute_granularity_visitor"]:
        table="bc_attribute_granularity_sales"
    if date=="":
        date = latest_date
    else:
        date=parse(date).date()  # �޸����ڸ�ʽ
    if table=="bc_attribute_granularity_sales":
        sql_select_f = "SELECT CT.`��ͼ����ͼ`,CT.`��������`,CT.`��Ʒ��Ϣ`,CT.`��������`,CT.`֧���Ӷ�����`,CT.`������������`,CT.`֧��ת����ָ��`,CT.`��������`,CT.`��������`,CT.`�鿴����`,CT.`ͬ���Դ`"
        if variable not in ["��������","֧���Ӷ�����","������������","֧��ת����ָ��"]:
            variable="��������"
    elif table=="bc_attribute_granularity_visitor":
        sql_select_f = "SELECT CT.`��ͼ����ͼ`,CT.`��������`,CT.`��Ʒ��Ϣ`,CT.`��������`,CT.`����ָ��`,CT.`��������`,CT.`֧���Ӷ�����`,CT.`��������`,CT.`��������`,CT.`�鿴����`,CT.`ͬ���Դ`"
        if variable not in ["��������","����ָ��","��������","֧���Ӷ�����"]:
            variable="��������"
    # Try to connect with the mysql and back a date which minimum.
    try:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        cursor = conn.cursor()
        cursor.execute("SELECT min(`����`),max(`����`) from "+table+" where `��Ŀ`='"+category+"';")
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
        sql_select_m += ",MAX(CASE ST.���� WHEN "+str(date - datetime.timedelta(length-i-1)).replace("-","")+" THEN ST."+variable+" ELSE NULL END) AS `���ڣ�"+str(date - datetime.timedelta(length-i-1)).replace("-","")+"` "
    sql_select_e="FROM "+table+" AS CT LEFT JOIN "+table+" AS ST ON SUBSTRING(CT.`��������` ,- 12) = SUBSTRING(ST.`��������` ,- 12) WHERE CT.`����` = "+str(date).replace("-","")+" AND CT.��Ŀ = '"+category+"' AND ST.���� >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.��Ŀ = '"+category+"' GROUP BY CT.`"+variable+"` ORDER BY CT.`��������`;"
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    conn.close()
    df = df.fillna(fillna)
    if debug not in [1,2,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 8:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        return df
    elif debug== 2:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        print("- date��%r \n- category��%r \n- length��%r \n- SQL��%r \n- table: %r \n- variable��%r \n- debug��%r \n- path: %r"%(str(date),category,str(length),SQL,table,variable,debug,path))
    elif debug== 1:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        print( "  SQL_choice: %r \n- category: %r \n- length: %r \n- date: %r \n- SQL: %r"%(SQL,category,str(length),str(date),sql_select_f+sql_select_m+sql_select_e))
    elif debug==9:
        import os
        print ("- Running time��%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="�������顿["+table.split("_")[-1]+"_"+category+"_Top500"+"]"+variable+"_"+datetime.datetime.strftime(date,"%m%d")+"-"+str(length)+"_"+SQL+".csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> ���CSV�ļ���",path,",",csv_filename)
        except Exception as e:
            print("> ���CSV�ļ�ʧ�ܣ�����ԭ��",e)
# 2017-03-20
def xiaobaods_w03(date="",category="ţ�п�",length=7,SQL="xiaobaods",choice="���Ѻ��Ĵ�",variable="����",sort=0,fillna="",debug=0,path=""):
    time_s = time.time()
    latest_date=datetime.datetime.today().date()-datetime.timedelta(1)
    choice_list = {"�������δ�":{"table":"bc_searchwords_hotwords","variable":("��������","�����������","�����","�������","֧��ת����","ֱͨ���ο���")},
                  "����Ʒ�ƴ�":{"table":"bc_searchwords_hotwords","variable":("��������","�����������","�����","�������","֧��ת����","ֱͨ���ο���")},
                  "����������":{"table":"bc_searchwords_hotwords","variable":("��������","�̳ǵ��ռ��","�����","�������","֧��ת����","ֱͨ���ο���")},
                  "���Ѻ��Ĵ�":{"table":"bc_searchwords_hotwords","variable":("��������","�����������","�����","�������","֧��ת����","ֱͨ���ο���")},
                  "���ѳ�β��":{"table":"bc_searchwords_hotwords","variable":("��������","�̳ǵ��ռ��","�����","�������","֧��ת����","ֱͨ���ο���")},
                  "������δ�":{"table":"bc_searchwords_risewords",
                           "variable":("�����������","��������","�ʾ�������������","�������","֧��ת����","ֱͨ���ο���")},
                  "���Ʒ�ƴ�":{"table":"bc_searchwords_risewords",
                           "variable":("�����������","��������","�ʾ�������������","�������","֧��ת����","ֱͨ���ο���")},
                  "���������":{"table":"bc_searchwords_risewords",
                           "variable":("������������","��������","�����","�������","֧��ת����","ֱͨ���ο���")},
                  "������Ĵ�":{"table":"bc_searchwords_risewords",
                           "variable":("�����������","��������","�ʾ�������������","�������","֧��ת����","ֱͨ���ο���")},
                  "�����β��":{"table":"bc_searchwords_risewords",
                           "variable":("������������","��������","�����","�������","֧��ת����","ֱͨ���ο���")},}
    if category not in ["ţ�п�","��׿�","���п�"]:
        category="ţ�п�"
    if length>30 or length<3:
        length=7
    if SQL not in SQL_msg:
        SQL=SQL_msg[0]
    if choice not in choice_list:
        choice = "�������δ�"
    if variable not in choice_list[choice]["variable"]:
        variable="����"
    if date=="":
        date = latest_date
    else:
        date=parse(date).date()  # �޸����ڸ�ʽ
    # Try to connect with the mysql and back a date which minimum.
    try:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"],passwd=SQL_msg[SQL]["passwd"],
                               charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        cursor = conn.cursor()
        cursor.execute("SELECT min(`����`),max(`����`) from "+choice_list[choice]["table"]+" where `��Ŀ`='"+category+"' and `�ֶ�`='"+choice+"';")
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
    sql_select_f="SELECT CT.`����`,CT.`������`"
    for i in range(len(choice_list[choice]["variable"])):
        sql_select_f += ",CT.`"+choice_list[choice]["variable"][i]+"`"
    sql_select_m=""
    for i in range(length):
        sql_select_m += ",MAX(CASE ST.���� WHEN "+str(date - datetime.timedelta(length-i-1)).replace("-","")+" THEN ST."+variable+" ELSE NULL END) AS `���ڣ�"+str(date - datetime.timedelta(length-i-1)).replace("-","")+"` "
    sql_select_e="FROM "+choice_list[choice]["table"]+" AS CT LEFT JOIN "+choice_list[choice]["table"]+" AS ST ON CT.������ = ST.������ WHERE CT.`����` = "+str(date).replace("-","")+" AND CT.��Ŀ = '"+category+"' AND CT.�ֶ�='"+choice+"' AND ST.�ֶ�='"+choice+"' AND ST.���� >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.��Ŀ = '"+category+"' GROUP BY CT.`"+variable+"` ORDER BY CT.`����`;"
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    conn.close()
    # Order
    if sort not in [0,1,2]:
        sort=0
    if variable=="����":
        sort_p = 1
        fillna_default=501
    else:
        sort_p = -1
        fillna_default=0

    if sort==0:
        df = df.fillna(fillna)
        df["sort"]=df["����"]
    elif sort==1:
        import numpy as np
        # Sorting algorithm:��������
        df = df.fillna(fillna_default)
        df["sort"]=0
        for i in range(len(df)):
            df.ix[i,"sort"]=(np.mean(df.iloc[i,-length:])-np.mean(df.iloc[i,-3:]))/df.ix[i,"����"]*sort_p
    elif sort==2:
        import numpy as np
        # Sorting algorithm:�������
        df = df.fillna(fillna_default)
        df["sort"]=0
        for i in range(len(df)):
            df.ix[i,"sort"]=(np.mean(df.iloc[i,-length:])-np.mean(df.iloc[i,-3:]))/df.ix[i,"����"]**2*sort_p
    df.sort_values("sort",inplace=True)
    # Debug
    if debug not in [1,2,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 1:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        print( "  SQL_choice: %r \n- category: %r \n- length: %r \n- date: %r \n- SQL: %r"%(SQL,category,str(length),str(date),sql_select_f+sql_select_m+sql_select_e))
    elif debug== 2:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        print("- date��%r \n- category��%r \n- length��%r \n- SQL��%r \n- choice: %r \n- table: %r \n- variable��%r \n- sort: %r \n- fillna: %r \n- debug��%r \n- path: %r"%(str(date),category,str(length),SQL,choice,choice_list[choice]["table"],variable,sort,fillna,debug,path))
    elif debug== 8:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        return df
    elif debug== 9:
        import os
        print ("- Running time��%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="�������顿["+choice_list[choice]["table"].split("_")[-1]+"_"+category+"_"+choice+"]"+variable+"_"+datetime.datetime.strftime(date,"%m%d")+"-"+str(length)+"_"+SQL+".csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> ���CSV�ļ���",path,",",csv_filename)
        except Exception as e:
            print("> ���CSV�ļ�ʧ�ܣ�����ԭ��",e)
def xiaobaods_s01(date="",category="ţ�п�",length=7,SQL="xiaobaods",table="bc_searchwords_hotwords",variable="��������",fillna="",debug=0,path=""):
    # db:bc_searchwords_hotwords
    # 2017-04-10
    time_s = time.time()
    latest_date=datetime.datetime.today().date()-datetime.timedelta(1)
    if category not in ["ţ�п�","��׿�","���п�"]:
        category="ţ�п�"
    if length>30 or length<3:
        length=7
    table="bc_searchwords_hotwords"
    if date=="":
        date = latest_date
    else:
        date=parse(date).date()  # �޸����ڸ�ʽ
    if variable not in ["�������δ�","����Ʒ�ƴ�","����������","���Ѻ��Ĵ�","���ѳ�β��"]:
        variable="����������"
    # return time range
    try:
        conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
        cursor = conn.cursor()
        cursor.execute("SELECT min(`����`),max(`����`) from "+table+" where `��Ŀ`='"+category+"'and `�ֶ�`='"+variable+"';")
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
    sql_select_f="SELECT CT.`����`,CT.`��Ŀ`,CT.`����`,CT.`�ֶ�`,CT.`����`,CT.`������`,CT.`�����������`,CT.`�̳ǵ��ռ��`,CT.`�����`,CT.`�������`,CT.`֧��ת����`,CT.`ֱͨ���ο���`"
    sql_select_m=""
    for i in range(length):
        sql_select_m += ",MAX(CASE ST.���� WHEN "+str(date - datetime.timedelta(length-i-1)).replace("-","")+" THEN ST.`����` ELSE NULL END) AS `���ڣ�"+str(date - datetime.timedelta(length-i-1)).replace("-","")+"` "
    sql_select_e="FROM "+table+" AS CT LEFT JOIN "+table+" AS ST ON CT.`������` = ST.`������` WHERE CT.`����` = "+str(date).replace("-","")+" AND CT.��Ŀ = '"+category+"' AND CT.�ֶ�='"+variable+"' AND ST.�ֶ�='"+variable+"' AND ST.���� >= "+str(date - datetime.timedelta(length)).replace("-","")+" AND ST.��Ŀ = '"+category+"' GROUP BY CT.`������` ORDER BY CT.`����`;"
    # read msg from Mysql
    conn = pymysql.connect(host=SQL_msg[SQL]["host"], port=int(SQL_msg[SQL]["port"]), user=SQL_msg[SQL]["user"], passwd=SQL_msg[SQL]["passwd"], charset=SQL_msg[SQL]["charset"], db=SQL_msg[SQL]["db"])
    df = pd.io.sql.read_sql_query(sql_select_f+sql_select_m+sql_select_e,conn)
    conn.close()
    df = df.fillna(fillna)
    # form change
    if variable in ["�������δ�","����������","���ѳ�β��"]:
        df.drop("�����������",axis=1,inplace=True)
    elif variable in ["����Ʒ�ƴ�","���Ѻ��Ĵ�"]:
        df.drop("�̳ǵ��ռ��",axis=1,inplace=True)
    # debug return
    if debug not in [1,2,8,9]:
        print(df.to_json(orient="index"))
    elif debug== 8:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        return df
    elif debug== 2:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        print("- date��%r \n- category��%r \n- length��%r \n- SQL��%r \n- table: %r \n- variable��%r \n- debug��%r \n- path: %r"%(str(date),category,str(length),SQL,table,variable,debug,path))
    elif debug== 1:
        print ("- Running time��%.4f s"%(time.time()-time_s))
        print( "  SQL_choice: %r \n- category: %r \n- length: %r \n- date: %r \n- SQL: %r"%(SQL,category,str(length),str(date),sql_select_f+sql_select_m+sql_select_e))
    elif debug==9:
        import os
        print ("- Running time��%.4f s"%(time.time()-time_s))
        path_default=os.path.join(os.path.expanduser("~"), 'Desktop')
        if not os.path.isdir(path):
            path = path_default
        csv_filename="�������顿["+table.split("_")[-1]+"_"+category+"_Top500"+"]"+variable+"_"+datetime.datetime.strftime(date,"%m%d")+"-"+str(length)+"_"+SQL+".csv"
        try:
            df.to_csv(path+"\\"+csv_filename)
            print("> ���CSV�ļ���",path,",",csv_filename)
        except Exception as e:
            print("> ���CSV�ļ�ʧ�ܣ�����ԭ��",e)
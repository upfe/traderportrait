# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 23:27:37 2017

@author: barbuwu
"""

import MySQLdb
import cx_Oracle
from tagsettings import TAG_TABLE as TAG_TABLE
from tagsettings import TAG_DEC as TAG_DEC
import pandas as pd
def getstat(STK_CODE,TAG_CODE):
    datatable = TAG_TABLE[TAG_CODE]
    LabConnect=MySQLdb.connect(host = "172.16.8.128",\
                               user = 'upchina',\
                               passwd = 'upchina2016',\
                               db = 'db_sectag' ,\
                               charset="utf8")
    
#    curLab=LabConnect.cursor() 
    if datatable == "STK_TAG_FIX":
        strsql = "SELECT TAG_VALUE FROM STK_TAG_FIX WHERE STK_CODE = '{}' and TAG_CODE = '{}'".format(STK_CODE,TAG_CODE)

    elif datatable== "STK_TAG_FIN":
        strsql = "SELECT TAG_VALUE FROM STK_TAG_FIN WHERE STK_CODE = '{}' and TAG_CODE = '{}' and TRD_DATE = '20171227'".format(STK_CODE,TAG_CODE)
    else :
        strsql = "SELECT TAG_VALUE FROM {} WHERE STK_CODE = '{}' and TAG_CODE = '{}' and TRD_DATE = '20171228'".format(datatable,STK_CODE,TAG_CODE)


    dfMySqlData = pd.read_sql(strsql, LabConnect)
    
    if len(dfMySqlData)>0:
        return list(dfMySqlData['TAG_VALUE'])[0].encode('utf-8')
    else:
        return ""
    


def getstktag(STK_CODE):
    strlist = []
    for key in TAG_TABLE:
        TAG_CODE = key
        dfMySqlData = getstat(STK_CODE,TAG_CODE)
#        strlist = strlist + dfMySqlData +"---"
        strlist.append(dfMySqlData)
    return strlist
    

def jiaogedan(CLIENT_ID):
    db = cx_Oracle.connect('HS_HIS', 'dgv12Hc24vbd', '172.16.8.200:1521/upapp')
    sql="select CLIENT_ID,init_date,entrust_bs,stock_code,business_price,business_amount,business_balance \
        from HS_HIS.HIS_DELIVER a \
        WHERE (a.exchange_type=1 or a.exchange_type=2) and a.stock_type='0' and a.CLIENT_ID = '{}'\
        and a.init_date > 20170830".format(CLIENT_ID)
    

    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    rzrq = pd.DataFrame(data)
    rzrq.columns =  'USERCODE', 'CURRDATE','TRDID','SECUCODE','MATCHEDPRICE','MATCHEDQTY','MATCHEDAMT'
    return rzrq
    

def getstat1(TAG_CODE,lt_client_buy):
    datatable = TAG_TABLE[TAG_CODE]
    LabConnect=MySQLdb.connect(host = "172.16.8.128",\
                               user = 'upchina',\
                               passwd = 'upchina2016',\
                               db = 'db_sectag' ,\
                               charset="utf8")
    
#    curLab=LabConnect.cursor() 

    strsql = "select  TAG_VALUE from {} where TAG_CODE = {} and STK_CODE in {}".format(datatable,TAG_CODE,lt_client_buy)
    dfMySqlData = pd.read_sql(strsql, LabConnect)

    sorted_df = dfMySqlData['TAG_VALUE'].value_counts()
    strsql = "select  TAG_VALUE from {} where TAG_CODE = {} and STK_CODE in {}".format(datatable,TAG_CODE,lt_client_buy)
    dfMySqlData = pd.read_sql(strsql, LabConnect)
    if TAG_CODE == "10010400":

        new_sorted_df = pd.DataFrame(columns=["TAG_VALUE"])
        dellist = [u'融资融券',u'汇金',u'深港通',u'沪港通',u'证金',u'MSCI']
        jdx = 0
        namelist = []
        for idx in range(len(sorted_df)):
            concept = sorted_df.index[idx]
            TAG_VALUE = sorted_df[idx]
            if concept not in  dellist:
                namelist.append(concept)
                new_sorted_df.loc[jdx,"TAG_VALUE"] = TAG_VALUE
                jdx = jdx +1
        
        new_sorted_df.index = namelist  
        sorted_df = new_sorted_df

        
    if len(sorted_df) == 0:
        return ""
    else:

        if TAG_CODE in TAG_DEC.keys():
            if "转义" in TAG_DEC[TAG_CODE].keys():
                return TAG_DEC[TAG_CODE]["转义"][sorted_df.index[0].encode('utf-8')]
            if "拼接" in TAG_DEC[TAG_CODE].keys():
                return TAG_DEC[TAG_CODE]["拼接"]+sorted_df.index[0].encode('utf-8')
            if "直达" in TAG_DEC[TAG_CODE].keys():
                return sorted_df.index[0].encode('utf-8')
        else:
            return sorted_df.index[0].encode('utf-8')


#CLIENT_ID = "51002874"
def getusertag(CLIENT_ID):
    client_id_data=jiaogedan(CLIENT_ID)
    lt_client_buy = tuple({}.fromkeys(list(client_id_data[client_id_data["TRDID"] == "2"]["SECUCODE"])).keys() )
    
    dictUserTag = {}
    dictUserTag["TAG_CODE"] =  []
    dictUserTag["TAG_VALUE"] =  []
#    strlist = []
    for key in TAG_TABLE:
        TAG_CODE = key
        dfMySqlData = getstat1(TAG_CODE,lt_client_buy)
#        strlist = strlist + dfMySqlData +"---"  
#        strlist.append(dfMySqlData)
        dictUserTag["TAG_CODE"].append(TAG_CODE)
        dictUserTag["TAG_VALUE"].append(dfMySqlData)
    return  dictUserTag       

CLIENT_ID = "51002874"
CLIENT_TAG = getusertag(CLIENT_ID)








# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 15:09:18 2017

@author: barbuwu
"""

from __future__ import division


import datetime
import time
import MySQLdb
import pandas as pd
import numpy as np
import json
from urllib2 import urlopen

from tagsettings import TICAI_CLASS as TICAI_CLASS
from tagsettings import DATABASES as DATABASES


# 域名
sHost = "http://hqdata.upchina.com"	
# 获得数据接口有时候会超时, 设置重试次数
retry = 5


# 获取Url的返回值 已解析JSON
##################=============================================================prepared 函数===========================================================
def getUrlRsp(sUrl):
	i = 0
	while i <= retry :
		try:

			f = urlopen(sHost + sUrl, timeout = 5)
			rsp=f.read()
			data = json.loads(rsp)
			return data
		except Exception, e:
			i += 1
			print e
			continue

	pass


# 获取码表
# market 市场
# type 股票类型   默认沪深A股
def getStkDict(market=1, type = 6):
	sUrl = "/data/regStkDict?market=" + str(market) + "&type=" + str(type)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):

		return data['vStock']#['sCode']
	else:

		return []
  
  
  
def startconnect(databsename):
    LabConnect=MySQLdb.connect(host = DATABASES.get(databsename).get("host"),\
                               user = DATABASES.get(databsename).get("user"),\
                               passwd = DATABASES.get(databsename).get("passwd"),\
                               db = DATABASES.get(databsename).get("db") ,\
                               charset="utf8")
    
    curLab=LabConnect.cursor() 
    return LabConnect,curLab

 

def insert2db(LabConnect,curLab ,tablename,STK_CODE, TAG_VALUE, TAG_CODE):
    insert_table_sqllab="REPLACE INTO " + str(tablename) + "\
    (UPDATE_TIME,STK_CODE, TAG_VALUE, TAG_CODE) \
    values(DATE_FORMAT('%s','%%Y-%%m-%%d %%H:%%i:%%s'), '%s', '%s', '%s') "\
    %(pd.Timestamp(time.strftime('%Y-%m-%d  %H:%M:%S',time.localtime(time.time()))),STK_CODE, TAG_VALUE, TAG_CODE)
    
    curLab.execute(insert_table_sqllab)
    LabConnect.commit()

def GetConcptInfo(concp_type):
    concpt_name = []#概念名称
    concpt_code = []#概念代码
    #33:概念  32：行业  31：地域
    #调取行情接口中市场属性的码表        
#    reponse=urlopen("http://hqdata.upchina.com/data/regStkDict?market=0&type=31").read()
#    data=json.loads(reponse)['vStock']
#    for strobj in data:
#        concpt_code.append(strobj['sCode'])
#        concpt_name.append(strobj['sName'])

    reponse=urlopen("http://hqdata.upchina.com/data/regStkDict?market=0&type={}".format(concp_type)).read()
    data=json.loads(reponse)['vStock']
    for strobj in data:
        concpt_code.append(strobj['sCode'])
        concpt_name.append(strobj['sName'])


#    reponse=urlopen("http://hqdata.upchina.com/data/regStkDict?market=0&type=33").read()
#    data=json.loads(reponse)['vStock']
#    for strobj in data:
#        concpt_code.append(strobj['sCode'])
#        concpt_name.append(strobj['sName'])

    return concpt_code,concpt_name



def GetConcptBranch(ConcptCode):
    branch_name=[]
    branch_code=[]
    reponse=urlopen("http://hqdata.upchina.com/data/block2Stock?market=1&code="+ConcptCode).read()
    data=json.loads(reponse)['mStockList'][ConcptCode]['vStock']
    for strobj in data:
        branch_code.append(strobj['sCode'])
        branch_name.append(strobj['sName'])

    return branch_code,branch_name    


    

def GetNowHq(MKT,STKCODE):
    reponse=urlopen("http://hqdata.upchina.com/data/stockHq?market="+str(MKT)+"&code="+str(STKCODE)).read()
    data=json.loads(reponse)['vStockHq'][0]['stSimHq']['fChgRatio']
    return data 

def Get_Data_From_Line(num,markt,STK_CODE,offset):
    f=urlopen("http://hqdata.upchina.com/data/kline?num="+str(num)+"&market="+str(markt)+"&code="+str(STK_CODE)+"&offset="+str(offset),timeout=30)
    reponse=f.read()
    data=json.loads(reponse.decode())['vAnalyData']
    return data


#下载行情数据函数，处理原始交易数据 
def DwnHqData(num,markt,offset,STK_CODE):
    OPEN_PRICE=[]
    HIGH_PRICE=[]
    LOW_PRICE=[]
    CLOSE_PRICE=[]
    RISE_DROP_RANGE=[]
    TRADE_VOL=[]
    TRADE_AMUT=[]
    TRADE_DATE=[] 
    testnum=0#卡顿容许的次数
    data=pd.DataFrame()#原始行情交易数据
    while testnum<=5:
        try:
            data=Get_Data_From_Line(num,markt,STK_CODE,offset)
            break
        except Exception,e:#读取异常的处理 ，并重新读取   
            print e
            testnum=testnum+1


    if len(data)==0:#如果获取的数据都没有交易数据，则赋空值
        stkdata=pd.DataFrame(columns=['OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','RISE_DROP_RANGE','TRADE_VOL','TRADE_AMUT'])
    else:
        for i in range(0,len(data)):
            strobj=data[i]
            CLOSE_PRICE.append(strobj['fClose'])
            HIGH_PRICE.append(strobj['fHigh'])
            LOW_PRICE.append(strobj['fLow'])
            OPEN_PRICE.append(strobj['fOpen'])
            TRADE_VOL.append(strobj['lVolume'])
            TRADE_AMUT.append(strobj['fAmount'])
            TRADE_DATE.append(datetime.datetime.strptime(str(strobj['sttDateTime']['iDate']), "%Y%m%d"))
            #需要计算涨跌幅数据
            if i<1:#第一个日期无法计算涨跌幅
                RISE_DROPRANGE=np.nan
            else:
                RISE_DROPRANGE=100*(data[i]['fClose']/data[i-1]['fClose']-1)#涨跌幅公式
            RISE_DROP_RANGE.append(RISE_DROPRANGE)
        stkdata=pd.DataFrame({'OPEN_PRICE':OPEN_PRICE,'HIGH_PRICE':HIGH_PRICE,'LOW_PRICE':LOW_PRICE,'CLOSE_PRICE':CLOSE_PRICE,'RISE_DROP_RANGE':RISE_DROP_RANGE,'TRADE_VOL':TRADE_VOL,'TRADE_AMUT':TRADE_AMUT},index=TRADE_DATE)
        return stkdata


LabConnect,curLab = startconnect("mysql_test")


stkpool = getStkDict(0, type = 6)
TAG_CODE= "10010100"
for item in stkpool:
    STK_CODE = str(item['sCode'])
    if STK_CODE[:2] == "60":
        TAG_VALUE = "上海主板"
    elif STK_CODE[:3] == "000":
        TAG_VALUE = "深圳主板"
    elif STK_CODE[:3] == "300":
        TAG_VALUE = "创业板"
    else :
        TAG_VALUE = "中小板"  
    insert2db(LabConnect,curLab ,"STK_TAG_FIX",STK_CODE, TAG_VALUE, TAG_CODE)


for key in TICAI_CLASS.keys():
    ticaitype = key
    TAG_CODE = TICAI_CLASS[key]
    concpt_code,concpt_name = GetConcptInfo(key)
    for jdx in range(len(concpt_code)):
        ConcptCode = concpt_code[jdx]
        ConcptName = concpt_name[jdx]
        branch_code,branch_name = GetConcptBranch(ConcptCode)
        for STK_CODE in branch_code:
            STK_CODE = str(STK_CODE)
            TAG_VALUE = ConcptName
            TAG_CODE = TAG_CODE
            insert2db(LabConnect,curLab ,"STK_TAG_FIX",STK_CODE, TAG_VALUE, TAG_CODE)
            

# -*- coding: utf-8 -*-
"""
Created on Wed Dec 27 14:56:23 2017

@author: barbuwu
"""

import MySQLdb
import pandas as pd
from tagsettings import DATABASES as DATABASES
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
from urllib2 import urlopen
import json
import cx_Oracle
import time
import requests
import pickle
import numpy as np
import datetime
 

def insert2db(LabConnect,curLab ,tablename,TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE):
    insert_table_sqllab="REPLACE INTO " + str(tablename) + "\
    (UPDATE_TIME,TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE) \
    values(DATE_FORMAT('%s','%%Y-%%m-%%d %%H:%%i:%%s'),'%s', '%s', '%s', '%s') "\
    %(pd.Timestamp(time.strftime('%Y-%m-%d  %H:%M:%S',time.localtime(time.time()))),TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
    
    curLab.execute(insert_table_sqllab)
    LabConnect.commit()
    


sHost = "http://hqdata.upchina.com"	
# 获得数据接口有时候会超时, 设置重试次数
retry = 5

def MKT(STK_CODE):
    mkt = 0
    if (STK_CODE[0]=='6') or (STK_CODE[0]=='5'):
        mkt = 1
    return mkt

def getKlineData(market, code, linetype = 0, offset = 0, num = 10, flag = 0):
	sUrl = "/data/kline?market=" + str(market) + "&code=" + code + "&linetype=" + str(linetype)\
		   + "&offset=" + str(offset) + "&num=" + str(num) + "&flag=" + str(flag)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):

		return data['vAnalyData']
	else:

		return []
  
# 获取Url的返回值 已解析JSON
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


def getStockHq(code):
     market = MKT(code)
     sUrl = "/data/stockHq?market=" + str(market) + "&code=" + code
     data = getUrlRsp(sUrl)
     if (data.has_key('iRet') and data['iRet'] == 0):
		return data['vStockHq']
     else:
		return []
  

# 获取码表
# market 市场
# type 股票类型   默认沪深A股
def getStkDict(market, type = 6):
	sUrl = "/data/regStkDict?market=" + str(market) + "&type=" + str(type)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):
		return data['vStock']
	else:
		return []


#获取码表以及前后交易日期
def istrade(curtime):
    isTrade = False#今日是否为交易日
    todayTrdDate = None#
    PreTrdDate= None#
    orgin_hq_data_kline=getKlineData(market = 1,code = '000001',linetype = 0, offset = 0, num = 2, flag = 1)
    PreTrdDate = str(orgin_hq_data_kline[0]['sttDateTime']['iDate'])
    todayTrdDate = str(orgin_hq_data_kline[1]['sttDateTime']['iDate'])
    if datetime.datetime.strptime(todayTrdDate, "%Y%m%d").day == curtime.tm_mday:#判断今日是否是交易日
        isTrade=True
    return isTrade ,todayTrdDate, PreTrdDate
    


def startconnect(databsename):
    LabConnect=MySQLdb.connect(host = DATABASES.get(databsename).get("host"),\
                               user = DATABASES.get(databsename).get("user"),\
                               passwd = DATABASES.get(databsename).get("passwd"),\
                               db = DATABASES.get(databsename).get("db") ,\
                               charset="utf8")
    
    curLab=LabConnect.cursor() 
    return LabConnect,curLab


def read_data_from_sql(strsql,conn):
    # datadecr：为调这次数据的介绍，字符串类型
    dfOraData = pd.DataFrame()
    retry = 5
    request_num = 0
    while request_num <= retry:
        try:
            dfOraData = pd.read_sql(strsql, conn)
            break
        except Exception, e:
            request_num +=1
            print e
       
    if len(dfOraData) == 0:
        print 'sql本次调用数据量为0，请注意'

    if request_num == 6:
        print '特别提醒，sql本次调用超过5次，已失败！，sql语句为{}'.format(strsql)

    return dfOraData


def getheatyuncaijing(stkcode):
    newheaders = {'Accept':'application/json, text/javascript, */*; q=0.01',\
    'Accept-Encoding':'gzip, deflate',\
    'Accept-Language':'zh-CN,zh;q=0.9',\
    'Connection':'keep-alive',\
    'Content-Length':'14',\
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',\
    'Host': 'www.yuncaijing.com',
    'Origin': 'http://www.yuncaijing.com',
    
    'Upgrade-Insecure-Requests':'1',\
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'}
    login_response = []
    session = requests.Session();
    loginparams = {'stkcode':stkcode} #
    loginurl = 'http://www.yuncaijing.com/stock/get_heat/yapi/ajax.html' #登录url
    num = 0
    while num <=5:
        try:
            login_response = session.post(loginurl, data=loginparams, headers = newheaders).json()['data']['data'] 
            break# 登录
        except Exception, e:
            print e
            num = num+1
    return login_response


#获取龙虎榜数据
def getLHB(TRD_DATE):
    strsql = "select b.SEC_CODE  from upcenter.STK_EXCRA_INFO_MAIN a, upcenter.PUB_SEC_CODE b \
    where to_char(a.TRD_DATE,'yyyymmdd')={} and  a.SEC_UNI_CODE = b.SEC_UNI_CODE GROUP BY b.SEC_CODE ".format(TRD_DATE)
    db = cx_Oracle.connect('upreader', 'reader_2017', '172.16.8.20:1521/dbcenter')
    cursor = db.cursor()
    cursor.execute(strsql)
    data = cursor.fetchall()
    dfLHBData = pd.DataFrame(data)

    ltLHBData = list(dfLHBData[0])
    return ltLHBData


#获取热门股
def getHotStk():
    ltLHBData = getLHB(TRD_DATE)
    stkpool = getStkDict(0, type = 6)
    stk_pool = []
    dict_heat ={}
    for idx in range(len(stkpool)):
        print idx
        stkcode = str(stkpool[idx]['sCode'])
        stk_pool.append(stkcode)
        dict_heat[stkcode] = getheatyuncaijing(stkcode)

    df_heat = pd.DataFrame(columns= ['STK_CODE','today_hot_value','yesterday_hot_value','hot_value_delta'])
    keylist = dict_heat.keys()
    for idx in range(len(dict_heat)):
        key = keylist[idx]
        df_heat.loc[idx,'STK_CODE'] = key
        df_heat.loc[idx,'today_hot_value'] = float(dict_heat[key][-1])
        df_heat.loc[idx,'yesterday_hot_value'] = float(dict_heat[key][-2])
        df_heat.loc[idx,'hot_value_delta'] = 100*(float(dict_heat[key][-1])/float(dict_heat[key][-2])-1) if float(dict_heat[key][-2]) > 100 else 0

    today_hot = list(df_heat.sort_values(by = 'today_hot_value',ascending = False)[0:15]['STK_CODE'])
    yesterday_hot = list(df_heat.sort_values(by = 'yesterday_hot_value',ascending = False)[0:15]['STK_CODE'])

    dfLHBData =pd.DataFrame(columns = ['STK_CODE','ChgRatio'])
    for idx in range(len(ltLHBData)):
        stkcode = ltLHBData[idx]
        dfLHBData.loc[idx,'STK_CODE'] = stkcode
        dfLHBData.loc[idx,'ChgRatio'] = float(getStockHq(stkcode)[0]['stSimHq']['fChgRatio'])
    dfLHBData = dfLHBData[(dfLHBData['ChgRatio']>2)]
    ltLHBRiseData = list(dfLHBData[(dfLHBData['ChgRatio']<10.1)]['STK_CODE'])
    
    heat_hot = list(set(today_hot).union(set(yesterday_hot)))
    hot_stk = list(set(ltLHBRiseData).union(set(heat_hot)))
    return hot_stk

def Load_Data():
    #加载行情存量数据        
    pkl_file = open('hqdata.pkl', 'rb')
    hqdata=pickle.load(pkl_file)
    pkl_file.close()
    _dfOra_Index=hqdata['_dfOra_Index']
    _dfOra_Stk=hqdata['_dfOra_Stk']
    return  _dfOra_Index,_dfOra_Stk

#下载换手率数据
def dwnturnover():
    stkpool = getStkDict(0, type = 6)
    dict_turnoverratio ={}
    for idx in range(len(stkpool)):
        print idx
        stkcode = str(stkpool[idx]['sCode'])
        data = getStockHq(stkcode)
        dict_turnoverratio[stkcode] = data[0]["stExHq"]["fTurnoverRate"]
    return dict_turnoverratio
        
        
def hqpost(hq_stk_data,turnover_stk_data):
    risedrop = hq_stk_data.ix[-1,"RISE_DROP_RANGE"]

    ltvol = list(hq_stk_data['TRADE_VOL'][-2:])
    if len(hq_stk_data) >= 13:
        ltcloseprice = list(hq_stk_data['CLOSE_PRICE'][-20:])
        PL = np.mean(ltcloseprice[-13:])
        PM = np.mean(ltcloseprice[-8:])
        PS = np.mean(ltcloseprice[-5:])
        
    elif 13>len(hq_stk_data) >= 8:
        ltcloseprice = list(hq_stk_data['CLOSE_PRICE'][-8:])
        PL = np.mean(ltcloseprice[-8:])
        PM = np.mean(ltcloseprice[-5:])
        PS = np.mean(ltcloseprice[-3:])

    elif 8>len(hq_stk_data) >= 5:
        ltcloseprice = list(hq_stk_data['CLOSE_PRICE'][-5:])
        PL = np.mean(ltcloseprice[-5:])
        PM = np.mean(ltcloseprice[-3:])
        PS = np.mean(ltcloseprice[-2:])    

    else:
        PL = 1
        PM = 1
        PS = 1   
        
    dictTag = {}
    dictTag["TAG_CODE"] = []
    dictTag["TAG_VALUE"] = []
    TAG_CODE = "10020200"
    TAG_VALUE1 = ""
    if risedrop > 9.9:
        TAG_VALUE = "股价涨停"
    if 9.9>=risedrop >= 4:
        TAG_VALUE = "股价大涨"
    if 4 > risedrop >= 1.5:
        TAG_VALUE = "股价上涨"
    if 1.5>risedrop >= 0.5:
        TAG_VALUE = "股价小涨"        
    if 0.5>risedrop >= 0:
        TAG_VALUE = "股价微涨" 
    if risedrop < -9.9:
        TAG_VALUE = "股价跌停"
    if -4>=risedrop >= -9.9:
        TAG_VALUE = "股价大跌"
    if -4 < risedrop <= -1.5:
        TAG_VALUE = "股价下跌"
    if -1.5<risedrop <= -0.5:
        TAG_VALUE = "股价小跌"        
    if -0.5<risedrop < 0:
        TAG_VALUE = "股价微跌" 

    if ltvol[-1] > ltvol[-2]:
        TAG_VALUE1 = "成交量增"
    if ltvol[-1] == ltvol[-2]:
        TAG_VALUE1 = "成交量平"        
    if ltvol[-1] < ltvol[-2]:
        TAG_VALUE1 = "成交量减"        
        
    dictTag["TAG_CODE"].append(TAG_CODE)
    dictTag["TAG_VALUE"].append([TAG_VALUE,TAG_VALUE1])

    TAG_CODE = "10020600"
    if turnover_stk_data >= 25:
        TAG_VALUE = "天量成交"
    if 25 > turnover_stk_data >= 15:
        TAG_VALUE = "巨量成交"
    if 15>turnover_stk_data >= 8:
        TAG_VALUE = "放量成交"        
    if 8>turnover_stk_data >= 5:
        TAG_VALUE = "带量成交" 
    if 5>=turnover_stk_data >= 3:
        TAG_VALUE = "成交活跃"
    if 3 > turnover_stk_data >= 2:
        TAG_VALUE = "成交温和"
    if 2>turnover_stk_data >= 1:
        TAG_VALUE = "成交低迷"        
    if 1>turnover_stk_data >= 0:
        TAG_VALUE = "无量成交" 
    dictTag["TAG_CODE"].append(TAG_CODE)
    dictTag["TAG_VALUE"].append(TAG_VALUE)        


    TAG_CODE = "10020100"

    if PL > PM > PS:
        TAG_VALUE = "趋势下降"
    elif PL < PM < PS:
        TAG_VALUE = "趋势上升"      
    else:
        TAG_VALUE = "趋势震荡" 

        
    dictTag["TAG_CODE"].append(TAG_CODE)
    dictTag["TAG_VALUE"].append(TAG_VALUE)  
    
    return dictTag


def getHqTag(TRD_DATE):
    jdx = 0
    dfHqTag = pd.DataFrame(columns =['STK_CODE','TRD_DATE','TAG_VALUE','TAG_CODE'])
    ltLHBData = getLHB(TRD_DATE)
    stkpool = getStkDict(0, type = 6)  
    hot_stk = getHotStk()
    
    LabConnect,curLab = startconnect("mysql_test")
    
                    
                        
    Hq_Index,Hq_Stk =  Load_Data()
    stkpool = getStkDict(0, type = 6)
    dict_turnoverratio = dwnturnover()
    
    for idx in range(len(stkpool)):
        stkcode = str(stkpool[idx]['sCode'])
    
        if stkcode in hot_stk:
            TAG_VALUE = "热门股"
            TAG_CODE = "10020500"        
            dfHqTag.loc[jdx,'STK_CODE'] = stkcode
            dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
            dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
            dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
            jdx = jdx +1  
        if stkcode in ltLHBData:
            TAG_VALUE = "龙虎榜"
            TAG_CODE = "10020400"        
            dfHqTag.loc[jdx,'STK_CODE'] = stkcode
            dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
            dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
            dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
            jdx = jdx +1      
    
    
    
        hq_stk_data = Hq_Stk[stkcode]
        turnover_stk_data = dict_turnoverratio[stkcode]
        if len(hq_stk_data) > 1:
            if hq_stk_data.index[-1].strftime("%Y%m%d" ) == TRD_DATE:
                dictTag = hqpost(hq_stk_data,turnover_stk_data)
                
                
                for mdx in range(len(dictTag["TAG_CODE"])):
                    TAG_CODE = dictTag["TAG_CODE"][mdx]
                    item_content = dictTag["TAG_VALUE"][mdx]
                    if isinstance(item_content, list):
                        for content in item_content:
                            TAG_VALUE = content
                            dfHqTag.loc[jdx,'STK_CODE'] = stkcode
                            dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
                            dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
                            dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
                            jdx = jdx +1
                    else:
                        TAG_VALUE = item_content
                        dfHqTag.loc[jdx,'STK_CODE'] = stkcode
                        dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
                        dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
                        dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
                        jdx = jdx +1          
    return  dfHqTag
    
def insertdata(dfHqTag):
    LabConnect,curLab = startconnect("mysql_test")
    for idx in range(len(dfHqTag)):
        STK_CODE = dfHqTag.loc[idx,'STK_CODE']
        TAG_VALUE = dfHqTag.loc[idx,'TAG_VALUE']
        TAG_CODE = dfHqTag.loc[idx,'TAG_CODE']
        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)


TRD_DATE = "20180103"
dfHqTag = getHqTag(TRD_DATE)
insertdata(dfHqTag)   

#if   __name__ == '__main__':
#    lastCheckDay = 0
#    logDate = 0
#
#    isdwn = False
#    while True:        
#        curtime = time.localtime(time.time())
#        if (logDate != curtime.tm_mday):#新建当日的日志文件
#            logDate = curtime.tm_mday
#            iscau = False
#        if (16*60+10 < time.localtime(time.time()).tm_hour*60+time.localtime(time.time()).tm_min < 23*60 + 45) & (not iscau):
#            isTrade ,TRD_DATE, PreTrdDate = istrade(curtime) 
#            if isTrade : 
#                dfHqTag = getHqTag(TRD_DATE)
#                insertdata(dfHqTag)                    
#                iscau = True            
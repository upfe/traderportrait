# -*- coding: utf-8 -*-
"""
Created on Wed Jan 03 14:41:55 2018

@author: barbuwu
"""

import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

import requests
import pandas as pd
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


def getjszbyuncaijing(stkcode):
    newheaders = {'Accept':'application/json, text/javascript, */*; q=0.01',\
    'Accept-Encoding':'gzip, deflate',\
    'Accept-Language':'zh-CN,zh;q=0.9',\
    'Cache-Control':'max-age=0',\
    'Connection':'keep-alive',\
    'Host': 'www.iwencai.com',
    'Upgrade-Insecure-Requests':'1',\
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'}

    session = requests.Session();

    loginurl = "https://www.iwencai.com/diag/block-detail?pid=8073&codes={}&codeType=stock&info=%7B%22view%22%3A%7B%22nolazy%22%3A1%2C%22parseArr%22%3A%7B%22_v%22%3A%22new%22%2C%22dateRange%22%3A%5B%5D%2C%22staying%22%3A%5B%5D%2C%22queryCompare%22%3A%5B%5D%2C%22comparesOfIndex%22%3A%5B%5D%7D%2C%22asyncParams%22%3A%7B%22tid%22%3A137%7D%7D%7D".format(stkcode) #登录url
    num = 0
    stkshape = {}
    stktech = {}
    while num <=5:
        try:
            stkshape = session.get(loginurl,  headers = newheaders).json()["data"]["data"]["result"]["zxst"]
            login_response1 = session.get(loginurl,  headers = newheaders).json()["data"]["data"]["result"]["buy"]
            login_response2 = session.get(loginurl,  headers = newheaders).json()["data"]["data"]["result"]["sell"]
            stktech=dict(login_response1, **login_response2)
            #f = urlopen(loginurl, timeout = 5)
            #rsp=f.read()
            #data = json.loads(rsp)
            break# 登录
        except Exception, e:
            print e
            num = num+1
    return stkshape,stktech


stkpool = getStkDict(0, type = 6)
TRD_DATE = "20180102"
jdx = 0
dfSnlTag = pd.DataFrame(columns =['STK_CODE','TRD_DATE','TAG_VALUE','TAG_CODE'])
for idx in range(len(stkpool)):
    print idx
    stkcode = str(stkpool[idx]['sCode'])
    time.sleep(2)
    stkshape,stktech = getjszbyuncaijing(stkcode)
    
    for key in stkshape:
        if u'日线' in stkshape[key]["tag"]:
            TAG_VALUE = stkshape[key]["query"]
            TAG_CODE = "10050101"        
            dfSnlTag.loc[jdx,'STK_CODE'] = stkcode
            dfSnlTag.loc[jdx,'TRD_DATE'] = TRD_DATE
            dfSnlTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
            dfSnlTag.loc[jdx,'TAG_CODE'] = TAG_CODE
            jdx = jdx +1 
    
    for key in stktech:
        if u'日线' in stktech[key]["tag"]:
            TAG_VALUE = stktech[key]["query"]
            TAG_CODE = "10050102"        
            dfSnlTag.loc[jdx,'STK_CODE'] = stkcode
            dfSnlTag.loc[jdx,'TRD_DATE'] = TRD_DATE
            dfSnlTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
            dfSnlTag.loc[jdx,'TAG_CODE'] = TAG_CODE
            jdx = jdx +1         
        
        
        
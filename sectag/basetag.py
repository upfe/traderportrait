# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 11:28:39 2017

@author: barbuwu
"""

import pickle
import pandas as pd
import numpy as np
import MySQLdb
import cx_Oracle
import time
import datetime

from urllib2 import urlopen
import json
from tagsettings import TAG_FILED as TAG_FILED
from tagsettings import TAG_CLASS as TAG_CLASS
from tagsettings import DATABASES as DATABASES
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def startconnect(databsename):
    LabConnect=MySQLdb.connect(host = DATABASES.get(databsename).get("host"),\
                               user = DATABASES.get(databsename).get("user"),\
                               passwd = DATABASES.get(databsename).get("passwd"),\
                               db = DATABASES.get(databsename).get("db") ,\
                               charset="utf8")
    
    curLab=LabConnect.cursor() 
    return LabConnect,curLab

 

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




def getKlineData(market, code, linetype = 0, offset = 0, num = 10, flag = 0):
	sUrl = "/data/kline?market=" + str(market) + "&code=" + code + "&linetype=" + str(linetype)\
		   + "&offset=" + str(offset) + "&num=" + str(num) + "&flag=" + str(flag)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):

		return data['vAnalyData']
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
    
    
def startOraConnect(databsename):
    LabConnect = cx_Oracle.connect( DATABASES.get(databsename).get("user"), DATABASES.get(databsename).get("passwd"), DATABASES.get(databsename).get("host") +'/' + DATABASES.get(databsename).get("sid"))
    return LabConnect


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
#字符串拆分函数
def joinlist(datalist):
    result = ""
    for item in datalist:
        if item is not None:
            result = result + ',' + str(item)
    return result 
            
        

#


#获取行业数据
def getInduClass():
     strsql = "select a.stk_code,c.INDU_NAME,c.SED_INDU_UNI_CODE from\
     upcenter.STK_BASIC_INFO a,upcenter.PUB_COM_INDU_RELA b,upcenter.PUB_INDU_CODE c\
     where a.COM_UNI_CODE=b.COM_UNI_CODE and b.INDU_UNI_CODE=c.INDU_UNI_CODE and b.INDU_SYS_CODE=15 and \
     c.INDU_SYS_PAR=15  and a.STK_TYPE_PAR=1 and a.SEC_MAR_PAR in ('1','2') and \
     a.LIST_SECT_PAR in ('1','2','3') and a.LIST_STA_PAR in ('1','4') and a.LIST_DATE<trunc(sysdate)"

     dfInduData = read_data_from_sql(strsql,LabConnect)
     
     return dfInduData
     

     
#获取研报
def getRecMnd(lastdate):
    strsql = "select a3.stk_code,a2.org_chi_short_name,a1.res_writer,a1.decl_date,a1.org_uni_code\
       from      (select * from upcenter.res_info) a1 join  (select * from upcenter.pub_org_info) a2 on a1.org_uni_code=a2.org_uni_code\
       join     (select * from upcenter.stk_basic_info) a3 on a3.stk_uni_code=a1.sec_uni_code        \
       where a3.ISVALID = 1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1     AND a3.END_DATE IS NULL\
       AND a3.LIST_DATE IS NOT NULL and to_char(a1.decl_date,'yyyymmdd') > '{}' ".format(lastdate)
    dfRecMndData = read_data_from_sql(strsql,LabConnect)
    return dfRecMndData

    
#获取评级数据
def getRating(lastdate):
    strsql = "select a3.stk_code,a1.res_rate_par,a1.rate_chg_par,a2.decl_date \
         from \
         (select * from upcenter.res_sec_rate) a1 join \
         (select * from upcenter.stk_basic_info) a3 on a1.sec_uni_code=a3.stk_uni_code join \
         (select * from upcenter.res_info) a2 on a2.res_id=a1.res_id \
         where a3.ISVALID = 1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1   \
         AND a3.END_DATE IS NULL AND a3.LIST_DATE IS NOT NULL and to_char(a2.decl_date,'yyyymmdd') > '{}'".format(lastdate)
    dfRatingData = read_data_from_sql(strsql,LabConnect)
    return dfRatingData    



#获取公告的数据
def getANNC(lastdate):
    strsql = "select a4.stk_code,a1.ann_cls_name,a5.decl_date,a1.ann_cls_code \
         from \
         (select * from upcenter.ann_cls_info) a1 join \
         (select * from upcenter.ann_cls_rela) a2 on a1.ann_cls_code=a2.ann_cls_code join \
         (select * from upcenter.ann_sec_rela) a3 on a3.ann_id=a2.ann_id join \
         (select * from upcenter.stk_basic_info) a4 on a3.sec_uni_code=a4.stk_uni_code join \
         (select * from upcenter.ann_main) a5 on a5.ann_id=a2.ann_id \
         where a4.ISVALID = 1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1 \
         AND a4.END_DATE IS NULL AND a4.LIST_DATE IS NOT NULL and to_char(a5.decl_date,'yyyymmdd') > '{}'".format(lastdate)
    dfANNCData = read_data_from_sql(strsql,LabConnect)
    return dfANNCData 

#获取题材热点数据
def getConcpt():
    strsql = "select a3.stk_code,a1.concept_name,a1.drive_event,a1.createtime,a1.conc_uni_code  \
         from  \
        (select * from upcenter.com_conc_info) a1 join \
        (select * from upcenter.conc_sec_new) a2 on a1.conc_uni_code=a2.conc_uni_code join \
        (select  * from upcenter.stk_basic_info where isvalid=1) a3 on  a2.sec_uni_code=a3.stk_uni_code \
        where a3.isvalid=1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1 \
        AND a3.END_DATE IS NULL AND a3.LIST_DATE IS NOT NULL and a1.is_prop_par=1 "
    dfConcptData = read_data_from_sql(strsql,LabConnect)
    return dfConcptData 



#获取题材热点数据
def getPlace():
    strsql = "SELECT a2.plate_name,a3.STK_CODE,a2.plate_uni_code   \
        FROM  \
       (select * from upcenter.PUB_SEC_PLATE  where isvalid=1) a1 join  \
       (select * from upcenter.PUB_PLATE_INFO  where isvalid=1 and FAT_UNI_CODE=5004120000) a2 on a1.PLATE_UNI_CODE=a2.PLATE_UNI_CODE join  \
       (select  * from upcenter.stk_basic_info where isvalid=1) a3 on a1.sec_uni_code=a3.stk_uni_code \
        where a3.isvalid=1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1 \
        AND a3.END_DATE IS NULL AND a3.LIST_DATE IS NOT NULL"
    dfPlaceData = read_data_from_sql(strsql,LabConnect)
    return dfPlaceData 

def transvalue2pingji(data):
    if not np.isnan(data):
        if data < 2:
            return "买入评级"
        if  3 < data <= 2:
            return "增持评级"
        if 4 < data <=3:
            return "观望评级"
        if 5 < data <=4:
            return "减持评级"    
        if 6 < data <=5:
            return "卖出等级"    
    else:
        return "无评级"

def getBaseData(TRD_DATE):
    dfInduData = getInduClass() 
    RecMndDate = (datetime.datetime.strptime(TRD_DATE, '%Y%m%d') - datetime.timedelta(days=2)).strftime('%Y%m%d')
    RatingDate = (datetime.datetime.strptime(TRD_DATE, '%Y%m%d') - datetime.timedelta(days=2)).strftime('%Y%m%d')
    ANNDate =  (datetime.datetime.strptime(TRD_DATE, '%Y%m%d') - datetime.timedelta(days=2)).strftime('%Y%m%d')
    dfRecMndData =  getRecMnd(RecMndDate) 
    dfRatingData = getRating(RatingDate)
    dfANNCData = getANNC(ANNDate) 
    dfConcptData = getConcpt()
    dfPlaceData =   getPlace() 
    
    
    dictIndu = {}
    dictWriter = {}
    dictOrg = {}
    dictRate = {}
    dictChg = {}
    dictANNC = {}
    dictConcept = {}
    dictPlace = {}

    for item in stkpool:
        STK_CODE = str(item['sCode'])
        print STK_CODE
        stk_data = dfInduData[dfInduData['STK_CODE']==STK_CODE]
        indu_temp = list(stk_data['INDU_NAME'])
        ltindu = joinlist(indu_temp)[1:].split(',')
        dictIndu[STK_CODE] = np.nan if ltindu == [""] else ltindu
    
        stk_data = dfRecMndData[dfRecMndData['STK_CODE']==STK_CODE].drop_duplicates(['ORG_UNI_CODE']) 
        writer_temp = list(stk_data['RES_WRITER'])
        ltwriter = joinlist(writer_temp)[1:].split(',')
        dictWriter[STK_CODE] = np.nan if ltwriter == [""] else ltwriter
        temp = joinlist(list(stk_data['ORG_CHI_SHORT_NAME']))[1:].split(',')
        dictOrg[STK_CODE] = np.nan if temp == [""] else temp

        stk_data = dfRatingData[dfRatingData['STK_CODE']==STK_CODE]
        rate_temp = list(stk_data['RES_RATE_PAR'])
        dictRate[STK_CODE] = transvalue2pingji(np.mean(rate_temp))
        rate_temp = list(stk_data['RATE_CHG_PAR'])
        dictChg[STK_CODE] = np.mean(rate_temp)

        stk_data = dfANNCData[dfANNCData['STK_CODE']==STK_CODE].drop_duplicates(['ANN_CLS_CODE']) 
        annc_temp = list(stk_data['ANN_CLS_NAME'])
        ltannc = joinlist(annc_temp)[1:].split(',')
        dictANNC[STK_CODE] = np.nan if ltannc == [""] else ltannc

        stk_data = dfConcptData[dfConcptData['STK_CODE']==STK_CODE]
        concept_temp = list(stk_data['CONCEPT_NAME'])
        ltconcep = joinlist(concept_temp)[1:].split(',')
        dictConcept[STK_CODE] = np.nan if ltconcep == [""] else ltconcep

        stk_data = dfPlaceData[dfPlaceData['STK_CODE']==STK_CODE]
        place_temp = list(stk_data['PLATE_NAME'])
        ltplace = joinlist(place_temp)[1:].split(',')
        dictPlace[STK_CODE] = np.nan if ltplace == [""] else ltplace        

    return dictIndu ,dictWriter,dictOrg,dictRate,dictChg ,dictANNC,dictConcept,dictPlace



def getTagData():
    dfBaseTag = pd.DataFrame(columns =['STK_CODE','TRD_DATE','TAG_VALUE','TAG_CODE'])
    idx = 0
    for item in stkpool:
        STK_CODE = str(item['sCode'])
        print STK_CODE + "正在合并大表"
        item_content = dictIndu[STK_CODE]
        TAG_CODE = "10010200"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1
    
        item_content = dictOrg[STK_CODE]
        TAG_CODE = "10030104"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1    
    
        item_content = dictWriter[STK_CODE]
        TAG_CODE = "10030101"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1
                
        item_content = dictRate[STK_CODE]
        TAG_CODE = "10030102"
        print item_content
        print type(item_content)
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif isinstance(item_content, str):
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1        
        elif isinstance(item_content, float) or isinstance(item_content, int):
            if np.isnan(item_content):
                pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1
    
        item_content = dictChg[STK_CODE]
        TAG_CODE = "10030103"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1           
    
        item_content = dictANNC[STK_CODE]
        TAG_CODE = "10030200"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1  
    
        item_content = dictConcept[STK_CODE]
        TAG_CODE = "10010400"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1  
    
    
        item_content = dictPlace[STK_CODE]
        TAG_CODE = "10010300"
        if isinstance(item_content, list):
            for content in item_content:
                TAG_VALUE = content
                dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
                dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
                dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
                dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
                idx = idx +1
        elif np.isnan(item_content):
            pass
        else:
            TAG_VALUE = item_content
            dfBaseTag.loc[idx,'STK_CODE'] = STK_CODE
            dfBaseTag.loc[idx,'TRD_DATE'] = TRD_DATE
            dfBaseTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
            dfBaseTag.loc[idx,'TAG_CODE'] = TAG_CODE
            idx = idx +1  
    return dfBaseTag

def insertdata(dfBaseTag):
    LabConnect,curLab = startconnect("mysql_test")
    for idx in range(len(dfBaseTag)):
        STK_CODE = dfBaseTag.loc[idx,'STK_CODE']
        TAG_VALUE = dfBaseTag.loc[idx,'TAG_VALUE']
        TAG_CODE = dfBaseTag.loc[idx,'TAG_CODE']
        insert2db(LabConnect,curLab ,"STK_TAG_BASE",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)


if   __name__ == '__main__':
    lastCheckDay = 0
    logDate = 0

    isdwn = False
    while True:        
        curtime = time.localtime(time.time())
        if (logDate != curtime.tm_mday):#新建当日的日志文件
            logDate = curtime.tm_mday
            iscau = False
        if (22*60+10 < time.localtime(time.time()).tm_hour*60+time.localtime(time.time()).tm_min < 23*60 + 45) & (not iscau):
            isTrade ,TRD_DATE, PreTrdDate = istrade(curtime) 
            if isTrade : 
                stkpool = getStkDict(0, type = 6)
                LabConnect = startOraConnect("dbcenter_local") 
                dictIndu ,dictWriter,dictOrg,dictRate,dictChg ,dictANNC,dictConcept,dictPlace = getBaseData(TRD_DATE)     
                dfBaseTag = getTagData() 
                insertdata(dfBaseTag)                    
                iscau = True            
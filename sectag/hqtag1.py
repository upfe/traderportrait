#-*- coding: utf-8 -*-
from __future__ import division
import cx_Oracle
import numpy as np
import pandas as pd
import urllib2
import json
from urllib2 import urlopen
import logging
import os

import MySQLdb
import datetime,time
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
from tagsettings import TAG_FILED as TAG_FILED
from tagsettings import TAG_CLASS as TAG_CLASS
from tagsettings import DATABASES as DATABASES

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%Y-%b-%d %H:%M:%S')

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
			logging.info("request = " + sUrl)
			f = urlopen(sHost + sUrl, timeout = 5)
			rsp=f.read()
			data = json.loads(rsp)
			return data
		except Exception, e:
			i += 1
			logging.warning("Exception:" + str(e)  + ", url = " + sUrl + ", retry = " + str(i + 1))
			continue
	logging.error("Retry " + str(i) + " times failed. url = " + sUrl)
	pass


# 获取码表
# market 市场
# type 股票类型   默认沪深A股
def getStkDict(market=1, type = 6):
	sUrl = "/data/regStkDict?market=" + str(market) + "&type=" + str(type)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):
		logging.info("Get data succuss. url = " + sUrl)
		return data['vStock']#['sCode']
	else:
		logging.error("Get stock dictionary data failed. url = " + sUrl)
		return []
#################################1.0 市场类型==============================================================================================================

def getMKT(code):
    market = 0
    if int(code[0])==6:
        market=1
    else:
        market=0
    return market
def zhongxiaoban(code):
    zhongxiao=0
    if int(code[0])==0 and int(code[1])==0 and int(code[2])==2:
        zhongxiao=1
    else:
        zhongxiao=0
    return zhongxiao
def chuangyeban(code):
    cyb=0
    if int(code[0])==3:
        cyb=1
    else:
        cyb=0
    return cyb

def st(code):
    stt=0
    if code[0]=='*' or code[0]=='S':
        stt=1
    return stt

def hushenzhuban(ccdd):
    hszb=[]
    szzb=[]
    for ii in ccdd:
     if getMKT(ii)==1:
        hszb.append(ii)
     else:
        szzb.append(ii)
    return  hszb,szzb

def zxb(ccdd):
   zxb=[]
   for ii in ccdd:
    if zhongxiaoban(ii)==1:
        zxb.append(ii)
   return zxb
def cyb(ccdd):
    cyb=[]
    for ii in ccdd:
      if chuangyeban(ii)==1:
        cyb.append(ii)
    return cyb
def stt(ccdd,ccnn):
    stcode=[]
    for ii in range(len(ccdd)):
     if st(ccnn[ii][0])==1:
        stcode.append(ccdd[ii])        
    return stcode
#=============================================================================1.0  财务指标及计算===========================================================
# 获取财务数据(实时)
# market 市场
# code   股票代码
def getCWPriceAmount(market, code):
	sUrl = "/data/cwData?market=" + str(market) + "&code=" + code
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):
		logging.info("Get data succuss. url = " + sUrl)
		return data['vInfo']
	else:
		logging.error("Get tick data failed. url = " + sUrl)
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
    
    
    
#获取龙虎榜数据
def getLHB(TRD_DATE):
    strsql = "select b.SEC_CODE  from upcenter.STK_EXCRA_INFO_MAIN a, upcenter.PUB_SEC_CODE b \
    where to_char(a.TRD_DATE,'yyyymmdd')={} and  a.SEC_UNI_CODE = b.SEC_UNI_CODE group by b.SEC_CODE".format(TRD_DATE)
    db = cx_Oracle.connect('upreader', 'reader_2017', '172.16.8.20:1521/dbcenter')
    cursor = db.cursor()
    cursor.execute(strsql)
    data = cursor.fetchall()
    dfLHBData = pd.DataFrame(data)

    ltLHBData = list(dfLHBData[0])
    return ltLHBData


# 绩优股
                 
def getjiyougu(codd,real):
 jyg=[]
 for ii in codd:
    market=getMKT(ii)
    ak=getCWPriceAmount(market,ii)
    if len(ak)>1:
     mgsy=ak[0]['dMGSY']
     xsmll=ak[0]['dXSMLL']
     cl=float(real[real['stkcode']==ii]['Close'].values)
     if mgsy>0:
       if (cl/mgsy>0)  and (cl/mgsy<30):
         if xsmll>30 and mgsy>0.5:
                jyg.append(ii)    
 return jyg

def baimagu(mgsy,jzcsy,mgjzc,yysrtb,jlrtb,close):
    bm=0
    if mgsy>0:
      if mgsy>0.5 and jzcsy>10 and close/mgsy<40 and close/mgsy>0 and mgjzc>3 and yysrtb>20 and jlrtb>30:
        bm=1
    return bm
def bmg(codd,real):
  bmg=[]
  for ii in codd:
    market=getMKT(ii)
    ak=getCWPriceAmount(market,ii)
    if len(ak)>0:
     mgsy=ak[0]['dMGSY']
     jzcsy=ak[0]['dJZCSYL']
     mgjzc=ak[0]['dMGJZC']
     yysrtb=ak[0]['dYYSRTB']
     jlrtb=ak[0]['dJLRTB']
     close=float(real[real['stkcode']==ii]['Close'].values)
     if baimagu(mgsy,jzcsy,mgjzc,yysrtb,jlrtb,close)==1:
        bmg.append(ii)   
  return bmg
#####===========================================================2.0 行情==================================================================================
# 获取K线行情数据
# market 市场
# code 股票代码
# linetype  K线类型     默认日K线
# offset  偏移量
# num 获取数量          默认取最后10条数据
# flag 除权标志         默认不要除权数据
def getKlineData(market,code, linetype = 0, offset = 0, num = 20, flag = 0):
	sUrl = "/data/kline?market=" + str(market) + "&code=" + code + "&linetype=" + str(linetype)\
		   + "&offset=" + str(offset) + "&num=" + str(num) + "&flag=" + str(flag)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):
		logging.info("Get data succuss. url = " + sUrl)
		return data['vAnalyData']
	else:
		logging.error("Get Kline data failed. url = " + sUrl)
		return []    
    
# 字符拼接 最大容量1100
def zhengbiao(ccdd):
 sh=''
 ss=''
 for ii in range(1100):#range(len(ccdd)):
    if int(ccdd[ii][0])==6:       
        sh=sh+ccdd[ii]+','
    else:
        ss=ss+ccdd[ii]+','
 ss=ss[:-1]        
 sh=sh[:-1]   

 sh1=''
 ss1=''
 for ii in range(1100,2200):#range(len(ccdd)):
    if int(ccdd[ii][0])==6:       
        sh1=sh1+ccdd[ii]+','
    else:
        ss1=ss1+ccdd[ii]+','
 ss1=ss1[:-1]        
 sh1=sh1[:-1]  

 sh2=''
 ss2=''
 for ii in range(2200,len(ccdd)):#range(len(ccdd)):
    if int(ccdd[ii][0])==6:       
        sh2=sh2+ccdd[ii]+','
    else:
        ss2=ss2+ccdd[ii]+','
 ss2=ss2[:-1]        
 sh2=sh2[:-1]                                                                                     

 def shenstock(stkcode):
    market = 0
    url = "http://hqdata.upchina.com/data/stockHq?market={0}&code={1}".format(market,stkcode)
    try:
        marketData = urllib2.urlopen(url).read()
    except:
        marketData = ""
    return marketData.decode('utf8')
    
 def shangstock(stkcode):
    market = 1
    url = "http://hqdata.upchina.com/data/stockHq?market={0}&code={1}".format(market,stkcode)
    try:
        marketData = urllib2.urlopen(url).read()
    except:
        marketData = ""
    return marketData.decode('utf8')    
    
 def getNewestDatass(stkcode):
    marketData = shenstock(stkcode)
    if len(marketData)>11:
        datas = json.loads(marketData)['vStockHq']
        datas_list = []
        for dat in datas:    
         datas_list.append([dat['sName'], dat['sCode'], float(dat['stSimHq']['fNowPrice']),float(dat['stSimHq']['fOpen']), float(dat['stSimHq']['fHigh']), float(dat['stSimHq']['fLow']),
                               float(dat['stSimHq']['fClose']),float(dat['stSimHq']['lVolume']),float(dat['stSimHq']['fAmount']),float(dat['stSimHq']['fChgRatio']),float(dat['stSimHq']['fZhenfu']),float(dat['stExHq']['lNowVol']),float(dat['stExHq']['lInside']),float(dat['stExHq']['lOutside']),float(dat['stExHq']['vBuyp'][0]),float(dat['stExHq']['vBuyp'][1]),float(dat['stExHq']['vBuyp'][2]),float(dat['stExHq']['vBuyp'][3]),float(dat['stExHq']['vBuyp'][4]),float(dat['stExHq']['vBuyv'][0]),float(dat['stExHq']['vBuyv'][1]),float(dat['stExHq']['vBuyv'][2]),float(dat['stExHq']['vBuyv'][3]),float(dat['stExHq']['vBuyv'][4]),float(dat['stExHq']['vSellp'][0]),float(dat['stExHq']['vSellp'][1]),float(dat['stExHq']['vSellp'][2]),float(dat['stExHq']['vSellp'][3]),float(dat['stExHq']['vSellp'][4]),float(dat['stExHq']['vSellv'][0]),float(dat['stExHq']['vSellv'][1]),float(dat['stExHq']['vSellv'][2]),float(dat['stExHq']['vSellv'][3]),float(dat['stExHq']['vSellv'][4]),float(dat['stExHq']['fAveragePrice']),float(dat['stExHq']['fZTPrice']),float(dat['stExHq']['fDTPrice']),float(dat['stExHq']['fTurnoverRate']),float(dat['stExHq']['iTradeDate']),float(dat['stExHq']['iTradeTime']),float(dat['stDeriveHq']['dLiangBi']),float(dat['stDeriveHq']['dUpSpeed']),float(dat['stDeriveHq']['lTradeNum'])])
         columns_list = ['Name','stkcode','nowprice','Open', 'High', 'Low', 'Close','Volume','Amount','ChgRatio','Zhenfu','NowVol','Inside','Outside','vBuyp1','vBuyp2','vBuyp3','vBuyp4','vBuyp5','vBuyv1','vBuyv2','vBuyv3','vBuyv4','vBuyv5','vSellp1','vSellp2','vSellp3','vSellp4','vSellp5','vSellv1','vSellv2','vSellv3','vSellv4','vSellv5','AveragePrice','ZTPrice','DTPrice','TurnoverRate','TradeDate','TradeTime','LiangBi','UpSpeed','TradeNum']
        df = pd.DataFrame(datas_list, columns=columns_list)
    else:
        df=[]
    return df    
    
 def getNewestDatash(stkcode):
    marketData = shangstock(stkcode)
    if len(marketData)>11:
        datas = json.loads(marketData)['vStockHq']
        datas_list = []
        for dat in datas:    
         datas_list.append([dat['sName'], dat['sCode'], float(dat['stSimHq']['fNowPrice']),float(dat['stSimHq']['fOpen']), float(dat['stSimHq']['fHigh']), float(dat['stSimHq']['fLow']),
                               float(dat['stSimHq']['fClose']),float(dat['stSimHq']['lVolume']),float(dat['stSimHq']['fAmount']),float(dat['stSimHq']['fChgRatio']),float(dat['stSimHq']['fZhenfu']),float(dat['stExHq']['lNowVol']),float(dat['stExHq']['lInside']),float(dat['stExHq']['lOutside']),float(dat['stExHq']['vBuyp'][0]),float(dat['stExHq']['vBuyp'][1]),float(dat['stExHq']['vBuyp'][2]),float(dat['stExHq']['vBuyp'][3]),float(dat['stExHq']['vBuyp'][4]),float(dat['stExHq']['vBuyv'][0]),float(dat['stExHq']['vBuyv'][1]),float(dat['stExHq']['vBuyv'][2]),float(dat['stExHq']['vBuyv'][3]),float(dat['stExHq']['vBuyv'][4]),float(dat['stExHq']['vSellp'][0]),float(dat['stExHq']['vSellp'][1]),float(dat['stExHq']['vSellp'][2]),float(dat['stExHq']['vSellp'][3]),float(dat['stExHq']['vSellp'][4]),float(dat['stExHq']['vSellv'][0]),float(dat['stExHq']['vSellv'][1]),float(dat['stExHq']['vSellv'][2]),float(dat['stExHq']['vSellv'][3]),float(dat['stExHq']['vSellv'][4]),float(dat['stExHq']['fAveragePrice']),float(dat['stExHq']['fZTPrice']),float(dat['stExHq']['fDTPrice']),float(dat['stExHq']['fTurnoverRate']),float(dat['stExHq']['iTradeDate']),float(dat['stExHq']['iTradeTime']),float(dat['stDeriveHq']['dLiangBi']),float(dat['stDeriveHq']['dUpSpeed']),float(dat['stDeriveHq']['lTradeNum'])])
         columns_list = ['Name','stkcode','nowprice','Open', 'High', 'Low', 'Close','Volume','Amount','ChgRatio','Zhenfu','NowVol','Inside','Outside','vBuyp1','vBuyp2','vBuyp3','vBuyp4','vBuyp5','vBuyv1','vBuyv2','vBuyv3','vBuyv4','vBuyv5','vSellp1','vSellp2','vSellp3','vSellp4','vSellp5','vSellv1','vSellv2','vSellv3','vSellv4','vSellv5','AveragePrice','ZTPrice','DTPrice','TurnoverRate','TradeDate','TradeTime','LiangBi','UpSpeed','TradeNum']
        df = pd.DataFrame(datas_list, columns=columns_list)
    else:
        df=[]
    return df
#    marketdata=shenstock(stkcode)  
 stkcode = ss      
 marketdata=getNewestDatass(stkcode) 
 stkcode1 = ss1      
 marketdata1=getNewestDatass(stkcode1)  
 stkcode2 = ss2     
 marketdata2=getNewestDatass(stkcode2) 
 if len(marketdata)>1 and len(marketdata1)>1:
    sss=marketdata.append(marketdata1,ignore_index=1)
    if len(marketdata2)>1:
            sss=sss.append(marketdata2,ignore_index=1)
    
    
 stkcode=sh
 marketdata3=getNewestDatash(stkcode) 
 stkcode=sh1
 marketdata4=getNewestDatash(stkcode) 
 stkcode=sh2
 marketdata5=getNewestDatash(stkcode) 
 if len(marketdata3)<=1:
   if len(marketdata4)>1:
       if len(marketdata5)>1:
           shh=marketdata4.append(marketdata5,ignore_index=1)
 hushen=shh.append(sss,ignore_index=1)    
 return hushen
    
    
# 获取历史日资金流数据
# market 市场
# code   股票代码
# num    数量
# offset 偏移量
def getHisDayMF(market, code, num=1, offset =0):
	sUrl = "/data/getHisDayMoneyFlow?market=" + str(market) + "&code=" + code + "&num=" + str(num) + "&offset=" + str(offset)
	data = getUrlRsp(sUrl)
	if (data.has_key('iRet') and data['iRet'] == 0):
		logging.info("Get data succuss. url = " + sUrl)
		return data['vStockMFlow']
	else:
		logging.error("Get his money flow data failed. url = " + sUrl)
		return []    


def hangqinghuafen(ccdd):
 sz=[]
 xd=[]
 zd=[]

 lz=[]
 lj=[]
 for ii in ccdd:
    market = getMKT(ii)
    kline=getKlineData(market,ii,0,0,2,0)
    zj=[]
    zjj=[]
    for j in range(len(kline)):
        zj.append(kline[j]['fClose'])
        zjj.append(kline[j]['lVolume'])
    if len(zj)>0:
        maa=None
        if len(zj)>=20:
           maa=[np.mean(zj[-5:]),np.mean(zj[-10:]),np.mean(zj[-20:])]
        elif len(zj)>=10:
           maa=[np.mean(zj[-5:]),np.mean(zj[-10:]),0]
        elif len(zj)>=5:
           maa=[np.mean(zj[-5:]),0,0]
        else:
           maa=[0,0,0]
        if maa[0]>maa[1] and maa[1]>maa[2]:
            sz.append(ii)
        elif maa[0]<maa[1] and maa[1]<maa[2]:
            xd.append(ii)
        else:
            zd.append(ii)
    if len(zjj)>=2:
        if zjj[-1]>zjj[-2]:#np.mean(zjj.iloc[1:6])*2:
            lz.append(ii)
        elif zjj[-1]<zjj[-2]:
            lj.append(ii)
 return sz,xd,zd,lz,lj

def jgzd(ccdd):
 gpsz=[]
 gpxd=[]
 gpzt=[]
 gpdt=[]
 gpcp=[]
 for ii in ccdd:
    market = getMKT(ii)
    kline=getKlineData(market,ii,0,0,2,0)
    zj=[]
    for j in range(len(kline)):
        zj.append(kline[j]['fClose'])
    if len(zj)>=2:
        if zj[1]>=round(zj[0]*1.1,2):
            gpzt.append(ii)
        elif zj[1]>zj[0]:
            gpsz.append(ii)
        elif zj[1]==zj[0]:
            gpcp.append(ii)
        elif zj[1]<=round(zj[0],2):
            gpdt.append(ii)
        else:
            gpxd.append(ii)
    else:
        1==1
 return gpzt,gpsz,gpxd,gpdt,gpcp
    
    
    
    
def turnover(ccdd):
 real=zhengbiao(ccdd)
 tover=pd.DataFrame(data=list(real.stkcode),columns=['stkcode'])
 tover['turnover']=real.TurnoverRate
 liangzong=tover
 wl=[]
 cjdm=[]
 cjwh=[]
 cjhy=[]
 dl=[]
 fl=[]
 jl=[]
 tl=[]
 for ii in range(len(liangzong)):
    if liangzong.iloc[ii]['turnover']<1:
        wl.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<2:
        cjdm.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<3:
        cjwh.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<5:
        cjhy.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<8:
        dl.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<15:
        fl.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<25:
        jl.append(liangzong['stkcode'].iloc[ii])
    elif liangzong.iloc[ii]['turnover']<=100:
        tl.append(liangzong['stkcode'].iloc[ii])
 return wl,cjdm,cjwh,cjhy,dl,fl,jl,tl


def zijinliu(ccdd):
 lr=[]
 lc=[]
 for ii in ccdd:
   market=getMKT(ii)    
   ad=getHisDayMF(market,ii)
   ad=ad[0]
   jingliuru=ad['fBigIn']+ad['fMidIn']+ad['fSmallIn']+ad['fSuperIn']-ad['fSuperOut']-ad['fBigOut']-ad['fMidOut']-ad['fSmallOut']
   if jingliuru>0:
       lr.append(ii)
   else:
       lc.append(ii)
 return lr,lc







##  数据库查询集合====================================================================================================================================
# 融资融券数据查询
def rzrq():   
    #db = cx_Oracle.connect('readonly/anegIjege@121.43.68.222:15210/upwhdb')
    db = cx_Oracle.connect('upreader', 'reader_2017', '172.16.8.20:1521/dbcenter')
    sql='select a2.stk_code,a1.in_date \
     from \
    (select * from upcenter.margin_und_sec) a1 join  \
    (select * from upcenter.stk_basic_info) a2 on a1.sec_uni_code=a2.stk_uni_code \
    where \
    a2.isvalid=1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1  AND a2.END_DATE IS NULL AND a2.LIST_DATE IS NOT NULL '
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    rzrq = pd.DataFrame(data)
    rzrq.columns =  'code', 'in_date'
    return rzrq


# 连续三年增长：
def zsn():     
    db = cx_Oracle.connect('upreader', 'reader_2017', '172.16.8.20:1521/dbcenter')
    sql='select a2.stk_code,a1.earning_chg,a1.end_date \
         from \
         (select * from upcenter.fin_idx_ana )a1 join \
         (select * from upcenter.stk_basic_info)a2 on a1.com_uni_code=a2.com_uni_code \
         where a2.ISVALID = 1 AND (SEC_MAR_PAR = 1 OR SEC_MAR_PAR = 2) AND STK_TYPE_PAR = 1  \
         AND a2.END_DATE IS NULL AND a2.LIST_DATE IS NOT NULL order by a1.end_date desc '
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    rzrq = pd.DataFrame(data)
    rzrq.columns =  'code', 'length','date'
    return rzrq
##===================================================================查询，1.0 财务部分，成长股===============================================================
def chengzhanggu(mgsy,xsmll,jlrtb,close):
    czgg=0
    if mgsy>0:
       if close/mgsy>20 and xsmll>30 and jlrtb>20:
        czgg=1
    return czgg

def czg(zong4,real):
 czg=[]
 for ii in zong4:
    market=getMKT(ii)
    ak=getCWPriceAmount(market,ii)
    if len(ak)>0:
     mgsy=ak[0]['dMGSY']
     xsmll=ak[0]['dXSMLL']
     jlrtb=ak[0]['dJLRTB']
     close=float(real[real['stkcode']==ii]['Close'].values)
     if chengzhanggu(mgsy,xsmll,jlrtb,close)==1:
        czg.append(ii)    
 return czg


def run(TRD_DATE):
#######==============================================================================================================================================##
## prepared data
######===============================================================================================================================================##

 
 aa=getStkDict(1)
 ccdd=[]
 for i in range(len(aa)):
    ccdd.append(aa[i]['sCode'])
    
 ccnn=[]
 for ii in range(len(aa)):
    ccnn.append(aa[ii]['sName'])
# 实时报价表
 real=zhengbiao(ccdd)
##================================================================================================================================================##
#2.2.1
#1
#沪市主板 hszb
# 深市主板 sszb
 ak=hushenzhuban(ccdd)
 hszb=ak[0]
 szzb=ak[1]
# 中小板
 zxb1=zxb(ccdd)
#  创业板
 cyb1=cyb(ccdd)
# 融资融券
# 查询        
 zong=rzrq()
 rzrqcode=list(pd.unique(zong['code']))       
# st股  股票代码中文名字中，含有 * 以及st字样的。
 stcode=stt(ccdd,ccnn)
#蓝筹股

# 绩优股
 codd=real.stkcode
 jyg=getjiyougu(codd,real)
# 成长股
# 查询
 zong=zsn()
 zong2=zong[zong['length']>=3]
 zong3=zong2[zong2['date']=='20161231']
 zong4=zong3['code']
 czgcode=czg(zong4,real)
# 白马股
 codd=real.stkcode
 bmgcode=bmg(codd,real)
## 2.1.2 行业

# 2.2.1-2.2.2    
# 上涨，下跌，震荡，量增，量减，还手划分。
 zong=hangqinghuafen(ccdd)
 sz=zong[0]
 xd=zong[1]
 zd=zong[2]
 lz=zong[3]
 lj=zong[4]
# 还手划分
 zong2=turnover(ccdd)
 wl=zong2[0]
 cjdm=zong2[1]
 cjwh=zong2[2]
 cjhy=zong2[3]
 dl=zong2[4]
 fl=zong2[5]
 jl=zong2[6]
 tl=zong2[7]
# 股票上涨 涨停 下跌 跌停
 zg=jgzd(ccdd)
 gpzt=zg[0]
 gpsz=zg[1]
 gpxd=zg[2]
 gpdt=zg[3]
 gpcp=zg[4]



#2.2.2.3 
# 资金流划分
 zong=zijinliu(ccdd)
 lr=zong[0]
 lc=zong[1]
#=======================================================================2.2.3========================================================================

 
## ================================================================================================================== 
 code=pd.DataFrame(data=ccdd,columns=['stk'])
 hs=[]
 for i in ccdd:
     if i in hszb:
         hs.append(1)
     else:
         hs.append(0)
 code['hszb']=hs      
 hs=[]
 for i in ccdd:
     if i in szzb:
         hs.append(1)
     else:
         hs.append(0)
 code['szzb']=hs          
 hs=[]
 for i in ccdd:
     if i in zxb1:
         hs.append(1)
     else:
         hs.append(0)
 code['zxb']=hs   
 hs=[]
 for i in ccdd:
     if i in cyb1:
         hs.append(1)
     else:
         hs.append(0)
 code['cyb']=hs   
 hs=[]
 for i in ccdd:
     if i in rzrqcode:
         hs.append(1)
     else:
         hs.append(0)
 code['rzrq']=hs   

#龙虎榜
 lhb1 = getLHB(TRD_DATE)

 hs=[]
 for i in ccdd:
     if i in lhb1:
         hs.append(1)
     else:
         hs.append(0)
 code['lhb']=hs 
  



 hs=[]
 for i in ccdd:
     if i in stcode:
         hs.append(1)
     else:
         hs.append(0)
 code['st']=hs   
 hs=[]
 for i in ccdd:
     if i in jyg:
         hs.append(1)
     else:
         hs.append(0)
 code['jyg']=hs
 hs=[]
 for i in ccdd:
     if i in czgcode:
         hs.append(1)
     else:
         hs.append(0)
 code['czg']=hs  
 hs=[]
 for i in ccdd:
     if i in bmgcode:
         hs.append(1)
     else:
         hs.append(0)
 code['bmg']=hs 


 hs=[]
 for i in ccdd:
     if i in sz:
         hs.append(1)
     else:
         hs.append(0)
 code['sz']=hs 
 hs=[]
 for i in ccdd:
     if i in xd:
         hs.append(1)
     else:
         hs.append(0)
 code['xd']=hs 
 hs=[]
 for i in ccdd:
     if i in zd:
         hs.append(1)
     else:
         hs.append(0)
 code['zd']=hs
 hs=[]
 for i in ccdd:
     if i in lz:
         hs.append(1)
     else:
         hs.append(0)
 code['lz']=hs
 hs=[]
 for i in ccdd:
     if i in lj:
         hs.append(1)
     else:
         hs.append(0)
 code['lj']=hs
 
 hs=[]
 for i in ccdd:
     if i in gpzt:
         hs.append(1)
     else:
         hs.append(0)
 code['gpzt']=hs
 
 hs=[]
 for i in ccdd:
     if i in gpsz:
         hs.append(1)
     else:
         hs.append(0)
 code['gpsz']=hs

 hs=[]
 for i in ccdd:
     if i in gpxd:
         hs.append(1)
     else:
         hs.append(0)
 code['gpxd']=hs
 
 
 hs=[]
 for i in ccdd:
     if i in gpdt:
         hs.append(1)
     else:
         hs.append(0)
 code['gpdt']=hs
 
 
 hs=[]
 for i in ccdd:
     if i in gpcp:
         hs.append(1)
     else:
         hs.append(0)
 code['gpcp']=hs
 
 
 
 hs=[]
 for i in ccdd:
     if i in wl:
         hs.append(1)
     else:
         hs.append(0)
 code['wl']=hs
 hs=[]
 for i in ccdd:
     if i in cjdm:
         hs.append(1)
     else:
         hs.append(0)
 code['cjdm']=hs
 hs=[]
 for i in ccdd:
     if i in cjwh:
         hs.append(1)
     else:
         hs.append(0)
 code['cjwh']=hs
 hs=[]
 for i in ccdd:
     if i in cjhy:
         hs.append(1)
     else:
         hs.append(0)
 code['cjhy']=hs
 hs=[]
 for i in ccdd:
     if i in dl:
         hs.append(1)
     else:
         hs.append(0)
 code['dl']=hs
 hs=[]
 for i in ccdd:
     if i in fl:
         hs.append(1)
     else:
         hs.append(0)
 code['fl']=hs
 hs=[]
 for i in ccdd:
     if i in jl:
         hs.append(1)
     else:
         hs.append(0)
 code['jl']=hs
 hs=[]
 for i in ccdd:
     if i in tl:
         hs.append(1)
     else:
         hs.append(0)
 code['tl']=hs
 hs=[]
 for i in ccdd:
     if i in lr:
         hs.append(1)
     else:
         hs.append(0)
 code['lr']=hs
 hs=[]
 for i in ccdd:
     if i in lc:
         hs.append(1)
     else:
         hs.append(0)
 code['lc']=hs
 

 return  code




def getTagData(biaoqian):
    dfHqTag = pd.DataFrame(columns =['TRD_DATE','STK_CODE', 'TAG_VALUE', 'TAG_CODE'])
    tag_class = TAG_CLASS.keys()
    TRD_DATE = "20171221"
    
    jdx = 0
    for idx in range(len(biaoqian)):
        everydata = biaoqian.iloc[idx]
        STK_CODE = str(everydata['stk'])
        print STK_CODE
        for TAG_CODE in tag_class:
            filed_list = TAG_CLASS[TAG_CODE]
            for filed_item in filed_list:
                if not TAG_FILED[filed_item]['islot']:
                    if TAG_FILED[filed_item]['is1']:
                        if everydata[filed_item] == 1:
                            TAG_VALUE = TAG_FILED[filed_item]['name']
                            dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
                            dfHqTag.loc[jdx,'STK_CODE'] = STK_CODE
                            dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
                            dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
                            jdx = jdx +1
    
    #                        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
                        else:
                            pass
                    else:
                        TAG_VALUE = everydata[filed_item]
    
                        dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
                        dfHqTag.loc[jdx,'STK_CODE'] = STK_CODE
                        dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
                        dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
                        jdx = jdx +1
    
    #                    insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
                else:
                    for TAG_VALUE in everydata[filed_item] :
                        if TAG_VALUE != "0":
                            pass
    
                            dfHqTag.loc[jdx,'TRD_DATE'] = TRD_DATE
                            dfHqTag.loc[jdx,'STK_CODE'] = STK_CODE
                            dfHqTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
                            dfHqTag.loc[jdx,'TAG_CODE'] = TAG_CODE
                            jdx = jdx +1
    
    
    #                        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)

    return dfHqTag

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

def insertdata(dfHqTag):
    LabConnect,curLab = startconnect("mysql_test")
    for idx in range(len(dfHqTag)):
        STK_CODE = dfHqTag.loc[idx,'STK_CODE']
        TAG_VALUE = dfHqTag.loc[idx,'TAG_VALUE']
        TAG_CODE = dfHqTag.loc[idx,'TAG_CODE']
        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)


if   __name__ == '__main__':
    lastCheckDay = 0
    logDate = 0

    isdwn = False
    while True:        
        curtime = time.localtime(time.time())
        if (logDate != curtime.tm_mday):#新建当日的日志文件
            logDate = curtime.tm_mday
            iscau = False
        if (20*60+16 < time.localtime(time.time()).tm_hour*60+time.localtime(time.time()).tm_min < 23*60 + 45) & (not iscau):
            isTrade ,TRD_DATE, PreTrdDate = istrade(curtime) 
            if isTrade : 
                biaoqian=run(TRD_DATE)
                dfHqTag = getTagData(biaoqian)
                insertdata(dfHqTag)
                iscau = True



# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:45:21 2017

@author: 9547
"""
from __future__ import division
import  pandas  as  pd
import  datetime
import numpy as np
import time
import json
from urllib2 import urlopen
import sys
import pickle



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



class   Calcu_HisData(object):
    def __init__(self):
        self._Period=200

 
    #获取当前所属星期几的函数
    def get_week_day(self,date):
      week_day_dict = {
        0 : 1,
        1 : 2,
        2 : 3,
        3 : 4,
        4 : 5,
        5 : 6,
        6 : 7,
      }
      day = date.weekday()
      return week_day_dict[day]

          
    #获取股票池信息的函数，从行情接口获取        
    def GetAllStkInfo(self):
        self._market = []#股票市场属性
        self._stkpool = []#股票池
        self._stkpool_name=[]#股票名称
        #调取行情接口中市场属性的码表        
        reponse=urlopen("http://hqdata.upchina.com/data/regStkDict?market=1&type=6").read()
        data=json.loads(reponse)['vStock']
        for strobj in data:
            self._stkpool.append(strobj['sCode'])
            self._stkpool_name.append(strobj['sName'])
            self._market.append(strobj['shtSetcode'])
        self.Stk2Mkt=pd.DataFrame(self._market,index=self._stkpool)



    #获取大盘行情函数
    def GetIndexData(self): 
        OPEN_PRICE=[]
        HIGH_PRICE=[]
        LOW_PRICE=[]
        CLOSE_PRICE=[]
        RISE_DROP_RANGE=[]
        TRADE_VOL=[]
        TRADE_AMUT=[]
        TRADE_DATE=[]         
        reponse=urlopen("http://hqdata.upchina.com/data/kline?num="+str(self._Period)+"&market=1&code=000001&offset=1").read()
        data=json.loads(reponse.decode())['vAnalyData']
        for i in range(0,len(data)):
            strobj=data[i]
            CLOSE_PRICE.append(strobj['fClose'])
            HIGH_PRICE.append(strobj['fHigh'])
            LOW_PRICE.append(strobj['fLow'])
            OPEN_PRICE.append(strobj['fOpen'])
            TRADE_VOL.append(strobj['lVolume'])
            TRADE_AMUT.append(strobj['fAmount'])
            TRADE_DATE.append(datetime.datetime.strptime(str(strobj['sttDateTime']['iDate']), "%Y%m%d"))
            if i<1:#第一个日期无法计算涨跌幅
                RISE_DROPRANGE=np.nan
            else:
                RISE_DROPRANGE=100*(data[i]['fClose']/data[i-1]['fClose']-1)#涨跌幅公式
            RISE_DROP_RANGE.append(RISE_DROPRANGE)            
        dfOra_Index=pd.DataFrame({'OPEN_PRICE':OPEN_PRICE,'HIGH_PRICE':HIGH_PRICE,'LOW_PRICE':LOW_PRICE,'CLOSE_PRICE':CLOSE_PRICE,'RISE_DROP_RANGE':RISE_DROP_RANGE,'TRADE_VOL':TRADE_VOL},index=TRADE_DATE)
        self._dfOra_Index=dfOra_Index        
        print '大盘交易数据已完成下载'



    #获取个股截至前一交易日过去一年内交易数据          
    def GetAllStkData(self):
        self.GetAllStkInfo()
        self._dfOra_Stk={}
        self._dfOra_StkAda={}
        _stkpool=self._stkpool#股票池中的各股
        _Period=self._Period#一年内的交易日数
        _market=self._market#各股所在的市场，行情接口参数里需要
        self._dfOra_StkAda['RISE_DROP_RANGE']=pd.DataFrame()
        self._dfOra_StkAda['CLOSE_PRICE']=pd.DataFrame()
        offset=0#下载前一交易日数据    
        for istk in range(0,len(_stkpool)):
            STK_CODE=_stkpool[istk]  
            OPEN_PRICE=[]
            HIGH_PRICE=[]
            LOW_PRICE=[]
            CLOSE_PRICE=[]
            RISE_DROP_RANGE=[]
            TRADE_VOL=[]
            TRADE_AMUT=[]
            TRADE_DATE=[] 
            markt=_market[istk]#用于判断个股所在的市场，行情接口里的参数需要用到
            testnum=0
            while testnum<=5:
                try:
                    reponse=urlopen("http://hqdata.upchina.com/data/kline?num="+str(_Period)+"&market="+str(markt)+"&code="+STK_CODE+"&offset="+str(offset),timeout=30).read()
                    data=json.loads(reponse.decode())['vAnalyData']
                    break
                except Exception,e:#读取异常的处理 ：暂停一秒继续下载
                    print '------'+str(STK_CODE)+"出现异常"+str(e)
                    testnum=testnum+1

            if len(data)<_Period:
                if(len(data)==0)|(len(data)==1):#如果获取的数据都没有交易数据，则赋空值
                    stkdata=pd.DataFrame(columns=['OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','RISE_DROP_RANGE','TRADE_VOL','TRADE_AMUT'])
                else:
                    data=data[:-1]
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

            self._dfOra_Stk[STK_CODE]=stkdata
            #计算_dfOra_StkAda
            Oridata=self._dfOra_Stk[STK_CODE].ix[:,['CLOSE_PRICE','RISE_DROP_RANGE']]
            Newdata=pd.DataFrame(columns=['CLOSE_PRICE','RISE_DROP_RANGE'],index=self._dfOra_Index.index)
            Newdata[Newdata.index.isin(Oridata.index)]=Oridata
            self._dfOra_StkAda['RISE_DROP_RANGE'][STK_CODE]=Newdata['RISE_DROP_RANGE'].sort_index(axis='index')
            self._dfOra_StkAda['CLOSE_PRICE'][STK_CODE]=Newdata['CLOSE_PRICE'].sort_index(axis='index')
            #提示进度
            sys.stdout.write( '\r%.2f%s\r'%(100*(istk/len(_stkpool)),'%的个股已完成下载'))
            sys.stdout.flush()

    #存入本地
    def Save2Local(self):   
        self.hqdata={}
        self.hqdata['_dfOra_Index']=self._dfOra_Index
        self.hqdata['_dfOra_Stk']=self._dfOra_Stk
        self.hqdata['mktnum']=self._market
        self.hqdata['stklist']=self._stkpool
        output = open('hqdata.pkl', 'wb')
        pickle.dump(self.hqdata, output)
        output.close()
        
if   __name__ == '__main__':
    lastCheckDay = 0
    logDate = 0
    isdwn = False
    while True:        
        curtime = time.localtime(time.time())
        if (logDate != curtime.tm_mday):#新建当日的日志文件
            logDate = curtime.tm_mday
            iscau = False
            
        if (15*60+10 < time.localtime(time.time()).tm_hour*60+time.localtime(time.time()).tm_min < 23*60 + 45) & (not iscau):
            isTrade ,TRD_DATE, PreTrdDate = istrade(curtime) 
            if isTrade : 
                demo=Calcu_HisData()
                demo.GetAllStkInfo()
                demo.GetIndexData()
                demo.GetAllStkData()
                demo.Save2Local()

                iscau = True      
                print "over"
                
        time.sleep(58)
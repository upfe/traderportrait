# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 19:53:37 2017

@author: 9547
"""


from __future__ import division
import  pandas  as  pd
import  datetime
import numpy as np
import json
from urllib2 import urlopen
import time
import pickle
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



def GetNewDate():
    objData=None
    testnum=0#卡顿容许的次数
    data=pd.DataFrame()#原始行情交易数据
    while testnum<=5:
        try:
            f=urlopen("http://hqdata.upchina.com/data/stockHq?market=1&code=000001",timeout=30) 
            reponse=f.read()
            data=json.loads(reponse)['vStockHq']
            break
        except Exception,e:#读取异常的处理 ，并重新读取   
            print e
            testnum=testnum+1
    if len(data)==0:#如果获取的数据都没有交易数据，则赋空值
        objData=None
    else:
        strobj=data[0]
        objData=datetime.datetime.strptime(str(strobj['stExHq']['iTradeDate']), "%Y%m%d")
    return objData      

def send_mail(mail_msg):
    
    _user = "upchinawyy@126.com"
    _pwd = "upchina9547"
    _to = ['yiyong.wu@upchina.com']
    
    #如名字所示Multipart就是分多个部分
    msg = MIMEMultipart()
    msg["Subject"] = "个股诊断事件提醒"
    msg["From"] = _user
    msg["To"] = ','.join(_to)
    try:  
        part=MIMEText(mail_msg,'html','utf-8')
        msg.attach(part)
        s = smtplib.SMTP("smtp.126.com", timeout=300)#连接smtp邮件服务器,端口默认是25
        s.login(_user, _pwd)#登陆服务器
        s.sendmail(_user, _to, msg.as_string())#发送邮件
        s.close()   
        return True  
    except Exception, e:  
        print str(e)  
        return False  




#获取当前所属星期几的函数
def get_week_day(date):
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


class   STK_Diagnosis_PriVol(object):
    def __init__(self):

        #日志记录开始
        self.file_object = open('privol.log', 'w')




    #加载本地历史行情（hqdata.pkl）以及历史偏离度数据 （devidata.pkl）
    def Load_Data(self):
        #加载行情存量数据        
        pkl_file = open('hqdata.pkl', 'rb')
        hqdata=pickle.load(pkl_file)
        pkl_file.close()
        self._dfOra_Index=hqdata['_dfOra_Index']
        self._dfOra_Stk=hqdata['_dfOra_Stk']




    #从行情接口获取未处理的交易数据
    def Get_Data_From_Line(self,_Period,markt,STK_CODE,offset):
        f=urlopen("http://hqdata.upchina.com/data/kline?num="+str(_Period)+"&market="+str(markt)+"&code="+str(STK_CODE)+"&offset="+str(offset),timeout=30)
        reponse=f.read()
        data=json.loads(reponse.decode())['vAnalyData']
        return data


    #下载行情数据函数，处理原始交易数据 
    def DwnHqData(self,_Period,markt,offset,STK_CODE):
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
                data=self.Get_Data_From_Line(_Period,markt,STK_CODE,offset)
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

    #更新上证指数行情数据，从数据中心获取           
    def UpdateIndexData(self):
        print "正尝试更新上证行情数据！"
        if (datetime.datetime.now() -self._dfOra_Index.index[-1]).days>0:
            dfOra_IndexN=self.DwnHqData(2,1,0,'000001')#多下载一天
            #确定数据状态以及最新的交易日期和次交易日期
            if (len(dfOra_IndexN)==0):#无当天数据问题
                self.updt_state=0
                self._NewTradeDate = self._dfOra_Index.index[-1]
                self._NewTradeDate_=self._dfOra_Index.index[-2]
                mail_msg= "无法获取当天数据，故没有更新任何行情数据,问题可能在于数据源，请检查"
                send_mail(mail_msg)

            else :
                if (self._dfOra_Index.index[-1]==dfOra_IndexN.index[-1]):#假日因素导致无数据
                    self.updt_state=0#当天节假日问题
                    self._NewTradeDate = self._dfOra_Index.index[-1]
                    self._NewTradeDate_=self._dfOra_Index.index[-2]
                    mail_msg= "假日因素导致无新数据，故没有更新任何行情数据，请注意"
                    send_mail(mail_msg)                
    
                else:#数据获取正常
                    self.updt_state=1
                    self._NewTradeDate = dfOra_IndexN.index[-1]
                    self._NewTradeDate_ = self._dfOra_Index.index[-1]
                    self._dfOra_Index=pd.concat([self._dfOra_Index,dfOra_IndexN[-1:]])

        else:#今天已经更新完毕
            self.updt_state=0
            self._NewTradeDate = self._dfOra_Index.index[-1]
            self._NewTradeDate_=self._dfOra_Index.index[-2]
        self._Period=len(self._dfOra_Index)

    #获取股行业码表信息的函数，从行情接口获取        
    def UpdateStkInduInfo(self):
        self._market = []#个股所属市场的码表
        self._stkpool = []#个股码表

        testnum=0
        while testnum<=5:
            try:
                reponse=urlopen("http://hqdata.upchina.com/data/regStkDict?market=1&type=6").read()
                data=json.loads(reponse)['vStock']
                for strobj in data:
                    self._stkpool.append(strobj['sCode'])
                    self._market.append(strobj['shtSetcode'])
                break
            except Exception,e:#读取异常的处理 ，并重新读取   
                print e
                testnum=testnum+1

        
    #更新个股数据，从行情接口获取  
    def UpdateStkData(self):
        print "正更新个股行情数据！"
        self._dfOra_StkAda={}#对齐后的数据
        self._dfOra_StkAda['RISE_DROP_RANGE']=pd.DataFrame()
        self._dfOra_StkAda['CLOSE_PRICE']=pd.DataFrame()
        stkpool=self._stkpool#当前股票池
        stklen=len(stkpool)
        for stknum in range(0,stklen):
            STK_CODE=stkpool[stknum]
            offset=0
            markt=self._market[stknum]#用于判断个股所在的市场，行情接口里的参数需要用到
            if self._dfOra_Stk.has_key(STK_CODE):
                stk=self._dfOra_Stk[STK_CODE]
                if (len(stk)==0):#过往的历史里无数据，只有字段名
                    stkdata=pd.DataFrame(columns=['OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','RISE_DROP_RANGE','TRADE_VOL','TRADE_AMUT'])
                else:
                    if (datetime.datetime.now() -stk.index[-1]).days==0:#已是最新数据，无需更新
                        stkdata=pd.DataFrame(columns=['OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE','RISE_DROP_RANGE','TRADE_VOL','TRADE_AMUT'])
                    elif (datetime.datetime.now() -stk.index[-1]).days>0:
                        _Period=(datetime.datetime.now() -stk.index[-1]).days+1
                        stkdata=self.DwnHqData(_Period,markt,offset,STK_CODE)
                self._dfOra_Stk[STK_CODE]=self._dfOra_Stk[STK_CODE].combine_first(stkdata[1:])
            else:
                _Period=len(self._dfOra_Index)
                stkdata=self.DwnHqData(_Period,markt,offset,STK_CODE)
                self._dfOra_Stk[STK_CODE]=stkdata
            #计算_dfOra_StkAda
            Oridata=self._dfOra_Stk[STK_CODE].ix[:,['CLOSE_PRICE','RISE_DROP_RANGE']]
            Newdata=pd.DataFrame(columns=['CLOSE_PRICE','RISE_DROP_RANGE'],index=self._dfOra_Index.index)
            Newdata[Newdata.index.isin(Oridata.index)]=Oridata
            self._dfOra_StkAda['RISE_DROP_RANGE'][STK_CODE]=Newdata['RISE_DROP_RANGE'].sort_index(axis='index')
            self._dfOra_StkAda['CLOSE_PRICE'][STK_CODE]=Newdata['CLOSE_PRICE'].sort_index(axis='index')




    #更新大盘和个股数据
    def Updt_All_Data(self):
        self.UpdateIndexData()
        if self.updt_state==1:
            self.UpdateStkInduInfo()
            self.UpdateStkData()


    #删除多余数据以备后续存入本地
    def Del_HqData(self):
        self._dfOra_Index=self._dfOra_Index.drop([self._dfOra_Index.index[0]],axis=0)
        
        for keycode in self._dfOra_Stk.keys():
            if len(self._dfOra_Stk[keycode])<2:
                pass
            else:
                self._dfOra_Stk[keycode]=self._dfOra_Stk[keycode].drop([self._dfOra_Stk[keycode].index[0]],axis=0)


    #存入本地
    def Save_Data(self): 
        #先删数据
        self.Del_HqData()   
		#Save2Local
    #构造字典存储hqdata
        self.hqdata={}
        self.hqdata['_dfOra_Index']=self._dfOra_Index
        self.hqdata['_dfOra_Stk']=self._dfOra_Stk
        self.hqdata['mktnum']=self._market
        self.hqdata['stklist']=self._stkpool
        output = open('hqdata.pkl', 'wb')
        pickle.dump(self.hqdata, output)
        output.close()





lastCheckDay = 0
logDate = 0
isdwn = False
while True:
    curtime = time.localtime(time.time())
    if (logDate != curtime.tm_mday):#新建当日的日志文件
        logDate = curtime.tm_mday
        isdwn = False
    if ((lastCheckDay != curtime.tm_mday) & (not isdwn) & (60*curtime.tm_hour + curtime.tm_min > 60*14+8) &(get_week_day(datetime.datetime.now())!=6)&(get_week_day(datetime.datetime.now())!=7)):#非周末情况下，下午三点自动运行
        if GetNewDate().day==datetime.datetime.now().day:
            demo=STK_Diagnosis_PriVol()
            demo.Load_Data()
            demo.Updt_All_Data()
            demo.Save_Data()
            isdwn = True

    else:
        time.sleep(57)#每隔57秒自动运行
        print "已完成下载"

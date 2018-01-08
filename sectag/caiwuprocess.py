# -*- coding: utf-8 -*-
from __future__ import division
import  cx_Oracle   as  ora
import pandas as pd
import datetime
import numpy as np
from math import isnan
from scipy import stats
import os
import time
from sqlalchemy import create_engine
import MySQLdb
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from urllib2 import urlopen

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',\
                    datefmt='%Y-%b-%d %H:%M:%S', filename='fin_value_test3.log', filemode='w')
def tic():
    globals()['tt'] = time.clock()
    
def toc():
    print '\nElapsed time: %.8f seconds\n' % (time.clock()-globals()['tt'])

tic()   
'''
@function 发送邮件
'''
_user = "upchinawyy@126.com"
_pwd ="upchina9547" 
_to = ['barbuwu@upchina.com']

retry = 5       #获取Url失败的时候重试次数

def send_mail(mail_msg):
    #如名字所示Multipart就是分多个部分
    msg = MIMEMultipart()
    msg["Subject"] = "个股诊断异常提醒"
    msg["From"] = _user
    msg["To"] = ','.join(_to)
    logging.info("发送邮件, 内容:" + mail_msg)
    try:  
        part=MIMEText(mail_msg,'html','utf-8')
        msg.attach(part)
        s = smtplib.SMTP("smtp.126.com")#连接smtp邮件服务器，端口默认是25
        s.login(_user, _pwd)#登陆服务器
        s.sendmail(_user, _to, msg.as_string())#发送邮件
        s.close()   
        return True  
    except Exception,e: 
        print str(e)
        return False

#获取最新交易日
def Get_NewTradeDate():
    i = 0
    while i < retry:
        try:
            sUrl = "http://hqdata.upchina.com/data/kline?num=1&market=1&code=000001"
            logging.info("request = " + sUrl)
            f=urlopen(sUrl,timeout=30)
            reponse=f.read()
            data=json.loads(reponse.decode())['vAnalyData']
            return data
        except Exception, e:
            i += 1
            logging.warning("Exception:" + str(e)  + ", url = " + sUrl + ", retry = " + str(i + 1))
            continue
    logging.error("Retry " + str(i) + " times failed. url = " + sUrl)
    pass
    
def DwnHqData_trd_date(num,market,code):
    TRADE_DATE=[]
    testnum=0#卡顿容许的次数
    data=pd.DataFrame()#原始行情交易数据
    while testnum<=5:
        try:
            data=Get_NewTradeDate()
            break
        except Exception,e:#读取异常的处理 ,并重新读取   
            print e
            testnum=testnum+1
    if len(data)==0:#如果获取的数据都没有交易数据，则赋空值
        stkdata=pd.DataFrame(columns=['TRADE_DATE'])
    else:
        for i in range(0,len(data)):
            strobj=data[i]
            TRADE_DATE.append((strobj['sttDateTime'])['iDate'])
            stkdata=pd.DataFrame({'TRADE_DATE':TRADE_DATE})

    return stkdata   
    
def getDataFromSql(sql):
    i = 0
    while i < 5:
        try:
            _dsn = ora.makedsn(host = "101.37.136.153", port="10521", sid= "upwhdb") #阿里云的连接设置
            db = ora.connect('readonly', 'anegIjege', _dsn)
            cursor = db.cursor()
            logging.debug("Execute sql = " + sql)
            cursor.execute(sql)
            tempData = cursor.fetchall()
            tempData=pd.DataFrame(tempData)
            cursor.close()
            db.close()
            return tempData
        except Exception, e:
            logging.warning("Exception:" + str(e) + ", sql =" + sql + ", retry = " + str(i + 1))
            i += 1
            continue
    logging.error("Exception:" + str(e) + ", sql =" + sql + ", retry = " + str(i + 1))
    pass

'''
@attention:    用于从数据中心获取 个股诊断 数据
'''
class   Stock_Diagnosis_Data(object):
    
    def __init__(self):
        
        self._dsn = ora.makedsn(host= "120.26.15.171", port="1521", sid= "upwhdb") #阿里云的连接设置
        self._conn_Ora = ora.connect('readonly', 'anegIjege',self._dsn)
        #日志记录开始
        #self.file_object = open('fin_value_final.log','w') 
            
        
        self._stkpool_uni = []
        self._stkpool = []
        self._stkpool_num = 0
        self._NewTradeDate = 0#大盘最新交易日期
        self._stk_indu=pd.DataFrame()#用于存储股票代码和行业的对应关系
        self._fin_data=pd.DataFrame() #用于存储净利润,每股收益,每股经营活动产生的额现金流量净额,每股净资产
        self._fin_data_new=pd.DataFrame() #用于存储公司盈利能力(销售净利率、销售毛利率、净资产收益率)、成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率)、资产质量及负债(总资产周转率、应收账款周转率、资产负债率)
        self._cash_flow=pd.DataFrame()#用来存最新一期所有A股的最新一期的现金流数据
        self._insti_rate=pd.DataFrame() #用来存储机构评级的数据
        self._pre_eps=pd.DataFrame() #用来存储所有A股的预测eps
        self._pe_close=pd.DataFrame() #存储所有A股的pe和close
        self._mkt_num=[] #存储市场编号
        self._eps=pd.DataFrame() #存储所有A股的eps
        self._inc_eps=pd.DataFrame() #存储所有股票的净利润和每股收益
        self._fin_value_egc=pd.DataFrame()
        self._fin_value_egc_new=pd.DataFrame()
        self._test_data_update=[] #存储每次数据长度的list
    '''
    @function: 取 股票池股票 数据
    '''
    def Get_StkBaseInfo(self):
        '''
        strsql = "SELECT code FROM stock_baseinfo"
        dfStkInfo = getDataFromSql(strsql, self._conn_Mysql_hq)
        for stkcode in dfStkInfo["code"]:
            self._stkpool.append(stkcode)
        self._stkpool_num = len(self._stkpool)
        '''
        logging.info('----start加载A股市场所有股票代码\r\n')
        #self.file_object.flush()
        strsql = "select a.stk_code,a.STK_UNI_CODE from upcenter.STK_BASIC_INFO a where a.isvalid=1 and a.LIST_STA_PAR in ('1','4') and a.SEC_MAR_PAR in('1','2') and a.STK_TYPE_PAR=1 and a.LIST_DATE<trunc(sysdate)"
        logging.info("获取股票池数据 sql = " + strsql)        
        dfOra = getDataFromSql(strsql)
        dfOra.columns=['STK_CODE','STK_UNI_CODE']
        self._stkpool=list(dfOra['STK_CODE'])
        self._stkpool_num = len(self._stkpool)
        logging.info('----end加载完A股市场所有股票代码\r\n')
        
        #将股票代码和市场编号对应
        logging.info('----start将股票代码和市场编号对应\r\n')
        self._mkt_num=self._stkpool_num*[0]
        location=[self._stkpool.index(x) for x in self._stkpool  if x[0]=='6' ] 
        for loc in location:
            self._mkt_num[loc]=1
        logging.info('----end将股票代码和市场编号对应\r\n')
    
        #最新日期  
        logging.info('----start计算市场最新交易日期\r\n')
        self._NewTradeDate=datetime.datetime.strptime(str(list(DwnHqData_trd_date(1,1,"000001")['TRADE_DATE'])[0]),'%Y%m%d') 
        logging.info('----end计算市场最新交易日期\r\n')
       
        #股票代码和行业的关系   
        logging.info('----start下载股票代码和行业的关系\r\n')
#        strsql="select a.stk_code,c.INDU_NAME from upcenter.STK_BASIC_INFO a,upcenter.PUB_COM_INDU_RELA b,upcenter.PUB_INDU_CODE c where a.COM_UNI_CODE=b.COM_UNI_CODE and b.INDU_UNI_CODE=c.INDU_UNI_CODE and b.INDU_SYS_CODE=16 and c.INDU_SYS_PAR=15 and a.STK_TYPE_PAR=1 and a.SEC_MAR_PAR in ('1','2') and a.LIST_SECT_PAR in ('1','2','3') and a.LIST_STA_PAR in ('1','4') and a.LIST_DATE<trunc(sysdate)"
        strsql="select a.stk_code,c.INDU_NAME,c.SED_INDU_UNI_CODE from upcenter.STK_BASIC_INFO a,upcenter.PUB_COM_INDU_RELA b,upcenter.PUB_INDU_CODE c where a.COM_UNI_CODE=b.COM_UNI_CODE and b.INDU_UNI_CODE=c.INDU_UNI_CODE and b.INDU_SYS_CODE=15 and c.INDU_SYS_PAR=15  and a.STK_TYPE_PAR=1 and a.SEC_MAR_PAR in ('1','2') and a.LIST_SECT_PAR in ('1','2','3') and a.LIST_STA_PAR in ('1','4') and a.LIST_DATE<trunc(sysdate)"
        logging.info("下载股票代码和行业关系 sql = " + strsql)        
        self._stk_indu=getDataFromSql(strsql)
        self._stk_indu.columns=['STK_CODE','INDU_NAME','SED_INDU_UNI_CODE']
        logging.info('----end下载股票代码和行业的关系\r\n')
        
        #所有股票最新一期的财务数据
        logging.info('----start下载所有股票最新一期的财务数据\r\n')
        strsql="select STK_CODE,END_DATE,INC_I,BEPS,PS_OCF,BPS from (select b.STK_CODE,a.END_DATE,a.INC_I,a.BEPS,a.PS_OCF,a.BPS,ROW_NUMBER() OVER(partition by b.stk_code ORDER BY a.END_DATE desc) as rk from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and SEC_MAR_PAR in ('1','2') and LIST_SECT_PAR in ('1','2','3') and a.com_uni_code=b.com_uni_code and b.LIST_STA_PAR in ('1','4') and b.LIST_DATE<trunc(sysdate) order by a.END_DATE DESC) where rk=1"
        logging.info("下载所有股票最新一期的财务数据 sql = " + strsql)
        self._fin_data=getDataFromSql(strsql)
        self._fin_data.columns=['STK_CODE','END_DATE','INC_I','BEPS','PS_OCF','BPS']
        logging.info('----end下载所有股票最新一期的财务数据\r\n')
        #所有股票大于2011年的财务数据
        logging.info('----start下载所有股票大于2011年的财务数据\r\n')
        strsql="select b.STK_CODE,a.END_DATE,a.INC_I,a.BEPS,a.PS_OCF,a.BPS from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and SEC_MAR_PAR in ('1','2') and LIST_SECT_PAR in ('1','2','3') and a.com_uni_code=b.com_uni_code and b.LIST_STA_PAR in ('1','4') and b.LIST_DATE<trunc(sysdate) and extract (year from a.end_date)>2011 order by b.stk_code,a.END_DATE DESC"
        logging.info("下载所有股票大于2011年的财务数据 sql = " + strsql)        
        self._fin_data_all=getDataFromSql(strsql)
        self._fin_data_all.columns=['STK_CODE','END_DATE','INC_I','BEPS','PS_OCF','BPS']
        logging.info('----end下载所有股票大于2011年的财务数据\r\n')

        #取出最近两期的公司盈利能力(销售净利率、销售毛利率、净资产收益率)、成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率)、资产质量及负债(总资产周转率、应收账款周转率、资产负债率)
        logging.info('----start下载盈利能力、成长能力、资产质量及负债\r\n')
        strsql="select a.end_date,b.stk_code,a.SAL_NPR,a.SAL_GIR,a.ROEA,a.OR_YOY,a.OP_YOY,a.NP_YOY,a.TA_RATE,a.AP_RATE,a.BAL_P,a.BAL_O from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.com_uni_code=b.com_uni_code  and extract (year from a.end_date) >=extract(year from trunc(sysdate))-2 and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by stk_code desc,a.end_date desc"
        logging.info("下载盈利能力、成长能力、资产质量及负债 sql = " + strsql)        
        self._fin_data_new=getDataFromSql(strsql)
        self._fin_data_new.columns=['END_DATE', 'STK_CODE','SAL_NPR', 'SAL_GIR', 'ROEA', 'OR_YOY', 'OP_YOY', 'NP_YOY', 'TA_RATE', 'AP_RATE', 'BAL_P', 'BAL_O']
        self._fin_data_new['debt_to_ability']=self._fin_data_new['BAL_P']/self._fin_data_new['BAL_O'] 
        logging.info('----end下载盈利能力、成长能力、资产质量及负债\r\n')

        # 现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额) 单位是亿元
        logging.info('----start下载现金流\r\n')
        strsql="select end_date,stk_code,CS_10000,CS_20000,CS_30000 FROM (select a.end_date,b.stk_code,Cast(a.CS_10000/power(10,8) as decimal(18,4)) as CS_10000 ,Cast(a.CS_20000/power(10,8) as decimal(18,4)) as CS_20000,Cast(a.CS_30000/power(10,8) as decimal(18,4)) as CS_30000,row_number() over (partition by b.stk_code order by a.end_date desc) as rk from upcenter.FIN_CASH_SHORT a,upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE  and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by a.end_date desc) WHERE RK=1"
        logging.info("下载现金流 sql = " + strsql)        
        self._cash_flow=getDataFromSql(strsql)
        self._cash_flow.columns=['END_DATE', 'STK_CODE', 'CS_10000', 'CS_20000','CS_30000']
        logging.info('----end下载现金流\r\n')
        
        #本次机构评级
        logging.info('----start下载本次机构评级\r\n')
        strsql="SELECT C.STK_CODE,a.DECL_DATE,A.RES_RATE_PAR FROM UPCENTER.RES_STK_FORE_PRICE A , UPCENTER.PUB_ORG_INFO B,UPCENTER. STK_BASIC_INFO C WHERE A.ORG_UNI_CODE = B.ORG_UNI_CODE AND A.STK_UNI_CODE=C.STK_UNI_CODE AND DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND C.STK_TYPE_PAR=1 AND C.SEC_MAR_PAR IN ('1','2') AND C.LIST_SECT_PAR IN ('1','2','3') AND C.LIST_STA_PAR IN ('1','4') ORDER BY C.STK_CODE,A.DECL_DATE DESC"
        logging.info("下载机构评级 sql = " + strsql)        
        self._insti_rate=getDataFromSql(strsql)
        self._insti_rate.columns = ['STK_CODE', 'DECL_DATE', 'RES_RATE_PAR']
        logging.info('----end下载本次机构评级\r\n')
       
       #机构预测eps的均值
        logging.info('----start下载机构预测eps的均值\r\n')
        strsql="select stk_code,end_date,subj_avg from (select a.END_DATE,b.stk_code,FORE_YEAR,SUBJ_AVG,ROW_NUMBER() OVER (partition by b.stk_code ORDER BY a.END_DATE desc) as rk from upcenter.RES_COM_PROFIT_FORE a,upcenter.STK_BASIC_INFO b where a.isvalid=1 and a.SEC_UNI_CODE=b.STK_UNI_CODE and a.SUBJ_CODE=14 and FORE_YEAR=to_number(to_char(sysdate,'yyyy')) and STAT_RANGE_PAR=4 order by end_date desc,FORE_YEAR desc) where rk=1"
        logging.info("下载机构预测EPS的均值 sql = " + strsql)        
        self._pre_eps=getDataFromSql(strsql)
        self._pre_eps.columns=['STK_CODE', 'END_DATE', 'SUBJ_AVG']
        logging.info('----end下载机构预测eps的均值\r\n')
    
        #收盘价和最新的pe
        logging.info('----start下载收盘价和最新的pe\r\n')
        strsql="SELECT stk_code,trade_date,CLOSE_PRICE,STK_PER_TTM FROM (select a.stk_code,b.TRADE_DATE,b.CLOSE_PRICE,b.STK_PER_TTM,row_number() over (partition by a.stk_code ORDER BY b.TRADE_DATE DESC ) AS RK from upcenter.STK_BASIC_INFO a,upcenter.STK_BASIC_PRICE_MID b where a.STK_UNI_CODE=b.STK_UNI_CODE and a.isvalid=1 and b.end_date=b.trade_date and a.STK_TYPE_PAR=1 and a.SEC_MAR_PAR in ('1','2')  and a.LIST_SECT_PAR in ('1','2','3') and a.LIST_STA_PAR in ('1','4') order by b.Trade_Date desc) WHERE RK=1"
        logging.info("下载收盘价和最新的PE sql = " + strsql)        
        self._pe_close=getDataFromSql(strsql)
        self._pe_close.columns = ['STK_CODE','TRADE_DATE', 'CLOSE_PRICE', 'STK_PER_TTM']
        logging.info('----end下载收盘价和最新的pe\r\n')
        
        #个股的历史eps
        logging.info('----start下载个股历史eps\r\n')
        strsql="select end_date,stk_code,beps from (select a.END_DATE,b.STK_CODE,a.BEPS,row_number() over(partition by b.stk_code order by a.end_date desc) as rk from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE and b.STK_TYPE_PAR=1 and b.SEC_MAR_PAR in ('1','2') and b.LIST_SECT_PAR in ('1','2','3') and b.LIST_STA_PAR in ('1','4') and extract(month from a.end_date) = 12 order by b.stk_code, a.end_date desc) where rk<=10"
        logging.info("下载个股历史EPS sql = " + strsql)        
        self._eps=getDataFromSql(strsql)
        self._eps.columns = ['END_DATE', 'STK_CODE', 'BEPS']
        logging.info('---end下载个股历史eps\r\n')
        
        #找到市场上所有股票的净利润和每股收益
        logging.info('----start下载市场上所有股票的净利润和每股收益\r\n')
        strsql="select extract(year from a.end_date) as end_date,b.stk_code,a.beps,a.inc_i from upcenter.fin_idx_ana a,upcenter.STK_BASIC_INFO b where b.STK_TYPE_PAR=1 and b.SEC_MAR_PAR in ('1','2') and b.LIST_SECT_PAR in ('1','2','3') and b.LIST_STA_PAR in ('1','4') and a.com_uni_code=b.com_uni_code and extract(month from a.end_date)=12 and a.end_date>to_date('20111231','yyyy-mm-dd') order by b.stk_code,a.end_date desc"
        logging.info("下载市场上所有股票的净利润和每股收益 sql = " + strsql)        
        self._inc_eps=getDataFromSql(strsql)
        self._inc_eps.columns=['END_DATE', 'STK_CODE', 'BEPS', 'INC_I']
        logging.info('----end下载市场上所有股票的净利润和每股收益\r\n')
        
        #找到市场上所有股票盈利能力,成长能力,资产状况的指标(大于2011)
        logging.info('----start下载所有股票盈利能力,成长能力,资产状况的指标(大于2011)\r\n')
        strsql="select a.end_date,b.stk_code,a.SAL_NPR,a.SAL_GIR,a.ROEA,a.OR_YOY,a.OP_YOY,a.NP_YOY,a.TA_RATE,a.AP_RATE,a.BAL_P,a.BAL_O from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.com_uni_code=b.com_uni_code  and extract (year from a.end_date) >=2011 and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by stk_code,a.end_date desc"
        logging.info("下载所有股票盈利能力,成长能力,资产状况的指标(大于2011) sql = " + strsql)        
        self._fin_value_egc=getDataFromSql(strsql)
        self._fin_value_egc.columns = ['END_DATE', 'STK_CODE', 'SAL_NPR', 'SAL_GIR', 'ROEA', 'OR_YOY', 'OP_YOY', 'NP_YOY', 'TA_RATE', 'AP_RATE', 'BAL_P', 'BAL_O']
        logging.info('----end下载所有股票盈利能力,成长能力,资产状况的指标(大于2011)\r\n')

        
        #找到市场上所有股票盈利能力，成长能力，资产状况的指标 最新一期的
        logging.info('----start下载所有股票盈利能力,成长能力，资产状况的指标 最新一期的\r\n')
        strsql="select end_date,stk_code,SAL_NPR,SAL_GIR,ROEA,OR_YOY,OP_YOY,NP_YOY,TA_RATE,AP_RATE,BAL_P,BAL_O FROM (select a.end_date,b.stk_code,a.SAL_NPR,a.SAL_GIR,a.ROEA,a.OR_YOY,a.OP_YOY,a.NP_YOY,a.TA_RATE,a.AP_RATE,a.BAL_P,a.BAL_O,row_number() over (partition by b.stk_code order by a.end_date desc) AS RK from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.com_uni_code=b.com_uni_code  and extract (year from a.end_date) >=2011 and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by stk_code,a.end_date desc) WHERE RK=1"
        logging.info("下载所有股票盈利能力,成长能力，资产状况的指标 最新一期的 sql = " + strsql)        
        self._fin_value_egc_new=getDataFromSql(strsql)
        self._fin_value_egc_new.columns = ['END_DATE', 'STK_CODE', 'SAL_NPR', 'SAL_GIR', 'ROEA', 'OR_YOY', 'OP_YOY', 'NP_YOY', 'TA_RATE', 'AP_RATE', 'BAL_P', 'BAL_O']
        logging.info('----end下载所有股票盈利能力,成长能力,资产状况的指标 最新一期的\r\n')
        
    '''
    @function 判断数据中心的数据有没有到齐(如果前15分钟的数据和现在的数据是一样的,则判断数据已到齐)
    '''  
    def Test_Data_Updated(self):
        number=0
        while number<=48:
            if len(self._stkpool)>0 and len(self._fin_data)>0 and len(self._fin_data_all)>0 and len(self._fin_data_new)>0 and len( self._cash_flow)>0 and len(self._insti_rate)>0 and len(self._pre_eps)>0 and len(self._pe_close)>0 and len(self._pe_close)>0 and len(self._eps)>0 and len(self._inc_eps)>0 and len(self._fin_value_egc)>0 and len(self._fin_value_egc_new)>0:
                self._test_data_update.append([self._stkpool,self._fin_data,self._fin_data_all,self._fin_data_new,self._cash_flow,self._insti_rate,self._pre_eps,self._pe_close,self._eps,self._fin_value_egc,self._fin_value_egc_new])
                if len(self._test_data_update)>=2 and  self._test_data_update[-1]==self._test_data_update[-2]:
                    return True
                else:
                    time.sleep(300)
                    self.Test_Data_Updated()
                    number=number+1
            else:
                time.sleep(300)
                self.Test_DaTa_Updated()
                number=number+1
        while number>=49:
            return False

    '''
    @function 给出净利润，每股收益，每股经营活动产生的额现金流量净额，每股净资产的值和排名
    '''    
    def Get_Value_Rank(self,stkcode):
#        tradeyear = int(self._NewTradeDate.strftime("%Y"))
#        trademonth = int(self._NewTradeDate.strftime("%m"))
#        tradeday = int(self._NewTradeDate.strftime("%d"))
#        today = datetime.date.today()
#        yesterday = today - datetime.timedelta(days=1)
#        if  (tradeyear == yesterday.year and trademonth == yesterday.month and tradeday == yesterday.day):
#            pass
#        else:
#            return#不是的话就不计算，防止星期天重复计算上周五的数据
        logging.info('----start'+str(stkcode)+'净利润,每股收益，每股经营活动产生的额现金流量净额，每股净资产的值和排名\r\n')
        #得到个股所属的行业
        
        location=list(self._stk_indu['STK_CODE']).index(stkcode)
        indu_name=self._stk_indu.iloc[location,2] #得到个股所属行业
        indu_name0=self._stk_indu.iloc[location,1] #得到个股所属行业
        
        #得到个股所在行业的成分股
        stk_indu_consi=self._stk_indu[self._stk_indu['SED_INDU_UNI_CODE']==indu_name]
        stk_code=list(stk_indu_consi['STK_CODE'])#成分股代码
        fin_data=self._fin_data[self._fin_data['STK_CODE'].isin(stk_code)]
        #找到行业中的最小日期
        min_date=min(fin_data['END_DATE'])
        stock_code=list(fin_data[fin_data['END_DATE']!=min_date]['STK_CODE'])#找到行业中最新财务时间不是最小日期的股票代码
        date=str(min_date)[0:11].replace("-","")
        #将最新日期不是最小值的股票 替换为最小日期的值
        
        if len(stock_code)!=0:
            for code in stock_code:
                mid=self._fin_data_all[self._fin_data_all['STK_CODE']==code] #找到个股所有的数据 
                mid=mid[mid['END_DATE']==min_date]
                if not mid.empty:
                    fin_data=pd.concat([fin_data[fin_data['END_DATE']==min_date],mid])
                else:
                    pass
        
                
        #找到该只股票的四个维度的值以及行业排名
        num=fin_data.shape[0] #行业中的个股数
        if stkcode in list(fin_data['STK_CODE']):
            location=list(fin_data['STK_CODE']).index(stkcode)
            stk_inc_i=fin_data.iloc[location,2]
            if stk_inc_i!=None:
                stk_inc_i=round(fin_data.iloc[location,2]/(10**8),4)#/(10**8) #单位为亿元 净利润
            else:
                stk_inc_i="--"
                
            stk_beps=fin_data.iloc[location,3]
            if stk_beps!=None:
                stk_beps=round(fin_data.iloc[location,3],4) #每股收益
            else:
                stk_beps="--"
                
            stk_ps_ocf=fin_data.iloc[location,4]
            if stk_ps_ocf!=None:
                stk_ps_ocf=round(fin_data.iloc[location,4],4) #每股经营活动产生的现金流量净额
            else:
                stk_ps_ocf="--"
                
            stk_bps=fin_data.iloc[location,5]
            
            if stk_bps!=None:
                stk_bps=round(fin_data.iloc[location,5],4) #每股净资产
            else:
                stk_bps="--"
            
            if stk_inc_i!="--":
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="INC_I",ascending=False)
                rank_inc_i=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_inc_i="--"
                
            if stk_beps!="--":
                fin_data=fin_data.sort_values(by="BEPS",ascending=False)
                rank_beps=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_beps="--"
            
            if stk_ps_ocf!="--":
                fin_data=fin_data.sort_values(by="PS_OCF",ascending=False)
                rank_ps_ocf=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_ps_ocf="--"
            
            if stk_bps!="--":
                fin_data=fin_data.sort_values(by="BPS",ascending=False)
                rank_bps=list(fin_data['STK_CODE']).index(stkcode)+1
            else:
                rank_bps="--"
    
            descri_year=date[0:4]
            if date[4:6]=='03':
                descri_quar='第一季度'
            elif date[4:6]=='06':
                descri_quar='第二季度'
            elif date[4:6]=='09':
                descri_quar='第三季度'
            elif date[4:6]=='12':
                descri_quar='第四季度'
            #判断公司品质优秀与否
            length=fin_data.shape[0]
            if rank_beps!="--" and rank_inc_i=="":
                if np.floor(rank_beps/length*100)<=25:
                    descri_quali='综合以上数据分析，公司质地优秀。'
                elif np.floor(rank_beps/length*100)>25 and np.floor(rank_beps/length*100)<=50:
                    descri_quali='综合以上数据分析，公司质地良好。'
                elif np.floor(rank_beps/length*100)>50 and np.floor(rank_beps/length*100)<=75:
                    descri_quali='综合以上数据分析，公司质地一般。'
                elif np.floor(rank_beps/length*100)>75 and np.floor(rank_beps/length*100)<=100:
                    descri_quali='综合以上数据分析，公司质地较差。'
	    elif rank_beps!="--" and rank_inc_i!="":
                if np.floor(rank_beps/length*100)<=25:
                    descri_quali='综合以上数据分析，公司质地优秀。'
                elif np.floor(rank_beps/length*100)>25 and np.floor(rank_beps/length*100)<=50:
                    descri_quali='综合以上数据分析，公司质地良好。'
                elif np.floor(rank_beps/length*100)>50 and np.floor(rank_beps/length*100)<=75:
                    descri_quali='综合以上数据分析，公司质地一般。'
                elif np.floor(rank_beps/length*100)>75 and np.floor(rank_beps/length*100)<=100:
                    descri_quali='综合以上数据分析，公司质地较差。'
            elif  rank_beps=="" and rank_inc_i!="--":
                if np.floor(rank_inc_i/length*100)<=25:
                    descri_quali='综合以上数据分析，公司质地优秀。'
                elif np.floor(rank_inc_i/length*100)>25 and np.floor(rank_inc_i/length*100)<=50:
                    descri_quali='综合以上数据分析，公司质地良好。'
                elif np.floor(rank_inc_i/length*100)>50 and np.floor(rank_inc_i/length*100)<=75:
                    descri_quali='综合以上数据分析，公司质地一般。'
                elif np.floor(rank_inc_i/length*100)>75 and np.floor(rank_inc_i/length*100)<=100:
                    descri_quali='综合以上数据分析，公司质地较差。'
            else:
                descri_quali='综合以上数据分析，公司质地--。'
                    
            descri="所属行业为"+indu_name0+"，"+descri_year+"年"+descri_quar+"净利润为"+str(stk_inc_i)+"亿元，"+"排名行业第"+str(rank_inc_i)+";每股收益为"+str(stk_beps)+"元，"+"排名行业第"+str(rank_beps)+";"+"每股经营活动产生的现金流量净额为"+str(stk_ps_ocf)+"，排名行业第"+str(rank_ps_ocf)+";每股净资产为"+str(stk_bps)+"，排名行业第"+str(rank_bps)+"。"+descri_quali
            # return descri
        else:
            logging.debug("无法获取财务数据的值和排名 stkcode = " + stkcode)
            descri=""
            rank_inc_i="--"
            rank_beps="--"
            rank_ps_ocf="--"
            rank_bps="--"
            num=fin_data.shape[0] 
            descri_quali=""
        return descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali
        
    '''
    缺失机器语
    @function 公司盈利能力(销售净利率、销售毛利率、净资产收益率)、成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率)、资产质量及负债(总资产周转率、应收账款周转率、资产负债率)、现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额)
    '''
    def Company_Manage(self,stkcode):

        logging.info('----start'+str(stkcode)+'盈利能力、成长能力、资产质量及负债、现金流机器语的生成\r\n')
        dfOra=self._fin_data_new[self._fin_data_new['STK_CODE']==stkcode]
        month=list(dfOra['END_DATE'])[0].month
        fin_data_two=dfOra[dfOra['END_DATE'].isin(filter(lambda x:x.month==month,dfOra['END_DATE']))]
        '''
        盈利能力(销售净利率、销售毛利率、净资产收益率)
        '''
#        earning_ability=dfOra[dfOra['END_DATE'].isin(date)] #找到个股的盈利能力 分别对应 销售净利率、销售毛利率、净资产收益率
        #比较销售净利率、销售毛利率、净资产收益率 本期和上期的差别
        if fin_data_two.shape[0]>1:
            fin_data_two=fin_data_two.iloc[[0,1],:]
            dfOra=self._fin_data_new[self._fin_data_new['STK_CODE']==stkcode]
            month=list(dfOra['END_DATE'])[0].month
            fin_data_two=dfOra[dfOra['END_DATE'].isin(filter(lambda x:x.month==month,dfOra['END_DATE']))]
                
            if not isnan(list(fin_data_two['ROEA'])[0]) and not isnan(list(fin_data_two['ROEA'])[1]):
                if list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]>0 and list(fin_data_two['ROEA'])[1]>0:
                    descri_ROEA='公司盈利能力有所上升，公司业绩处于提升阶段。'
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]==0 and list(fin_data_two['ROEA'])[1]>0:
                    descri_ROEA='公司盈利能力不变，保持同期增长趋势。'
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]<0 and list(fin_data_two['ROEA'])[1]>0:
                    descri_ROEA='公司盈利能力低于预期，公司业绩有所下降。'
                    
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]>0 and list(fin_data_two['ROEA'])[1]<0:
                    descri_ROEA='公司盈利能力提升较快，扭亏为盈。'   
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]==0 and list(fin_data_two['ROEA'])[1]<0:
                    descri_ROEA='公司盈利能力不足，业绩可能持续下滑。'
                elif list(fin_data_two['ROEA'])[0]-list(fin_data_two['ROEA'])[1]<0 and list(fin_data_two['ROEA'])[1]<0:
                    descri_ROEA='公司盈利能力持续下滑，可能继续亏损。'            
            else:
                #descri_ROEA='财务数据不足，盈利能力暂无法判断'
                descri_ROEA=''
            descri_prf_abli=descri_ROEA
                
            '''
            成长能力(营业收入同比增长率、营业利润同比增长率、净利润同比增长率) 值都乘以了100
            '''
                    
            if not isnan(list(fin_data_two['NP_YOY'])[0]) and not isnan(list(fin_data_two['NP_YOY'])[1]):
                if  list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]>0 and list(fin_data_two['NP_YOY'])[1]>0:
                    descri_NP_YOY='公司成长能力有所提升，进入快速成长期。'
                elif list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]==0 and list(fin_data_two['NP_YOY'])[1]>0:
                    descri_NP_YOY='公司成长能力不变，保持强势。'
                elif list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]<0 and list(fin_data_two['NP_YOY'])[1]>0:
                     descri_NP_YOY='公司成长能力有所减弱，业务有所收缩。'
                elif  list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]>0 and list(fin_data_two['NP_YOY'])[1]<0:
                    descri_NP_YOY='公司成长能力大幅提升，由萎缩期转为扩张期。'
                elif list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]==0 and list(fin_data_two['NP_YOY'])[1]<0:
                    descri_NP_YOY='公司成长能力没有改进，保持弱势。'
                elif list(fin_data_two['NP_YOY'])[0]-list(fin_data_two['NP_YOY'])[1]<0 and list(fin_data_two['NP_YOY'])[1]<0:
                     descri_NP_YOY='公司成长能力持续下降，业务有加速萎缩趋势。'     
                     
            else:
    #            descri_NP_YOY='财务数据不足，成长能力暂无法判断'
                descri_NP_YOY=""
            descri_grw_abli=descri_NP_YOY
            
            '''
            资产质量及负债(总资产周转率、应收账款周转率、资产负债率) 
            '''    
                    
            if not list(fin_data_two['AP_RATE'])[0] is None and not list(fin_data_two['AP_RATE'])[1] is None:
                if not isnan(list(fin_data_two['AP_RATE'])[0]) and not isnan(list(fin_data_two['AP_RATE'])[1]):
                    if  list(fin_data_two['AP_RATE'])[0]-list(fin_data_two['AP_RATE'])[1]>0:
                       descri_AP_RATE='公司资金周转提速，资产流动性增加，生产经营活动加强。'
                    elif list(fin_data_two['AP_RATE'])[0]-list(fin_data_two['AP_RATE'])[1]==0:
                       descri_AP_RATE='公司资金周转稳定，资产流动性稳定，生产经营活动正常。'
                    elif list(fin_data_two['AP_RATE'])[0]-list(fin_data_two['AP_RATE'])[1]<0:
                        descri_AP_RATE='公司资金周转放缓，资产流动性变差，生产经营活动减弱。'
                else:
    #               descri_AP_RATE='财务数据不足，无法反映公司生产经营状况'
                    descri_AP_RATE=""
            else:
               #descri_AP_RATE='财务数据不足，无法反映公司生产经营状况' 
                descri_AP_RATE=""
            descri_ass_quali=descri_AP_RATE
             
            if not isnan(list(fin_data_two['debt_to_ability'])[0]) and not isnan(list(fin_data_two['debt_to_ability'])[1]):
                if  list(fin_data_two['debt_to_ability'])[0]-list(fin_data_two['debt_to_ability'])[1]>0:
                    descri_debt_to_ability='公司资产负债率上升，资产质量下降。'
                elif list(fin_data_two['debt_to_ability'])[0]-list(fin_data_two['debt_to_ability'])[1]==0:
                    descri_debt_to_ability='公司资产负债率不变，资产质量稳定。'
                elif list(fin_data_two['debt_to_ability'])[0]-list(fin_data_two['debt_to_ability'])[1]<0:
                     descri_debt_to_ability='公司资产负债率下降，资产质量提升。'
            else:
                descri_debt_to_ability=''
                '''
                返回负债
                      银行的资产负债率会比较高，后期需要区分银行业和非银行业，即if中需要加入list(fin_data_two['debt_to_ability'])[1]>0.6，
                一般企业资产负债率>0.6需要警示
                '''
            descri_solv_marg=descri_debt_to_ability  
            
        else:
            logging.warning("无法计算盈利能力, 成长能力, 资产状况, 偿债能力. stkcode = " + stkcode)
            descri_prf_abli=""
            descri_grw_abli=""
            descri_ass_quali=""
            descri_solv_marg=""  
        '''
        现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额) 单位是亿元
        '''         
        #strsql="select a.end_date，a.CS_10000，a.CS_20000，a.CS_30000 from upcenter.FIN_CASH_SHORT a，upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+" and a.end_date>=to_date('20110331'，'yyyy-mm-dd') order by a.end_date desc" 
#        dfOra=dfOra[dfOra['END_DATE'].isin(date)]
        cash_flow=self._cash_flow[self._cash_flow['STK_CODE']==stkcode]#cash_flow用来画图
        if not isnan(list(cash_flow['CS_10000'])[0])and not isnan(list(cash_flow['CS_20000'])[0]) and not isnan(list(cash_flow['CS_30000'])[0]):
            if np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])>=1:
                descri_cash_flow='现金流充足。'
            elif -1<np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])<1:
                descri_cash_flow='现金流能维持周转。'
            elif np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])<=-1:
                descri_cash_flow='现金流紧张。'
        else:
            descri_cash_flow=''
        
        return  descri_prf_abli,descri_grw_abli,descri_ass_quali,descri_solv_marg,descri_cash_flow           
    '''
    @function 机构评级
    '''     
    def Insti_Rate(self,stkcode):
       #获取3个月内机构评级信息
        #strsql = "SELECT A.STK_CODE，A.ORG_UNI_CODE，B.ORG_CHI_SHORT_NAME，A.INDU_UNI_CODE，TO_CHAR(A.DECL_DATE，'YYYY-MM-DD') AS DECL_DATE，A.LAST_RATE_PAR，A.RES_RATE_PAR，A.RATE_CHG_PAR FROM UPCENTER.RES_STK_FORE_PRICE A JOIN UPCENTER.PUB_ORG_INFO B ON A.ORG_UNI_CODE = B.ORG_UNI_CODE WHERE DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND A.STK_CODE ="+"'"+stkcode+"'"+"    ORDER BY A.DECL_DATE DESC"
#       strsql="SELECT C.STK_CODE，A.ORG_UNI_CODE，B.ORG_CHI_SHORT_NAME，A.INDU_UNI_CODE，TO_CHAR(A.DECL_DATE，'YYYY-MM-DD') AS DECL_DATE，A.LAST_RATE_PAR，A.RES_RATE_PAR，A.RATE_CHG_PAR FROM UPCENTER.RES_STK_FORE_PRICE A ， UPCENTER.PUB_ORG_INFO B，UPCENTER. STK_BASIC_INFO C WHERE A.ORG_UNI_CODE = B.ORG_UNI_CODE AND A.STK_UNI_CODE=C.STK_UNI_CODE AND DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND C.STK_CODE ="+"'"+stkcode+"'"+"  ORDER BY A.DECL_DATE DESC"
#       dfOra = getDataFromSql(strsql， self._dbcenter_conn_Ora)
#        strsql="SELECT A.RES_RATE_PAR FROM UPCENTER.RES_STK_FORE_PRICE A ， UPCENTER.PUB_ORG_INFO B，UPCENTER. STK_BASIC_INFO C WHERE A.ORG_UNI_CODE = B.ORG_UNI_CODE AND A.STK_UNI_CODE=C.STK_UNI_CODE AND DECL_DATE > TRUNC(SYSDATE)-90 AND A.ISVALID = 1 AND B.ISVALID = 1 AND C.STK_CODE ="+"'"+stkcode+"'"+"  ORDER BY A.DECL_DATE DESC"
#        dfOra = getDataFromSql(strsql， self._dbcenter_conn_Ora)
        logging.info('----start'+str(stkcode)+'机构评级\r\n')
        
        dfOra=self._insti_rate[self._insti_rate['STK_CODE']==stkcode]
        if not dfOra.empty:
            dfOra_buy=dfOra[dfOra['RES_RATE_PAR']==1]
            dfOra_incr=dfOra[dfOra['RES_RATE_PAR']==2]
            dfOra_mid=dfOra[dfOra['RES_RATE_PAR']==3]
            dfOra_decr=dfOra[dfOra['RES_RATE_PAR']==4]
            dfOra_sell=dfOra[dfOra['RES_RATE_PAR']==5]
            dfOra_other=dfOra[dfOra['RES_RATE_PAR']==6]
            if  dfOra_buy.shape[0]+dfOra_incr.shape[0]+ dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0]!=0:
                mid_add=(dfOra_incr.shape[0]+ dfOra_buy.shape[0])/(dfOra_incr.shape[0]+dfOra_buy.shape[0]+dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0])*100
                mid_mid=dfOra_mid.shape[0]/(dfOra_incr.shape[0]+dfOra_buy.shape[0]+dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0])*100
                mid_sub=(dfOra_decr.shape[0]+dfOra_sell.shape[0])/(dfOra_incr.shape[0]+dfOra_buy.shape[0]+dfOra_mid.shape[0]+dfOra_decr.shape[0]+dfOra_sell.shape[0])*100
                if mid_add>=50:
                    descri='多数机构认为该股长期投资价值较高，投资者可加强关注。'
                elif mid_mid>=50:
                    descri='多数机构认为该股长期具有投资价值，投资者可持续观望。'
                elif mid_sub>=50:
                    descri='多数机构认为该股长期投资价值不高，投资者可给予较少的关注。'
            else:
                 descri=""
        else:
            logging.warning("无法计算机构评级, stkcode = " + stkcode)
            descri=""
        return descri  
    
    '''
    @fucntion 预测eps
    '''
    def Get_pre_eps_roe(self,stkcode):
        #从数据中心得到eps，bps(每年12月的数据)
#        strsql="select a.END_DATE，b.STK_CODE，a.BEPS，a.BPS from upcenter.FIN_IDX_ANA a，upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE and b.STK_TYPE_PAR=1 and b.SEC_MAR_PAR in ('1'，'2') and b.LIST_SECT_PAR in ('1'，'2'，'3') and b.LIST_STA_PAR in ('1'，'4') and STK_CODE="+"'"+stkcode+"'"+" and extract(month from a.end_date) = 12 order by a.end_date desc"
#        stkcode='600000'
        logging.info('----start'+str(stkcode)+'预测eps\r\n')
        
        stk_eps_roe=self._eps[self._eps['STK_CODE']==stkcode]
        stk_eps_roe=stk_eps_roe.dropna()
        #用最小二乘法对eps，bps进行预测，
        if  stk_eps_roe.shape[0]>3:#此处也就是需要股票至少有三年不为0的年报
            min_year=min(stk_eps_roe['END_DATE']).year
            x=np.array([x.year for x in stk_eps_roe['END_DATE']])-min_year+1
            y_eps=np.array(stk_eps_roe['BEPS'])
            slope, intercept, r_value, p_value, slope_std_error = stats.linregress(x, y_eps)
            predict_stk_eps_1year=str(slope*(x[0]+1)+intercept)
        else:
            logging.warning("无法获取3年的研报 stkcode = " + stkcode)
            predict_stk_eps_1year="--"
        return predict_stk_eps_1year
        
    '''
    @function 用PE预测出的股价
    '''          
    def Get_pre_stock_pe_pb(self,stkcode):
         #判断当前系统日期的前一天是不是最新的交易日
#        tradeyear = int(self._NewTradeDate.strftime("%Y"))
#        trademonth = int(self._NewTradeDate.strftime("%m"))
#        tradeday = int(self._NewTradeDate.strftime("%d"))
#        today = datetime.date.today()
#        yesterday = today - datetime.timedelta(days=1)
#        if  (tradeyear == yesterday.year and trademonth == yesterday.month and tradeday == yesterday.day):
#            pass
#        else:
#            return#不是的话就不计算，防止星期天重复计算上周五的数据
       #  从数据中心取出PE、PE(TTM)、PB(得到的是当日的PE等的值)
#        strsql="select b.STK_CODE，a.END_DATE，a.Trade_Date，a.STK_PER，a.STK_PER_TTM，a.PRICE_BOOKV_RATIO from upcenter.STK_BASIC_PRICE_MID a，upcenter.STK_BASIC_INFO b，(select end_date from (select b.END_DATE，row_number()over(order by b.end_date desc) as rk from upcenter.STK_BASIC_PRICE_MID b where b.end_date < trunc(sysdate) and b.isvalid=1) where rk=1) c where a.STK_UNI_CODE=b.STK_UNI_CODE and STK_CODE="+"'"+stkcode+"'"+" and b.STK_TYPE_PAR=1 and b.SEC_MAR_PAR in ('1'，'2') and b.LIST_SECT_PAR in ('1'，'2'，'3') and b.LIST_STA_PAR in ('1'，'4') and a.end_date =c.end_date "
#        stk_pe_pb= getDataFromSql(strsql，self._dbcenter_conn_Ora) #个股pe，pe（ttm)，个股pb
       #计算预测股价(计算的是下年的市盈率)
        logging.info('----start'+str(stkcode)+'PE预测出的股价\r\n')
        
        stk_pe=self._pe_close[self._pe_close['STK_CODE']==stkcode]
        predict_stk_eps_1year=self.Get_pre_eps_roe(stkcode)
        
        if predict_stk_eps_1year!="--":
            if stk_pe['STK_PER_TTM'].values>0 and predict_stk_eps_1year>0 :
                stk_pre_pri=round(float(predict_stk_eps_1year)*list(stk_pe['STK_PER_TTM'])[0],4)#根据市盈率出来的预测股价
            else:
                stk_pre_pri="--"
        else:
            logging.warning("无法获取预测的EPS stkcode = " + stkcode)
            stk_pre_pri="--" 
        return stk_pre_pri
        
        
    '''
    @function 机构预测
    '''
    def Market_predict(self,stkcode):
        logging.info('----start'+str(stkcode)+'机构预测\r\n')
        #strsql="select subj_avg from (select a.END_DATE，b.stk_code，FORE_YEAR，SUBJ_AVG，ROW_NUMBER() OVER (ORDER BY a.END_DATE desc) as rk  from upcenter.RES_COM_PROFIT_FORE a，upcenter.STK_BASIC_INFO b where a.isvalid=1 and a.SEC_UNI_CODE=b.STK_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+"  and a.SUBJ_CODE=14 and FORE_YEAR=to_number(to_char(sysdate，'yyyy')) and STAT_RANGE_PAR=4 order by end_date desc，FORE_YEAR desc) where rk=1"
        #strsql="select a.END_DATE，b.stk_code，FORE_YEAR， SUBJ_AVG from upcenter.RES_COM_PROFIT_FORE a，upcenter.STK_BASIC_INFO b where a.isvalid=1 and a.SEC_UNI_CODE=b.STK_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+"  and a.SUBJ_CODE=14 and FORE_YEAR>=to_number(to_char(sysdate，'yyyy'))-1 and STAT_RANGE_PAR=4 order by end_date desc，FORE_YEAR desc"
        dfOra=self._pre_eps[self._pre_eps['STK_CODE']==stkcode]
        #strsql="SELECT CLOSE_PRICE，STK_PER_TTM FROM (select b.TRADE_DATE，b.CLOSE_PRICE，b.STK_PER_TTM，row_number() over (ORDER BY b.TRADE_DATE DESC ) AS RK from upcenter.STK_BASIC_INFO a，upcenter.STK_BASIC_PRICE_MID b where a.STK_UNI_CODE=b.STK_UNI_CODE and a.stk_code="+"'"+stkcode+"'"+" and a.isvalid=1 and b.end_date=b.trade_date  order by b.Trade_Date desc) WHERE RK=1"
        dfOra_pe_close=self._pe_close[self._pe_close['STK_CODE']==stkcode]
        dfOra_eps=self._eps[self._eps['STK_CODE']==stkcode]
        stk_pre_pri=self.Get_pre_stock_pe_pb(stkcode)
        sysdate=datetime.date.today().year #系统时间
        predict_stk_eps_1year=self.Get_pre_eps_roe(stkcode)
        stk_inc_eps=self._inc_eps[self._inc_eps['STK_CODE']==stkcode] #存储历史的inc和eps
        #有机构预测的数据 且 市盈率不为空....并且eps和pe 都大于0
        if not dfOra.empty and not dfOra_pe_close.empty and list(dfOra['SUBJ_AVG'])[0]>0 and list(dfOra_pe_close['STK_PER_TTM'])[0]>0:
            pre_stkprice=round(list(dfOra['SUBJ_AVG'])[0]*list(dfOra_pe_close['STK_PER_TTM'])[0],4)
            if  list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice*(1+0.3):
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价被明显高估。'
                descri_mid_long='该股估值严重偏高，预期会大幅调整，不建议长期持有，注意风险。'
            elif list(dfOra_pe_close['CLOSE_PRICE'])[0]<pre_stkprice*(1+0.3) and list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice:
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价偏高。'
                descri_mid_long='该股估值偏高，预期会进一步调整，不建议长期持有，注意风险。'
            elif list(dfOra_pe_close['CLOSE_PRICE'])[0]<pre_stkprice*(1-0.3):
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价被明显低估。'
                descri_mid_long='该股估值严重偏低，预期会大幅上涨，建议长期持有。'
            elif list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice*(1-0.3) and (list(dfOra_pe_close['CLOSE_PRICE'])[0])<pre_stkprice:
                descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价偏低。'
                descri_mid_long='该股估值偏低，预期会进一步上涨，建议长期持有。'
            else:
                descri=""
                descri_mid_long=""
        #有机构预测 但是eps或者pe为负
        elif not dfOra.empty and (list(dfOra['SUBJ_AVG'])[0]<=0 or list(dfOra_pe_close['STK_PER_TTM'])[0]<=0):
            descri='机构预测'+str(sysdate)+'年的每股收益为'+str(list(dfOra['SUBJ_AVG'])[0])+'，按当前市盈率计算，估值为--元。'
            descri_mid_long=""
        #无机构预测并且价格不为--
        elif dfOra.empty and not dfOra_pe_close.empty  and stk_pre_pri!="--" :
            pre_stkprice=stk_pre_pri
            
            #时间
            date=list(dfOra_eps['END_DATE'])[0].year+1
            if not isinstance(pre_stkprice,str):
                if  list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice*(1+0.3):
                    descri='优品预测'+str(date)+'年的每股收益为'+str(predict_stk_eps_1year)+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价被明显高估。'
                    descri_mid_long='该股估值严重偏高，预期会大幅调整，不建议长期持有，注意风险。'
                elif list(dfOra_pe_close['CLOSE_PRICE'])[0]<pre_stkprice*(1+0.3) and list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice:
                    descri='优品预测'+str(date)+'年的每股收益为'+str(predict_stk_eps_1year)+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价偏高。'
                    descri_mid_long='该股估值偏高，预期会进一步调整，不建议长期持有，注意风险。'
                elif list(dfOra_pe_close['CLOSE_PRICE'])[0]<pre_stkprice*(1-0.3):
                    descri='优品预测'+str(date)+'年的每股收益为'+str(predict_stk_eps_1year)+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价被明显低估。'
                    descri_mid_long='该股估值严重偏低，预期会大幅上涨，建议长期持有。'
                elif list(dfOra_pe_close['CLOSE_PRICE'])[0]>=pre_stkprice*(1-0.3) and (list(dfOra_pe_close['CLOSE_PRICE'])[0])<pre_stkprice:
                    descri='优品预测'+str(date)+'年的每股收益为'+str(predict_stk_eps_1year)+'，按当前市盈率计算，估值为'+str(pre_stkprice)+'元，当前股价偏低。'
                    descri_mid_long='该股估值偏低，预期会进一步上涨，建议长期持有。'
                else:
                    descri=""
                    descri_mid_long=""
            else:
                descri=""
                descri_mid_long=""
        elif dfOra.empty and  predict_stk_eps_1year!="--":
            date=list(dfOra_eps['END_DATE'])[0].year+1
            descri='机构预测'+str(date)+'年的每股收益为'+str(predict_stk_eps_1year)+'，按当前市盈率计算，估值为--元。'
            descri_mid_long=""
            
        else:
            descri="--"
            descri_mid_long=""
        #画图的数据，判断规则：如果机构有预测数据 则用机构的数据 否则就用优品预测的
        if not dfOra.empty:
            mid=pd.DataFrame([sysdate,stkcode,list(dfOra['SUBJ_AVG'])[0],np.nan]).T #当机构对该只股票有预测，并且取值不为空的时候
            mid.columns=['END_DATE','STK_CODE','BEPS','INC_I']
            stk_inc_eps=stk_inc_eps.append(mid)
            stk_inc_eps=stk_inc_eps.sort_values(by='END_DATE',ascending=False)
        elif not dfOra_eps.empty and predict_stk_eps_1year!="--":
            date=list(dfOra_eps['END_DATE'])[0].year+1
            mid=pd.DataFrame([date,stkcode,predict_stk_eps_1year,np.nan]).T
            mid.columns=['END_DATE','STK_CODE','BEPS','INC_I']
            stk_inc_eps=stk_inc_eps.append(mid)
            stk_inc_eps=stk_inc_eps.sort_values(by='END_DATE',ascending=False)
        
        return descri,stk_inc_eps,descri_mid_long
    
    
    '''
    @公司质地(改成盈利能力,成长能力，资产状况，现金状况)
    '''    
    def Get_Com_Quali(self,stkcode):
        #找到盈利能力的三个指标 在行业内的排名
        #先找到个股所在行业的成分股
        #得到个股所在行业的成分股
         #得到个股所属的行业
        logging.info('----start'+str(stkcode)+'综合面的盈利能力，成长能力，资产状况，现金状况\r\n')
    
        location=list(self._stk_indu['STK_CODE']).index(stkcode)
        indu_name=self._stk_indu.iloc[location,1] #得到个股所属行业
        stk_indu_consi=self._stk_indu[self._stk_indu['SED_INDU_UNI_CODE']==indu_name]
        stk_code=list(stk_indu_consi['STK_CODE'])#成分股代码
        
        fin_data= self._fin_value_egc_new[ self._fin_value_egc_new['STK_CODE'].isin(stk_code)]

        #找到行业中的最小日期
        min_date=min(fin_data['END_DATE'])
        stock_code=list(fin_data[fin_data['END_DATE']!=min_date]['STK_CODE'])#找到行业中最新财务时间不是最小日期的股票代码
#        date=str(min_date)[0:11].replace("-"，"")
        #将最新日期不是最小值的股票 替换为最小日期的值
        
        if len(stock_code)!=0:
            for code in stock_code:
                mid=self._fin_value_egc[self._fin_value_egc['STK_CODE']==code] #找到个股所有的数据 
                mid=mid[mid['END_DATE']==min_date]
                if not mid.empty:
                    fin_data=pd.concat([fin_data[fin_data['END_DATE']==min_date],mid])
                else:
                    pass
    
        #找到该只股票的四个维度的值以及行业排名
        num=fin_data.shape[0] #行业中的个股数
        if stkcode in list(fin_data['STK_CODE']):
            loc=list(fin_data['STK_CODE']).index(stkcode)
            if not isnan(fin_data.iloc[loc,4]):
                fin_data=fin_data.sort_values(by="ROEA",ascending=False)
                rank_earning=list(fin_data['STK_CODE']).index(stkcode)+1 #用净资产收益率来衡量盈利能力
                if rank_earning/num<=0.3:
                    descri_earning='较好'
                elif rank_earning/num>=0.7:
                    descri_earning='较差'
                else:
                    descri_earning='一般'
            else:
                descri_earning=''
            if not isnan(fin_data.iloc[loc,7]):
                fin_data=fin_data.sort_values(by="NP_YOY",ascending=False)
                rank_growth=list(fin_data['STK_CODE']).index(stkcode)+1
                if rank_growth/num<=0.3:
                    descri_growth='较好'
                elif rank_growth/num>=0.7:
                    descri_growth='较差'
                else:
                    descri_growth='一般'
            else:
                descri_growth=''
            if not isnan(fin_data.iloc[loc,8]):
                fin_data=fin_data.sort_values(by="TA_RATE",ascending=False)
                rank_capital=list(fin_data['STK_CODE']).index(stkcode)+1
                if rank_capital/num<=0.3:
                    descri_capital='较好'
                elif rank_capital/num>=0.7:
                    descri_capital='较差'
                else:
                    descri_capital='一般'
            else:
                 descri_capital=''
        else:
              descri_earning=''
              descri_growth=''
              descri_capital=''
        '''
        现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额) 单位是亿元
        '''         
        #strsql="select a.end_date，a.CS_10000，a.CS_20000，a.CS_30000 from upcenter.FIN_CASH_SHORT a，upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE and b.stk_code="+"'"+stkcode+"'"+" and a.end_date>=to_date('20110331'，'yyyy-mm-dd') order by a.end_date desc" 
    #        dfOra=dfOra[dfOra['END_DATE'].isin(date)]
        cash_flow=self._cash_flow[self._cash_flow['STK_CODE']==stkcode]#cash_flow用来画图
        if not isnan(list(cash_flow['CS_10000'])[0])and not isnan(list(cash_flow['CS_20000'])[0]) and not isnan(list(cash_flow['CS_30000'])[0]):
            if np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])>=1:
                descri_cash_flow='较好'
            elif -1<np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])<1:
                descri_cash_flow='一般'
            elif np.floor(list(cash_flow['CS_10000'])[0]+list(cash_flow['CS_20000'])[0]+list(cash_flow['CS_30000'])[0])<=-1:
                descri_cash_flow='较差'
        else:
            descri_cash_flow=''
        
        return   descri_earning,descri_growth,descri_capital,descri_cash_flow


    ''' 
    @function 给股票打分
    '''
    def Get_Stk_Star(self,stkcode):
        logging.info('----start'+str(stkcode)+'得分和星级\r\n')
        
        descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali=self.Get_Value_Rank(stkcode)
        if sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==4:
            score=np.floor((rank_inc_i+rank_beps+rank_ps_ocf+rank_bps)*100/(4*num))
        elif sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==3:
            mid=[rank_inc_i,rank_beps,rank_ps_ocf,rank_bps]
            mid.remove('--')
            score=np.floor(sum(mid)*100/(num*3))
        elif sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==2:
            mid=[rank_inc_i,rank_beps,rank_ps_ocf,rank_bps]
            mid.remove('--')
            mid.remove('--')
            score=np.floor(sum(mid)*100/(num*2))
        elif sum([rank_inc_i!='--',rank_beps!='--' ,rank_ps_ocf!='--' ,rank_bps!='--'])==1:
            mid=[rank_inc_i,rank_beps,rank_ps_ocf,rank_bps]
            mid.remove('--')
            mid.remove('--')
            mid.remove('--')
            score=np.floor(sum(mid)*100/num)
        else:
            score=0
    
        if 0<score<=20:
            star=1
        elif score>20 and score<=40:
            star=2
        elif score>40 and score<=60:
            star=3
        elif score>60 and score<=80:
            star=4
        elif score>80 and score<=100:
            star=5
        else:
            star=0
        return star,score


    '''
    @function 得出结论
    '''
    def Get_Conclusion(self,stkcode):
        
        logging.info('----start'+str(stkcode)+'得分和星级\r\n')
        descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali=self.Get_Value_Rank(stkcode)
        if descri_quali=='综合以上数据分析，公司质地优秀。':
            descri_1='公司质地优秀，在行业中处于领先水平。'
        elif descri_quali=='综合以上数据分析，公司质地良好。'or descri_quali=='综合以上数据分析，公司质地一般。' :
            descri_1='公司质地一般，在行业中处于中等水平。'
        elif descri_quali=='综合以上数据分析，公司质地较差。':
            descri_1='公司质地较差，在行业中处于落后水平。'
        else:
            descri_1=""
            
        descri_2=self.Insti_Rate(stkcode)
        
        if descri_1=="":
            descri=descri_2
        elif descri_2=="" and descri_1=="公司质地优秀，在行业中处于领先水平。":
            descri="公司质地优秀。"
	elif descri_2=="" and descri_1=="公司质地一般，在行业中处于中等水平。":
	    descri="公司质地一般。"
	elif descri_2=="" and descri_1=="公司质地较差，在行业中处于落后水平。":
	    descri="公司质地一般。"

        elif descri_1=="公司质地优秀，在行业中处于领先水平。" and descri_2=="多数机构认为该股长期投资价值较高，投资者可加强关注。":
            descri="公司目前质地优秀，并且多数机构认为该股长期投资价值较高，投资者可加强关注。"
        elif descri_1=="公司质地优秀，在行业中处于领先水平。" and descri_2=="多数机构认为该股长期具有投资价值，投资者可持续观望。":
            descri="公司目前质地优秀，并且多数机构认为该股长期具有投资价值，投资者可适当关注。"
        elif descri_1=="公司质地优秀，在行业中处于领先水平。" and descri_2=="多数机构认为该股长期投资价值不高，投资者可给予较少的关注。":
            descri="公司目前质地优秀，但是多数机构认为该股长期投资价值不高，投资者要注意长期投资风险。"
        elif descri_1=="公司质地一般，在行业中处于中等水平。"and descri_2=="多数机构认为该股长期投资价值较高，投资者可加强关注。":
            descri="公司目前质地一般，但是多数机构认为该股长期投资价值较高，投资者可加强关注。"
        elif descri_1=="公司质地一般，在行业中处于中等水平。" and descri_2=="多数机构认为该股长期具有投资价值，投资者可持续观望。":
            descri="公司目前质地一般，但是多数机构认为该股长期具有投资价值，投资者可适当关注。"
        elif descri_1=="公司质地一般，在行业中处于中等水平。" and descri_2=="多数机构认为该股长期投资价值不高，投资者可给予较少的关注。":
            descri="公司目前质地一般，但是多数机构认为该股长期投资价值不高，投资者要注意长期投资风险。"
        elif descri_1=="公司质地较差，在行业中处于落后水平。" and descri_2=="多数机构认为该股长期投资价值较高，投资者可加强关注。":
            descri="公司目前质地一般，但是多数机构认为该股长期投资价值较高，投资者可加强关注。"
        elif descri_1=="公司质地较差，在行业中处于落后水平。" and descri_2=="多数机构认为该股长期具有投资价值，投资者可持续观望。":
            descri="公司目前质地一般，但是多数机构认为该股具有投资价值，投资者可适当关注。"
        elif descri_1=="公司质地较差，在行业中处于落后水平。" and descri_2=="多数机构认为该股长期投资价值不高，投资者可给予较少的关注。":
            descri="公司目前质地一般，并且多数机构认为该股长期投资价值不高,投资者要注意长期投资风险。"
        
        return descri        

lastCheckDate = 0


#cur = time.strftime("%H:%M")
cur = datetime.datetime.now()
if lastCheckDate == cur.day:
	logging.info("Today is check, process is sleeping")
	time.sleep(60)


#if cur.hour < 21 or cur.minute < 0:
#	logging.info("Process is sleeping.")
#	time.sleep(60)


lastCheckDate = cur.day    
demo=Stock_Diagnosis_Data()
self=demo
self.Get_StkBaseInfo()
#判断数据有没有到齐 当数据对齐的时候 再开始计算              
#判断当前的日期 特定的日子再更新     
#today=datetime.date.today()
#demo=Stock_Diagnosis_Data()  
#self=demo
#self.Get_StkBaseInfo()  
#new_tradedate=self._NewTradeDate.date()  
#      
#if today==new_tradedate: 

#   logging.info('----start'+'更新今日的财务数据\r\n')

fin_sum_mac=[] #总结性机器语
com_manag_mac=[] #公司经营机器语
prf_abli_mac=[]# 盈利能力机器语
grw_abli_mac=[]#成长能力机器语
ass_quali_mac=[]#资产质量机器语
solv_marg_mac=[]#偿债能力机器语
cash_flow_mac=[]
org_rat_mac=[] #机构评级机器语
org_pre_mac=[] #机构预测机器语
mid_long_sug_mac=[]#中长线建议
score=[]#财务估值得分
star=[]#财务估值星级
current_time=[]
com_quali=[]
com_prf_mac=[]
com_grw_mac=[]
com_ass_mac=[]
com_cash_flow_mac=[]


def StkInfo():
	market = []
	stkpool = []
	stkpool_name=[]    
	i = 0
	sUrl = "http://hqdata.upchina.com/data/regStkDict?market=1&type=6"
	while i < retry:
		try:
			logging.info("request = " + sUrl)
			f=urlopen(sUrl,timeout=30)
			reponse=f.read()
			data=json.loads(reponse)['vStock']
			
			logging.info("获得行情中心股票代码和市场编号成功.")
			for strobj in data:
				stkpool.append(strobj['sCode'])
				stkpool_name.append(strobj['sName'])
				market.append(strobj['shtSetcode'])
			return market,stkpool
		except Exception, e:
			i += 1
			logging.warning("Exception:" + str(e)  + ", url = " + sUrl + ", retry = " + str(i + 1))
			continue
		logging.error("Retry " + str(i) + " times failed. url = " + sUrl)
		pass
#将stkpool转化为str
   
market,stkpool=StkInfo()    

stkpool=[str(x) for x in stkpool]
redu_stkpool=list(set(stkpool).difference(self._stkpool))
loc=[stkpool.index(x) for x in redu_stkpool ] #返回股票代码 在市场编号里面的位置
#loc1=stkpool.index(redu_stkpool[0])
#loc2=stkpool.index(redu_stkpool[1])

redu_result=pd.DataFrame(columns=['UPDATE_TIME','STK_CODE','MKT_NUM','TRD_DATE','FIN_SUM_MAC','PRF_ABLI_MAC','GRW_ABLI_MAC','ASS_QUALI_MAC','SOLV_MARG_MAC','CASH_FLOW_MAC','ORG_RAT_MAC'])
#redu_result=redu_stkpool
redu_result['STK_CODE']=redu_stkpool
redu_result['UPDATE_TIME']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
redu_result['TRD_DATE']=self._NewTradeDate
redu_result['MKT_NUM']=[market[x] for x in loc]
redu_result['FIN_SUM_MAC']=''
redu_result['COM_MANAG_MAC']=''
redu_result['PRF_ABLI_MAC']=''
redu_result['GRW_ABLI_MAC']=''
redu_result['ASS_QUALI_MAC']=''
redu_result['SOLV_MARG_MAC']=''
redu_result['CASH_FLOW_MAC']=''
redu_result['ORG_RAT_MAC']=''
#    redu_result['ORG_PRE_MAC']=''
#    redu_result['COM_PRF_MAC']=''
#    redu_result['COM_GRW_MAC']=''
#    redu_result['COM_ASS_MAC']=''
#    redu_result['COM_CASH_FLOW_MAC']=''
#    redu_result['MID_LONG_SUG_MAC']=''
#    redu_result['SCORE']=0
#    redu_result['STAR']=0

m=1
for i  in range(0,len(self._stkpool)):#
	print m 
	m=m+1
	print self._stkpool[i]
	logging.info('----start'+str(self._stkpool[i])+'财务数据所有维度\r\n')
	stk_fin_sum_mac=self.Get_Conclusion(self._stkpool[i]) #得到结语
	descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali=self.Get_Value_Rank(self._stkpool[i])
#    print descri,rank_inc_i,rank_beps,rank_ps_ocf,rank_bps,num,descri_quali
	stk_com_manag_mac=descri+descri_quali
	fin_sum_mac.append(stk_fin_sum_mac) #总结性机器语
	com_manag_mac.append(stk_com_manag_mac) #公司经营的机器语
	stk_prf_abli_mac,stk_grw_abli_mac,stk_ass_quali_mac,stk_solv_marg_mac,stk_cash_flow_mac=self.Company_Manage(self._stkpool[i]) #
#    print stk_prf_abli_mac,stk_grw_abli_mac,stk_ass_quali_mac,stk_solv_marg_mac,stk_cash_flow_mac
	prf_abli_mac.append(stk_prf_abli_mac)# 盈利能力机器语
	grw_abli_mac.append(stk_grw_abli_mac)#成长能力机器语
	ass_quali_mac.append(stk_ass_quali_mac)#资产质量机器语
	solv_marg_mac.append(stk_solv_marg_mac)#偿债能力机器语
	cash_flow_mac.append(stk_cash_flow_mac)
	stk_org_rat_mac=self.Insti_Rate(self._stkpool[i])
	org_rat_mac.append(stk_org_rat_mac)#机构评级机器语
#        stk_org_pre_mac,stk_inc_eps,stk_mid_long_sug_mac=self.Market_predict(self._stkpool[i])
#        org_pre_mac.append(stk_org_pre_mac) #机构预测机器语
#        mid_long_sug_mac.append(stk_mid_long_sug_mac)
#        stk_star,stk_score=self.Get_Stk_Star(self._stkpool[i]) #星级 #得分
#        star.append(stk_star)
#        score.append(stk_score)
#        stk_com_prf_mac,stk_com_grw_mac,stk_com_ass_mac,stk_com_cash_flow_mac=self.Get_Com_Quali(self._stkpool[i])
#        com_prf_mac.append( stk_com_prf_mac)
#        com_grw_mac.append(stk_com_grw_mac)
#        com_ass_mac.append(stk_com_ass_mac)
#        com_cash_flow_mac.append(stk_com_cash_flow_mac)
	stk_current_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
	current_time.append(stk_current_time)
	
   
result=pd.DataFrame([current_time,self._stkpool,self._mkt_num,[self._NewTradeDate]*self._stkpool_num,fin_sum_mac,com_manag_mac,prf_abli_mac,grw_abli_mac,ass_quali_mac,solv_marg_mac,cash_flow_mac,org_rat_mac]).T    
result.columns=['UPDATE_TIME','STK_CODE','MKT_NUM','TRD_DATE','FIN_SUM_MAC','COM_MANAG_MAC','PRF_ABLI_MAC','GRW_ABLI_MAC','ASS_QUALI_MAC','SOLV_MARG_MAC','CASH_FLOW_MAC','ORG_RAT_MAC']
result=pd.concat((result,redu_result))






                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
            
    






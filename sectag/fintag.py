# -*- coding: utf-8 -*-
from __future__ import division
import  cx_Oracle   as  ora
import pandas as pd
import datetime
import numpy as np

import os
import time

import MySQLdb
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from urllib2 import urlopen
from tagsettings import DATABASES as DATABASES

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

#根据股票财务得分获得评价标签
def tran2pingji(data):
    if data >=75 :
        return "优秀"
    if 75>data >=50 :
        return "良好" 
    if 50>data >=25 :
        return "一般"         
    if 25>data >0 :
        return "较差" 
    if  data == 0:
        return "数据欠缺"  

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


        # 现金流(经营活动现金净额、投资活动现金净额、筹资活动现金净额) 单位是亿元
        logging.info('----start下载现金流\r\n')
        strsql="select end_date,stk_code,CS_10000,CS_20000,CS_30000 FROM (select a.end_date,b.stk_code,Cast(a.CS_10000/power(10,8) as decimal(18,4)) as CS_10000 ,Cast(a.CS_20000/power(10,8) as decimal(18,4)) as CS_20000,Cast(a.CS_30000/power(10,8) as decimal(18,4)) as CS_30000,row_number() over (partition by b.stk_code order by a.end_date desc) as rk from upcenter.FIN_CASH_SHORT a,upcenter.STK_BASIC_INFO b where a.COM_UNI_CODE=b.COM_UNI_CODE  and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by a.end_date desc) WHERE RK=1"
        logging.info("下载现金流 sql = " + strsql)        
        self._cash_flow=getDataFromSql(strsql)
        self._cash_flow.columns=['END_DATE', 'STK_CODE', 'CS_10000', 'CS_20000','CS_30000']
        logging.info('----end下载现金流\r\n')


        #找到市场上所有股票盈利能力，成长能力，资产状况的指标 最新一期的
        logging.info('----start下载所有股票盈利能力,成长能力，资产状况的指标 最新一期的\r\n')
        strsql="select end_date,stk_code,SAL_NPR,SAL_GIR,ROEA,OR_YOY,OP_YOY,NP_YOY,TA_RATE,AP_RATE,BAL_P,BAL_O FROM (select a.end_date,b.stk_code,a.SAL_NPR,a.SAL_GIR,a.ROEA,a.OR_YOY,a.OP_YOY,a.NP_YOY,a.TA_RATE,a.AP_RATE,a.BAL_P,a.BAL_O,row_number() over (partition by b.stk_code order by a.end_date desc) AS RK from upcenter.FIN_IDX_ANA a,upcenter.STK_BASIC_INFO b where a.com_uni_code=b.com_uni_code  and extract (year from a.end_date) >=2011 and b.isvalid=1 and b.LIST_STA_PAR in ('1','4') and b.SEC_MAR_PAR in('1','2') and b.STK_TYPE_PAR=1 and b.LIST_DATE<trunc(sysdate) order by stk_code,a.end_date desc) WHERE RK=1"
        logging.info("下载所有股票盈利能力,成长能力，资产状况的指标 最新一期的 sql = " + strsql)        
        self._fin_value_egc_new=getDataFromSql(strsql)
        self._fin_value_egc_new.columns = ['END_DATE', 'STK_CODE', 'SAL_NPR', 'SAL_GIR', 'ROEA', 'OR_YOY', 'OP_YOY', 'NP_YOY', 'TA_RATE', 'AP_RATE', 'BAL_P', 'BAL_O']
        logging.info('----end下载所有股票盈利能力,成长能力,资产状况的指标 最新一期的\r\n')
        


    '''
    @function 给出净利润，每股收益，每股经营活动产生的额现金流量净额，每股净资产的值和排名
    '''    
    def Get_Fin_Rank(self,stkcode):
        logging.info('----start'+str(stkcode)+'净利润,每股收益，每股经营活动产生的额现金流量净额，每股净资产的值和排名\r\n')
        #得到个股所属的行业
        
        location=list(self._stk_indu['STK_CODE']).index(stkcode)
        indu_name=self._stk_indu.iloc[location,2] #得到个股所属行业

        
        #得到个股所在行业的成分股
        stk_indu_consi=self._stk_indu[self._stk_indu['SED_INDU_UNI_CODE']==indu_name]
        stk_code=list(stk_indu_consi['STK_CODE'])#成分股代码
        fin_data=self._fin_data[self._fin_data['STK_CODE'].isin(stk_code)]

                
        #找到该只股票的四个维度的值以及行业排名
        num=fin_data.shape[0] #行业中的个股数

        if stkcode in list(fin_data['STK_CODE']):
            location=list(fin_data['STK_CODE']).index(stkcode)
            stk_inc_i=fin_data.iloc[location,2]
            if not np.isnan(stk_inc_i):
                stk_inc_i=round(fin_data.iloc[location,2]/(10**8),4)#/(10**8) #单位为亿元 净利润
            else:
                stk_inc_i=0
                
            stk_beps=fin_data.iloc[location,3]
            if not np.isnan(stk_beps):
                stk_beps=round(fin_data.iloc[location,3],4) #每股收益
            else:
                stk_beps=0
                
            stk_ps_ocf=fin_data.iloc[location,4]
            if not np.isnan(stk_ps_ocf):
                stk_ps_ocf=round(fin_data.iloc[location,4],4) #每股经营活动产生的现金流量净额
            else:
                stk_ps_ocf=0
                
            stk_bps=fin_data.iloc[location,5]
            
            if not np.isnan(stk_bps):
                stk_bps=round(fin_data.iloc[location,5],4) #每股净资产
            else:
                stk_bps=0
            
            if stk_inc_i!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="INC_I",ascending=True)
                rank_inc_i=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_inc_i=0
                
            if stk_beps!=0:
                fin_data=fin_data.sort_values(by="BEPS",ascending=True)
                rank_beps=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_beps=0
            
            if stk_ps_ocf!=0:
                fin_data=fin_data.sort_values(by="PS_OCF",ascending=True)
                rank_ps_ocf=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_ps_ocf=0
            
            if stk_bps!=0:
                fin_data=fin_data.sort_values(by="BPS",ascending=True)
                rank_bps=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_bps=0

        else:
            logging.debug("无法获取财务数据的值和排名 stkcode = " + stkcode)

            rank_inc_i=0
            rank_beps=0
            rank_ps_ocf=0
            rank_bps=0

        return rank_inc_i,rank_beps,rank_ps_ocf,rank_bps
        
    
    def Get_Cash_Rank(self,stkcode):
        logging.info('----start'+str(stkcode)+'净利润,每股收益，每股经营活动产生的额现金流量净额，每股净资产的值和排名\r\n')
        #得到个股所属的行业
        
        location=list(self._stk_indu['STK_CODE']).index(stkcode)
        indu_name=self._stk_indu.iloc[location,2] #得到个股所属行业

        
        #得到个股所在行业的成分股
        stk_indu_consi=self._stk_indu[self._stk_indu['SED_INDU_UNI_CODE']==indu_name]
        stk_code=list(stk_indu_consi['STK_CODE'])#成分股代码
        fin_data=self._cash_flow[self._cash_flow['STK_CODE'].isin(stk_code)]

                
        #找到该只股票的四个维度的值以及行业排名
        num=fin_data.shape[0] #行业中的个股数

        if stkcode in list(fin_data['STK_CODE']):
            location=list(fin_data['STK_CODE']).index(stkcode)
            stk_cs_10000=fin_data.iloc[location,2]
            if not np.isnan(stk_cs_10000):
                stk_cs_10000=round(fin_data.iloc[location,2],4)
            else:
                stk_cs_10000=0
                
            stk_cs_20000=fin_data.iloc[location,3]
            if not np.isnan(stk_cs_20000):
                stk_cs_20000=round(fin_data.iloc[location,3],4) 
            else:
                stk_cs_20000=0
                
            stk_cs_30000=fin_data.iloc[location,4]
            if not np.isnan(stk_cs_30000):
                stk_cs_30000=round(fin_data.iloc[location,4],4) 
            else:
                stk_cs_30000=0
                

            
            if stk_cs_10000!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="CS_10000",ascending=True)
                rank_cs_10000=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_cs_10000=0
                
            if stk_cs_20000!=0:
                fin_data=fin_data.sort_values(by="CS_20000",ascending=True)
                rank_cs_20000=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_cs_20000=0
            
            if stk_cs_30000!=0:
                fin_data=fin_data.sort_values(by="CS_30000",ascending=True)
                rank_cs_30000=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_cs_30000=0
            

        else:
            logging.debug("无法获取财务数据的值和排名 stkcode = " + stkcode)

            rank_cs_10000=0
            rank_cs_20000=0
            rank_cs_30000=0

        return rank_cs_10000,rank_cs_20000,rank_cs_30000

    def Get_Egc_Rank(self,stkcode):
        logging.info('----start'+str(stkcode)+'净利润,每股收益，每股经营活动产生的额现金流量净额，每股净资产的值和排名\r\n')
        #得到个股所属的行业
        
        location=list(self._stk_indu['STK_CODE']).index(stkcode)
        indu_name=self._stk_indu.iloc[location,2] #得到个股所属行业

        
        #得到个股所在行业的成分股
        stk_indu_consi=self._stk_indu[self._stk_indu['SED_INDU_UNI_CODE']==indu_name]
        stk_code=list(stk_indu_consi['STK_CODE'])#成分股代码
        fin_data=self._fin_value_egc_new[self._fin_value_egc_new['STK_CODE'].isin(stk_code)]

                
        #找到该只股票的四个维度的值以及行业排名
        num=fin_data.shape[0] #行业中的个股数

        if stkcode in list(fin_data['STK_CODE']):
            location=list(fin_data['STK_CODE']).index(stkcode)
            stk_sal_npr=fin_data.iloc[location,2]
            if not np.isnan(stk_sal_npr):
                stk_sal_npr=round(fin_data.iloc[location,2],4)
            else:
                stk_sal_npr=0
                
            stk_sal_gir=fin_data.iloc[location,3]
            if not np.isnan(stk_sal_gir):
                stk_sal_gir=round(fin_data.iloc[location,3],4) 
            else:
                stk_sal_gir=0
                
            stk_roea=fin_data.iloc[location,4]
            if not np.isnan(stk_roea):
                stk_roea=round(fin_data.iloc[location,4],4) 
            else:
                stk_roea=0

            stk_or_yoy=fin_data.iloc[location,5]
            if not np.isnan(stk_or_yoy):
                stk_or_yoy=round(fin_data.iloc[location,4],4) 
            else:
                stk_or_yoy=0

            stk_op_yoy=fin_data.iloc[location,6]
            if not np.isnan(stk_op_yoy):
                stk_op_yoy=round(fin_data.iloc[location,4],4) 
            else:
                stk_op_yoy=0

            stk_np_yoy=fin_data.iloc[location,7]
            if not np.isnan(stk_np_yoy):
                stk_np_yoy=round(fin_data.iloc[location,4],4) 
            else:
                stk_np_yoy=0                

            stk_ta_rate=fin_data.iloc[location,8]
            if not np.isnan(stk_ta_rate):
                stk_ta_rate=round(fin_data.iloc[location,4],4) 
            else:
                stk_ta_rate=0   

            stk_ap_rate=fin_data.iloc[location,9]
            if not np.isnan(stk_ap_rate):
                stk_ap_rate=round(fin_data.iloc[location,4],4) 
            else:
                stk_ap_rate=0                   

            stk_bal_p=fin_data.iloc[location,10]

            if not np.isnan(stk_bal_p):
                stk_bal_p=round(fin_data.iloc[location,4],4) 
            else:
                stk_bal_p=0    

            stk_bal_o=fin_data.iloc[location,11]
            if not np.isnan(stk_bal_o):
                stk_bal_o=round(fin_data.iloc[location,4],4) 
            else:
                stk_bal_o=0                    


            if stk_sal_npr!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="SAL_NPR",ascending=True)
                rank_sal_npr=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_sal_npr=0
                
            if stk_sal_gir!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="SAL_GIR",ascending=True)
                rank_sal_gir=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_sal_gir=0

            if stk_roea!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="ROEA",ascending=True)
                rank_stk_roea=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_stk_roea=0
                
            if stk_or_yoy!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="OR_YOY",ascending=True)
                rank_or_yoy=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_or_yoy=0

            if stk_op_yoy!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="OP_YOY",ascending=True)
                rank_op_yoy=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_op_yoy=0


            if stk_np_yoy!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="NP_YOY",ascending=True)
                rank_np_yoy=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_np_yoy=0

            if stk_ta_rate!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="TA_RATE",ascending=True)
                rank_ta_rate=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_ta_rate=0

            if stk_ap_rate!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="AP_RATE",ascending=True)
                rank_ap_rate=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_ap_rate=0

            if stk_bal_p!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="BAL_P",ascending=True)
                rank_bal_p=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_bal_p=0

            if stk_bal_o!=0:
                #找到四个维度 该只股票在行业中的排名
                fin_data=fin_data.sort_values(by="BAL_O",ascending=True)
                rank_bal_o=(list(fin_data['STK_CODE']).index(stkcode)+1)/num*100
            else:
                rank_bal_o=0
                
        else:
            logging.debug("无法获取财务数据的值和排名 stkcode = " + stkcode)
            rank_sal_npr=0
            rank_sal_gir = 0
            rank_stk_roea = 0
            rank_or_yoy=0
            rank_op_yoy=0
            rank_np_yoy=0
            rank_ta_rate=0
            rank_ap_rate=0
            rank_bal_p=0
            rank_bal_o=0


        return rank_sal_npr,rank_sal_gir,rank_stk_roea,rank_or_yoy,rank_op_yoy,rank_np_yoy,rank_ta_rate,rank_ap_rate,rank_bal_p,rank_bal_o
        
  

#dfCwData = pd.DataFrame(columns =['STK_CODE','rank_inc_i','rank_beps','rank_ps_ocf','rank_bps',\
#'rank_cs_10000','rank_cs_20000','rank_cs_30000','rank_sal_npr','rank_sal_gir','rank_stk_roea',\
#'rank_or_yoy','rank_op_yoy','rank_np_yoy','rank_ta_rate','rank_ap_rate','rank_bal_p','rank_bal_o'])
def getTagData():
    dfFinTag = pd.DataFrame(columns =['STK_CODE','TRD_DATE','TAG_VALUE','TAG_CODE'])
#    TRD_DATE = "20171222"
    idx = 0
    for i  in range(len(self._stkpool)):#
        stkcode = self._stkpool[i]
        STK_CODE = stkcode
        print i
        rank_inc_i,rank_beps,rank_ps_ocf,rank_bps = self.Get_Fin_Rank(stkcode)
        rank_cs_10000,rank_cs_20000,rank_cs_30000 = self.Get_Cash_Rank(stkcode)
        rank_sal_npr,rank_sal_gir,rank_stk_roea,rank_or_yoy,rank_op_yoy,rank_np_yoy,rank_ta_rate,rank_ap_rate,rank_bal_p,rank_bal_o = self.Get_Egc_Rank(stkcode)
    
        TAG_CODE = '10040101'
        TAG_VALUE = tran2pingji(rank_inc_i)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
     
        TAG_CODE = '10040102'
        TAG_VALUE = tran2pingji(rank_beps)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
    
    
        TAG_CODE = '10040103'
        TAG_VALUE = tran2pingji(rank_ps_ocf)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
     
        TAG_CODE = '10040104'
        TAG_VALUE = "每股净资产"+tran2pingji(rank_bps)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
        
        TAG_CODE = '10040201'
        TAG_VALUE = tran2pingji(rank_cs_10000)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
    
    
        TAG_CODE = '10040202'
        TAG_VALUE = tran2pingji(rank_cs_20000)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
     
        TAG_CODE = '10040203'
        TAG_VALUE = "筹资现金流"+tran2pingji(rank_cs_30000)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1    
    
    
        TAG_CODE = '10040301'
        TAG_VALUE = tran2pingji(rank_sal_npr)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
    
    
        TAG_CODE = '10040302'
        TAG_VALUE = tran2pingji(rank_sal_gir)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
     
        TAG_CODE = '10040303'
        TAG_VALUE = tran2pingji(rank_stk_roea)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1    
     
        TAG_CODE = '10040304'
        TAG_VALUE = tran2pingji(rank_or_yoy)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
    
    
        TAG_CODE = '10040305'
        TAG_VALUE = tran2pingji(rank_op_yoy)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
     
        TAG_CODE = '10040306'
        TAG_VALUE = tran2pingji(rank_np_yoy)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1       
        
        
        TAG_CODE = '10040307'
        TAG_VALUE = "总资产周转率"+tran2pingji(rank_ta_rate)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
    
    
        TAG_CODE = '10040308'
        TAG_VALUE = tran2pingji(rank_ap_rate)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1
     
        TAG_CODE = '10040309'
        TAG_VALUE = tran2pingji(rank_bal_p)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1       
    
        TAG_CODE = '10040310'
        TAG_VALUE = tran2pingji(rank_bal_o)
        dfFinTag.loc[idx,'STK_CODE'] = STK_CODE
        dfFinTag.loc[idx,'TRD_DATE'] = TRD_DATE
        dfFinTag.loc[idx,'TAG_VALUE'] = TAG_VALUE
        dfFinTag.loc[idx,'TAG_CODE'] = TAG_CODE
        idx = idx +1           
    
    #    dfCwData.loc[i,'STK_CODE'] = stkcode
    #    dfCwData.loc[i,'rank_inc_i'] = int(rank_inc_i)
    #    dfCwData.loc[i,'rank_beps'] = int(rank_beps)
    #    dfCwData.loc[i,'rank_ps_ocf'] = int(rank_ps_ocf)
    #    dfCwData.loc[i,'rank_bps'] = int(rank_bps)
    #    dfCwData.loc[i,'rank_cs_10000'] = int(rank_cs_10000)
    #    dfCwData.loc[i,'rank_cs_20000'] = int(rank_cs_20000)
    #    dfCwData.loc[i,'rank_cs_30000'] = int(rank_cs_30000)
    #    dfCwData.loc[i,'rank_sal_npr'] = int(rank_sal_npr)
    #    dfCwData.loc[i,'rank_sal_gir'] = int(rank_sal_gir)
    #    dfCwData.loc[i,'rank_stk_roea'] = int(rank_stk_roea)
    #    dfCwData.loc[i,'rank_or_yoy'] = int(rank_or_yoy)
    #    dfCwData.loc[i,'rank_op_yoy'] = int(rank_op_yoy)
    #    dfCwData.loc[i,'rank_np_yoy'] = int(rank_np_yoy)
    #    dfCwData.loc[i,'rank_ta_rate'] = int(rank_ta_rate)
    #    dfCwData.loc[i,'rank_ap_rate'] = int(rank_ap_rate)
    #    dfCwData.loc[i,'rank_bal_p'] = int(rank_bal_p)
    #    dfCwData.loc[i,'rank_bal_o'] = int(rank_bal_o)
    return dfFinTag

def insertdata():
    LabConnect,curLab = startconnect("mysql_test")
    for i in range(len(dfFinTag)):
        STK_CODE = dfFinTag.loc[i,'STK_CODE']
        TAG_VALUE = dfFinTag.loc[i,'TAG_VALUE']
        TAG_CODE = dfFinTag.loc[i,'TAG_CODE']
        insert2db(LabConnect,curLab ,"STK_TAG_FIN",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
        
#
#curtime = time.localtime(time.time())
#demo=Stock_Diagnosis_Data()
#self=demo
#self.Get_StkBaseInfo()
#isTrade ,TRD_DATE, PreTrdDate = istrade(curtime)
#dfFinTag = getTagData()
#insertdata()

if   __name__ == '__main__':
    lastCheckDay = 0
    logDate = 0
    isdwn = False
    while True:        
        curtime = time.localtime(time.time())
        if (logDate != curtime.tm_mday):#新建当日的日志文件
            logDate = curtime.tm_mday
            iscau = False
            
        if (21*60+46 < time.localtime(time.time()).tm_hour*60+time.localtime(time.time()).tm_min < 23*60 + 45) & (not iscau):
            isTrade ,TRD_DATE, PreTrdDate = istrade(curtime) 
            if isTrade : 
                demo=Stock_Diagnosis_Data()
                self=demo
                self.Get_StkBaseInfo()
                isTrade ,TRD_DATE, PreTrdDate = istrade(curtime)
                dfFinTag = getTagData()
                insertdata()
                iscau = True

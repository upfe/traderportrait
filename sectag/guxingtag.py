# -*- coding: utf-8 -*-
"""
Created on Thu Dec 28 16:27:24 2017

@author: barbuwu
"""
import pandas as pd
import numpy as np
import MySQLdb
import time

import pandas as pd
import numpy as np
import json
from urllib2 import urlopen

from tagsettings import TICAI_CLASS as TICAI_CLASS
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

LabConnect,curLab = startconnect("mysql_test")
TRD_DATE = "20171228"
dframe = pd.read_excel("fund_para.xls")
TAG_CODE= "10010500"
for idx in range(len(dframe)):
    lanchou = dframe.loc[idx,"lanchou"]
    if not np.isnan(lanchou):
        STK_CODE = "0"*(6 -len(str(int(lanchou)))) + str(int(lanchou))
        TAG_VALUE = "蓝筹股"
        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
    baima = dframe.loc[idx,"baima"]
    if not np.isnan(baima):
        STK_CODE = "0"*(6 -len(str(int(baima)))) + str(int(baima))
        
        
        TAG_VALUE = "白马股"
        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)

    chengzhang = dframe.loc[idx,"chengzhang"]
    if not np.isnan(chengzhang):

        STK_CODE = "0"*(6 -len(str(int(chengzhang)))) + str(int(chengzhang))
        print STK_CODE

        TAG_VALUE = "成长股"
        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
        
        
        
        
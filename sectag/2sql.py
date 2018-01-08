# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 11:28:39 2017

@author: barbuwu
"""

import pickle
import pandas as pd

import MySQLdb
import time
from tagsettings import TAG_FILED as TAG_FILED
from tagsettings import TAG_CLASS as TAG_CLASS
from tagsettings import DATABASES as DATABASES


def startconnect(databsename):
    LabConnect=MySQLdb.connect(host = DATABASES.get(databsename).get("host"),\
                               user = DATABASES.get(databsename).get("user"),\
                               passwd = DATABASES.get(databsename).get("passwd"),\
                               db = DATABASES.get(databsename).get("db") ,\
                               charset="utf8")
    
    curLab=LabConnect.cursor() 
    return LabConnect,curLab

LabConnect,curLab = startconnect("mysql_test") 

def insert2db(LabConnect,curLab ,tablename,TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE):
    insert_table_sqllab="REPLACE INTO " + str(tablename) + "\
    (UPDATE_TIME,TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE) \
    values(DATE_FORMAT('%s','%%Y-%%m-%%d %%H:%%i:%%s'),'%s', '%s', '%s', '%s') "\
    %(pd.Timestamp(time.strftime('%Y-%m-%d  %H:%M:%S',time.localtime(time.time()))),TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
    
    curLab.execute(insert_table_sqllab)
    LabConnect.commit()
    
with open('biaoqian_sum1121.pickle','rb+') as f:  
#    biaoqian1=pickle.load(f)
#    biaoqian=biaoqian1[0]
    biaoqian1=pickle.load(f)

biaoqian=biaoqian1[0]   
TRD_DATE = "20171219"
tag_class = TAG_CLASS.keys()
dfStkTag = pd.DataFrame(columns =['TRD_DATE','STK_CODE', 'TAG_VALUE', 'TAG_CODE'])

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
#                        dfStkTag.loc[jdx,'TRD_DATE'] = TRD_DATE
#                        dfStkTag.loc[jdx,'STK_CODE'] = STK_CODE
#                        dfStkTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
#                        dfStkTag.loc[jdx,'TAG_CODE'] = TAG_CODE
#                        jdx = jdx +1

#                        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
                    else:
                        pass
                else:
                    TAG_VALUE = everydata[filed_item]

#                    dfStkTag.loc[jdx,'TRD_DATE'] = TRD_DATE
#                    dfStkTag.loc[jdx,'STK_CODE'] = STK_CODE
#                    dfStkTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
#                    dfStkTag.loc[jdx,'TAG_CODE'] = TAG_CODE
#                    jdx = jdx +1

#                    insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)
            else:
                for TAG_VALUE in everydata[filed_item] :
                    if TAG_VALUE != "0":
                        pass

#                        dfStkTag.loc[jdx,'TRD_DATE'] = TRD_DATE
#                        dfStkTag.loc[jdx,'STK_CODE'] = STK_CODE
#                        dfStkTag.loc[jdx,'TAG_VALUE'] = TAG_VALUE
#                        dfStkTag.loc[jdx,'TAG_CODE'] = TAG_CODE
#                        jdx = jdx +1

#                        insert2db(LabConnect,curLab ,"STK_TAG",TRD_DATE,STK_CODE, TAG_VALUE, TAG_CODE)

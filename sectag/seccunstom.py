# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 11:14:21 2017

@author: 96003
"""
from __future__ import division
import cx_Oracle
import pickle
import pandas as pd
import numpy as np

def jiaogedan():
    db = cx_Oracle.connect('HS_HIS', 'dgv12Hc24vbd', '172.16.8.200:1521/upapp')
    sql="select CLIENT_ID,init_date,entrust_bs,stock_code,business_price,business_amount,business_balance \
        from HS_HIS.HIS_DELIVER a \
        WHERE (a.exchange_type=1 or a.exchange_type=2) and a.stock_type='0' "
    cursor = db.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    rzrq = pd.DataFrame(data)
    rzrq.columns =  'USERCODE', 'CURRDATE','TRDID','SECUCODE','MATCHEDPRICE','MATCHEDQTY','MATCHEDAMT'
    return rzrq
    

data=jiaogedan()




#data=pd.read_csv('a8.csv')
#cde=data['SECUCODE']
##zd=[]
##for i in cde:
##    if len(str(i))<6:
##        zjj=str(i*0.000001)[2:8]
##    else:
##        zjj=str(i)
##    zd.append(zjj)
#data['SECUCODE']=zd
user=pd.unique(data['USERCODE'])
hz=[]
for ii in user:
    data1=data[data['USERCODE'].isin([ii])]
    dt=pd.unique(data1['CURRDATE'])
    for jj in dt:
        data2=data1[data1['CURRDATE'].isin([jj])]
        z1=data2[data2['TRDID'].isin(['1'])]
        z2=data2[data2['TRDID'].isin(['2'])]
        if len(z1)>0:
            ucode=pd.unique(z1['SECUCODE'])
            shuju=[]
            for kk in ucode:
                data3=z1[z1['SECUCODE'].isin([kk])]
                shuju.append([ii,jj,1,kk,np.average(data3['MATCHEDPRICE']),np.sum(data3['MATCHEDQTY']),np.sum(data3['MATCHEDAMT'])])
            hz.append(shuju)
#            list(set(hz).union(set(shuju)))
        if len(z2)>0:
            ucode=pd.unique(z2['SECUCODE'])
            shuju=[]
            for kk in ucode:
                data3=z2[z2['SECUCODE'].isin([kk])]
                shuju.append([ii,jj,2,kk,np.average(data3['MATCHEDPRICE']),np.sum(data3['MATCHEDQTY']),np.sum(data3['MATCHEDAMT'])])
            hz.append(shuju)
#            list(set(hz).union(set(shuju)))

zg=[]
for ii in hz:
   zg.append(ii[0])               
dd=pd.DataFrame(data=zg,columns=['USERCODE','CURRDATE','TRDID','SECUCODE','MATCHEDPRICE','MATCHEDQTY','MATCHEDAMT'])
output = open('trader_sum.pkl', 'wb')
pickle.dump(dd, output)
output.close()
dd.to_csv('kehuhuizong.csv')            
#with open('D:/96003/360/a8.xls','rb') as f:
#    reader = csv.reader(f)
#cxcy=[]
#for ii in reader:
#    print ii
#csv_reader=csv.reader(open('D:\96003\360\a7.xls','wb'))
#pd.read_csv()
#n=0
#for ii in csv_reader:
##    if n==0:
##        n=1
##    else:
#       cxcy.append(ii[1])
pkl_file = open('trader_sum.pkl', 'rb')
trader_sum=pickle.load(pkl_file)
pkl_file.close()

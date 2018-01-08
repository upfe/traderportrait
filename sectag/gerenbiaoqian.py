# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 09:51:15 2017
标签贴到个人身上。分为全标签类，以及按照统计频率or固定样式选择前3等。
数据库数据是覆盖形式的，所以标签是个从现在积累开始的概念。
@author: 96003
"""
import pickle
import pandas as pd
import numpy as np

with open('biaoqian_sum1121.pickle','rb+') as f:  
#    biaoqian1=pickle.load(f)
#    biaoqian=biaoqian1[0]
    biaoqian1=pickle.load(f)
with open('trader_sum.pkl','rb+') as f:  
    trader=pickle.load(f)     
biaoqian=biaoqian1[0]    
user=list(pd.unique(trader['USERCODE']))
# 客户汇总表只做
hz=[]
bq=pd.DataFrame(data=biaoqian.values,index=biaoqian.stk,columns=['stk', 'hszb', 'szzb', 'zxb', 'cyb', 'rzrq', 'st', 'jyg','czg', 'bmg', 'hy', 'hyid', 'dy', 'dyid', 'tc', 'tcid', 'sz','xd', 'zd', 'lz', 'lj','gpzt','gpsz','gpxd','gpdt','gpcp', 'wl', 'cjdm', 'cjwh', 'cjhy', 'dl','fl', 'jl', 'tl', 'lr', 'lc', 'jigou', 'jigouid', 'renming','pj', 'pjchange', 'gg', 'ggid'])
hszb=[]
szzb=[]
zxb=[] 
cyb=[]
rzrq=[]
st=[]
jyg=[]
czg=[]
bmg=[]

sz=[]
xd=[]
zd=[]
lz=[]
lj=[]
wl=[]
cjdm=[]
cjwh=[]
cjhy=[]
dl=[]
fl=[]
jl=[]
tl=[]
lr=[]
lc=[]

gpsz=[]
gpzt=[]
gpxd=[]
gpdt=[]
gpcp=[]

pj=[0 for i in xrange(len(user))]
pjchange=[0 for i in xrange(len(user))]
hyidd=[0 for i in xrange(len(user))]
dyidd=[0 for i in xrange(len(user))]
tcidd=[0 for i in xrange(len(user))]
jgidd=[0 for i in xrange(len(user))]
ggidd=[0 for i in xrange(len(user))]
rmidd=[0 for i in xrange(len(user))]
for ii in user:
    data1=trader[trader['USERCODE'].isin([ii])]
    z1=data1[data1['TRDID'].isin([1])]
    stock=pd.unique(z1['SECUCODE'])
    if len(stock)>1:
     data2=bq.loc[stock,:]
#     hz.append([int(np.sum(data2.hszb)),int(np.sum(data2.szzb)),int(np.sum(data2.zxb)),int(np.sum(data2.cyb)),int(np.sum(data2.rzrq)),int(np.sum(data2.st)),int(np.sum(data2.jyg)),int(np.sum(data2.czg)),int(np.sum(data2.bmg)),int(np.sum(data2.sz)),int(np.sum(data2.xd)),int(np.sum(data2.zd)),int(np.sum(data2.lz)),int(np.sum(data2.lj)),int(np.sum(data2.wl)),int(np.sum(data2.cjdm)),int(np.sum(data2.cjwh)),int(np.sum(data2.cjhy)),int(np.sum(data2.dl)),int(np.sum(data2.fl)),int(np.sum(data2.jl)),int(np.sum(data2.tl)),int(np.sum(data2.lr)),int(np.sum(data2.lc)),int(np.nanmean(data2.pj)),int(np.nanmean(data2.pjchange))])
#     hz.append([np.sum(data2.hszb),np.sum(data2.szzb),np.sum(data2.zxb),np.sum(data2.cyb),np.sum(data2.rzrq),np.sum(data2.st),np.sum(data2.jyg),np.sum(data2.czg),np.sum(data2.bmg),np.sum(data2.sz),np.sum(data2.xd),np.sum(data2.zd),np.sum(data2.lz),np.sum(data2.lj),np.sum(data2.wl),np.sum(data2.cjdm),np.sum(data2.cjwh),np.sum(data2.cjhy),np.sum(data2.dl),np.sum(data2.fl),np.sum(data2.jl),np.sum(data2.tl),np.sum(data2.lr),np.sum(data2.lc),np.nanmean(data2.pj),np.nanmean(data2.pjchange)])

     hszb.append(int(np.sum(data2.hszb)))
     szzb.append(int(np.sum(data2.szzb)))
     zxb.append(int(np.sum(data2.zxb)))
     cyb.append(int(np.sum(data2.cyb)))
     rzrq.append(int(np.sum(data2.rzrq)))
     st.append(int(np.sum(data2.st)))
     jyg.append(int(np.sum(data2.jyg)))
     czg.append(int(np.sum(data2.czg)))
     bmg.append(int(np.sum(data2.bmg)))

     sz.append(int(np.sum(data2.sz)))
     xd.append(int(np.sum(data2.xd)))
     zd.append(int(np.sum(data2.zd)))
     lz.append(int(np.sum(data2.lz)))
     lj.append(int(np.sum(data2.lj)))
     wl.append(int(np.sum(data2.wl)))
     cjdm.append(int(np.sum(data2.cjdm)))
     cjwh.append(int(np.sum(data2.cjwh)))
     cjhy.append(int(np.sum(data2.cjhy)))
     dl.append(int(np.sum(data2.dl)))
     fl.append(int(np.sum(data2.fl)))
     jl.append(int(np.sum(data2.jl)))
     tl.append(int(np.sum(data2.tl)))
     lr.append(int(np.sum(data2.lr)))
     lc.append(int(np.sum(data2.lc)))
     
     gpzt.append(int(np.sum(data2.gpzt)))
     gpsz.append(int(np.sum(data2.gpsz)))
     gpxd.append(int(np.sum(data2.gpxd)))
     gpdt.append(int(np.sum(data2.gpdt)))
     gpcp.append(int(np.sum(data2.gpcp)))




     pj[user.index(ii)]=np.mean(data2.pj)  
     pjchange[user.index(ii)]=np.mean(data2.pjchange)
     cdd=data2.hyid.values
     zj=cdd[0]
     for idk in range(1,len(cdd)):
#         if np.isnan(cdd[idk]).values:
#            zj=zj+[0]
#         else:
         try:
             zj=np.concatenate((zj,cdd[idk]))
         except:
             zj=cdd[idk]
     hyidd[user.index(ii)]=zj
     
     cdd=data2.dyid.values
     zj=cdd[0]
     for idk in range(1,len(cdd)):
         try:
             zj=np.concatenate((zj,cdd[idk]))
         except:
             zj=cdd[idk]
     dyidd[user.index(ii)]=zj

     cdd=data2.tcid.values
     zj=cdd[0]
     for idk in range(1,len(cdd)):
         try:
             zj=np.concatenate((zj,cdd[idk]))
         except:
             zj=cdd[idk]
     tcidd[user.index(ii)]=zj

     
     cdd=data2.jigouid.values
     zj=cdd[0]
     for idk in range(1,len(cdd)):
         try:
             zj=np.concatenate((zj,cdd[idk]))
         except:
             zj=cdd[idk]
     jgidd[user.index(ii)]=zj

     cdd=data2.ggid.values
     zj=cdd[0]
     for idk in range(1,len(cdd)):
         try:
             zj=np.concatenate((zj,cdd[idk]))
         except:
             zj=cdd[idk]
     ggidd[user.index(ii)]=zj
     
     cdd=data2.renming.values
     zj=cdd[0]
     for idk in range(1,len(cdd)):
         try:
             zj=np.concatenate((zj,cdd[idk]))
         except:
             zj=cdd[idk]
     rmidd[user.index(ii)]=zj
     
     
    else:
#     hz.append([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0])
     hszb.append(0)
     szzb.append(0)
     zxb.append(0)
     cyb.append(0)
     rzrq.append(0)
     st.append(0)
     jyg.append(0)
     czg.append(0)
     bmg.append(0)

     sz.append(0)
     xd.append(0)
     zd.append(0)
     lz.append(0)
     lj.append(0)
     wl.append(0)
     cjdm.append(0)
     cjwh.append(0)
     cjhy.append(0)
     dl.append(0)
     fl.append(0)
     jl.append(0)
     tl.append(0)
     lr.append(0)
     lc.append(0)

     gpzt.append(0)
     gpsz.append(0)
     gpxd.append(0)
     gpdt.append(0)
     gpcp.append(0)




     pj[user.index(ii)]=0
     pjchange[user.index(ii)]=0   
     hyidd[user.index(ii)]=0
     dyidd[user.index(ii)]=0
     tcidd[user.index(ii)]=0
     jgidd[user.index(ii)]=0
     ggidd[user.index(ii)]=0
     rmidd[user.index(ii)]=0
us=pd.DataFrame(data=user,columns=['user'])        
us['hszb']=hszb
us['szzb']=szzb
us['zxb']=zxb
us['cyb']=cyb
us['rzrq']=rzrq
us['st']=st
us['jyg']=jyg
us['czg']=czg
us['bmg']=bmg

us['sz']=sz
us['xd']=xd
us['zd']=zd
us['lz']=lz
us['lj']=lj
us['wl']=wl
us['cjdm']=cjdm
us['cjwh']=cjwh
us['cjhy']=cjhy
us['dl']=dl
us['fl']=fl
us['jl']=jl
us['tl']=tl
us['lr']=lr
us['lc']=lc

us['gpzt']=gpzt
us['gpsz']=gpsz
us['gpdt']=gpdt
us['gpxd']=gpxd
us['gpcp']=gpcp



us['pj']=pj
us['pjchange']=pjchange        
us['hyid']=hyidd
us['dyid']=dyidd
us['tcid']=tcidd
us['jgid']=jgidd    
us['ggid']=ggidd
us['rmid']=rmidd 




## ======================
hszbv=[]
szzbv=[]
zxbv=[] 
cybv=[]
rzrqv=[]
stv=[]
jygv=[]
czgv=[]
bmgv=[]

szv=[]
xdv=[]
zdv=[]
lzv=[]
ljv=[]
wlv=[]
cjdmv=[]
cjwhv=[]
cjhyv=[]
dlv=[]
flv=[]
jlv=[]
tlv=[]
lrv=[]
lcv=[]

gpztv=[]
gpszv=[]
gpdtv=[]
gpxdv=[]
gpcpv=[]



pjv=[]
pjchangev=[]
hyidv=[]
dyidv=[]
tcidv=[]
jgidv=[]
ggidv=[]

for i in range(len(us)):
    if us.iloc[i]['hszb']>np.mean(us.hszb):
        hszbv.append(1)
    else:
        hszbv.append(0)
    if us.iloc[i]['szzb']>np.mean(us.szzb):
        szzbv.append(1)
    else:
        szzbv.append(0)
    if us.iloc[i]['zxb']>np.mean(us.zxb):
        zxbv.append(1)
    else:
        zxbv.append(0)
    if us.iloc[i]['cyb']>np.mean(us.cyb):
        cybv.append(1)
    else:
        cybv.append(0)
    if us.iloc[i]['rzrq']>np.mean(us.rzrq):
        rzrqv.append(1)
    else:
        rzrqv.append(0)    
    if us.iloc[i]['st']>np.mean(us.st):
        stv.append(1)
    else:
        stv.append(0)    
    if us.iloc[i]['jyg']>np.mean(us.jyg):
        jygv.append(1)
    else:
        jygv.append(0)    
    if us.iloc[i]['czg']>np.mean(us.czg):
        czgv.append(1)
    else:
        czgv.append(0) 
    if us.iloc[i]['bmg']>np.mean(us.bmg):
        bmgv.append(1)
    else:
        bmgv.append(0) 
    if us.iloc[i]['sz']>np.mean(us.sz):
        szv.append(1)
    else:
        szv.append(0)    
    if us.iloc[i]['xd']>np.mean(us.xd):
        xdv.append(1)
    else:
        xdv.append(0) 
    if us.iloc[i]['zd']>np.mean(us.zd):
        zdv.append(1)
    else:
        zdv.append(0) 
    if us.iloc[i]['lz']>np.mean(us.lz):
        lzv.append(1)
    else:
        lzv.append(0)         
    if us.iloc[i]['lj']>np.mean(us.lj):
        ljv.append(1)
    else:
        ljv.append(0) 
    if us.iloc[i]['wl']>np.mean(us.wl):
        wlv.append(1)
    else:
        wlv.append(0) 
    if us.iloc[i]['cjdm']>np.mean(us.cjdm):
        cjdmv.append(1)
    else:
        cjdmv.append(0) 
    if us.iloc[i]['cjwh']>np.mean(us.cjwh):
        cjwhv.append(1)
    else:
        cjwhv.append(0)     
    if us.iloc[i]['cjhy']>np.mean(us.cjhy):
        cjhyv.append(1)
    else:
        cjhyv.append(0)
    if us.iloc[i]['dl']>np.mean(us.dl):
        dlv.append(1)
    else:
        dlv.append(0) 
    if us.iloc[i]['fl']>np.mean(us.fl):
        flv.append(1)
    else:
        flv.append(0) 
    if us.iloc[i]['jl']>np.mean(us.jl):
        jlv.append(1)
    else:
        jlv.append(0) 
    if us.iloc[i]['tl']>np.mean(us.tl):
        tlv.append(1)
    else:
        tlv.append(0)    
    if us.iloc[i]['lr']>np.mean(us.lr):
        lrv.append(1)
    else:
        lrv.append(0) 
    if us.iloc[i]['lc']>np.mean(us.lc):
        lcv.append(1)
    else:
        lcv.append(0)  
    if us.iloc[i]['pj']>np.mean(us.pj):
        pjv.append(1)
    else:
        pjv.append(0)  
    if us.iloc[i]['pjchange']>np.mean(us.pjchange):
        pjchangev.append(1)
    else:
        pjchangev.append(0)  
    if us.iloc[i]['gpzt']>np.mean(us.gpzt):
        gpztv.append(1)
    else:
        gpztv.append(0)
    if us.iloc[i]['gpsz']>np.mean(us.gpsz):
        gpszv.append(1)
    else:
        gpszv.append(0)
    if us.iloc[i]['gpdt']>np.mean(us.gpdt):
        gpdtv.append(1)
    else:
        gpdtv.append(0)
    if us.iloc[i]['gpxd']>np.mean(us.gpxd):
        gpxdv.append(1)
    else:
        gpxdv.append(0)
    if us.iloc[i]['gpcp']>np.mean(us.gpcp):
        gpcpv.append(1)
    else:
        gpcpv.append(0)
        
        
        
        
        
    if type(us.iloc[i]['hyid'])<>np.ndarray:
        xz=0
    else:
        zj=pd.unique(us.iloc[i]['hyid'])
        zj.tolist()
        if len(zj)>0:
         shumu=[]
         for ii in zj:
            shumu.append(np.count_nonzero(ii))
         if max(shumu)>1:
            xz=zj[shumu.index(max(shumu))]
         else:
            xz=zj[0]
        else:
            xz=0
    hyidv.append(xz)
            
    if type(us.iloc[i]['dyid'])<>np.ndarray:
        xz=0
    else:
        zj=pd.unique(us.iloc[i]['dyid'])
        zj.tolist()
        if len(zj)>0:
         shumu=[]
         for ii in zj:
            shumu.append(np.count_nonzero(ii))
         if max(shumu)>1:
            xz=zj[shumu.index(max(shumu))]
         else:
            xz=zj[0]
        else:
            xz=0
    dyidv.append(xz)

    if type(us.iloc[i]['tcid'])<>np.ndarray:
        xz=0
    else:
        zj=pd.unique(us.iloc[i]['tcid'])
        zj.tolist()
        if len(zj)>0:
         shumu=[]
         for ii in zj:
            shumu.append(np.count_nonzero(ii))
         if max(shumu)>1:
            xz=zj[shumu.index(max(shumu))]
         else:
            xz=zj[0]
        else:
            xz=0
    tcidv.append(xz)

    if type(us.iloc[i]['jgid'])<>np.ndarray:
        xz=0
    else:
        zj=pd.unique(us.iloc[i]['jgid'])
        zj.tolist()
        if len(zj)>0:
         shumu=[]
         for ii in zj:
            shumu.append(np.count_nonzero(ii))
         if max(shumu)>1:
            xz=zj[shumu.index(max(shumu))]
         else:
            xz=zj[0]
        else:
            xz=0
    jgidv.append(xz)

    if type(us.iloc[i]['ggid'])<>np.ndarray:
        xz=0
    else:
         zj=pd.unique(us.iloc[i]['ggid'])
         zj.tolist()
         if len(zj)>0:
          shumu=[]
          for ii in zj:
             shumu.append(np.count_nonzero(ii))
          if max(shumu)>1:
             xz=zj[shumu.index(max(shumu))]
          else:
             xz=zj[0]
         else:
             xz=0
    ggidv.append(xz)


usbq=pd.DataFrame(data=us.user,columns=['user'])
usbq['zxb']=zxbv
usbq['cyb']=cybv
usbq['rzrq']=rzrqv
usbq['st']=stv
usbq['jyg']=jygv
usbq['czg']=czgv
usbq['bmg']=bmgv

usbq['sz']=szv
usbq['xd']=xdv
usbq['zd']=zdv
usbq['lz']=lzv
usbq['lj']=ljv
usbq['wl']=wlv
usbq['cjdm']=cjdmv
usbq['cjwh']=cjwhv
usbq['cjhy']=cjhyv
usbq['dl']=dlv
usbq['fl']=flv
usbq['jl']=jlv
usbq['tl']=tlv
usbq['lr']=lrv
usbq['lc']=lcv

usbq['gpzt']=gpztv
usbq['gpsz']=gpszv
usbq['gpdt']=gpdtv
usbq['gpxd']=gpxdv
usbq['gpcp']=gpcpv




usbq['pj']=pjv
usbq['pjchange']=pjchangev     
usbq['hyid']=hyidv  
usbq['dyid']=dyidv    
usbq['tcid']=tcidv  
usbq['jgid']=jgidv  
usbq['ggid']=ggidv  



## 标签转换文字。
hyzong=biaoqian1[1]
diyucode=biaoqian1[2]
ticaicode=biaoqian1[3]
fxs=biaoqian1[4]
gg=biaoqian1[5]

hym=[]
dym=[]
tcm=[]
fxsm=[]
ggm=[]
for i in range(len(usbq)):
    if usbq.iloc[i]['hyid']<>0:
       zj=usbq.iloc[i]['hyid']
       data1=hyzong[hyzong['id'].isin([zj])]
       hym.append(data1.iloc[0]['hangye'])
    else:
        hym.append(0)
    if usbq.iloc[i]['dyid']<>0:
       zj=usbq.iloc[i]['dyid']
       data1=diyucode[diyucode['id'].isin([zj])]
       dym.append(data1.iloc[0]['plate'])
    else:
        dym.append(0)
    if usbq.iloc[i]['tcid']<>0:
       zj=usbq.iloc[i]['tcid']
       data1=ticaicode[ticaicode['id'].isin([zj])]
       tcm.append(data1.iloc[0]['concept'])
    else:
        tcm.append(0)
    if usbq.iloc[i]['jgid']<>0:
       zj=usbq.iloc[i]['jgid']
       data1=fxs[fxs['id'].isin([zj])]
       fxsm.append(data1.iloc[0]['depart'])
    else:
        fxsm.append(0)
    if usbq.iloc[i]['ggid']<>0:
       zj=usbq.iloc[i]['ggid']
       data1=gg[gg['id'].isin([zj])]
       ggm.append(data1.iloc[0]['leixing'])        
    else:
        ggm.append(0)

usbq['hym']=hym
usbq['dym']=dym
usbq['tcm']=tcm
usbq['fxsm']=fxsm
usbq['ggm']=ggm


output = open('biaoqian_pj.pickle', 'wb')
pickle.dump(usbq, output)
output.close()


##  建立标签和数字对应
# 每一个类别统计代码出现最多的，都一样，就取第一个。





# 关于标签：汉子标签：行业地域题材，机构，任命，公告：希望转换为数据库代码，方便统计概率。汉子标签通过出现的概率最多帖标签。 

#d=us
#d.drop(d.index,inplace=True)
#user=us['user']
#hszb.sort()
#fw=0.9
#hszbv=hszb[int(fw*len(hszb))]
#szzb.sort()
#szzbv=szzb[int(fw*len(szzb))]
#zxb.sort()
#zxbv=zxb[int(fw*len(szzb))]
#cyb.sort()
#cybv=cyb[int(fw*len(szzb))]
#rzrq.sort()
#rzrqv=rzrq[int(fw*len(szzb))]
#st.sort()
#stv=st[int(fw*len(szzb))]
#jyg.sort()
#jygv=jyg[int(fw*len(szzb))]
#czg.sort()
#czgv=czg[int(fw*len(szzb))]
#bmg.sort()
#bmgv=bmg[int(fw*len(szzb))]
#
#sz.sort()
#szv=sz[int(fw*len(szzb))]
#xd.sort()
#xdv=xd[int(fw*len(szzb))]
#zd.sort()
#zdv=zd[int(fw*len(szzb))]
#lz.sort()
#lzv=lz[int(fw*len(szzb))]
#lj.sort()
#ljv=lj[int(fw*len(szzb))]
#wl.sort()
#wlv=wl[int(fw*len(szzb))]
#cjdm.sort()
#cjdmv=cjdm[int(fw*len(szzb))]
#cjwh.sort()
#cjwhv=cjwh[int(fw*len(szzb))]
#cjhy.sort()
#cjhyv=cjhy[int(fw*len(szzb))]
#dl.sort()
#dlv=dl[int(fw*len(szzb))]
#fl.sort()
#flv=fl[int(fw*len(szzb))]
#jl.sort()
#jlv=jl[int(fw*len(szzb))]
#tl.sort()
#tlv=tl[int(fw*len(szzb))]
#lr.sort()
#lrv=lr[int(fw*len(szzb))]
#lc.sort()
#lcv=lc[int(fw*len(szzb))]
#
#
#pj.sort()
#pjv=pj[int(fw*len(szzb))]
#pjchange.sort()
#pjchangev=pjchange[int(fw*len(szzb))]
#
#
#
#
#for i in us.user:
#    
    
        
    

# 数字标签： 应该在参与评价的所有个体的人内部进行评级，按照分位数四分之一给予标签。不能按照你出现概率多就用那个定义你。比如不能因为超跌多，你就是抄底，有可能你追涨再人群中也多

# 数字标签：主板，创业板，中小板（偏好那个版块，可以自己来比较）；st 绩优股，成长股，白马股，按照分别按照群体比例帖标签；上证，下跌，震荡，根据群体帖标签为追涨or上的。量能
    
 
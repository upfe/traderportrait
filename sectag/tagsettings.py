# -*- coding: utf-8 -*-
"""
标签参数

"""
TAG_FILED = {u'hszb': '沪深主板',
u'szzb':{'name':'深圳主板','is1':True,'islot':False},
u'zxb':{'name':'中小板','is1':True,'islot':False},
u'cyb':{'name':'创业板','is1':True,'islot':False},
u'rzrq':{'name':'融资融券','is1':True,'islot':False},
u'st':{'name':'st股','is1':True,'islot':False},
u'jyg':{'name':'绩优股','is1':True,'islot':False},
u'czg': {'name':'成长股','is1':True,'islot':False},
u'bmg': {'name':'白马股','is1':True,'islot':False},
u'hy': {'name':'行业','is1':False,'islot':True},
u'hyid':{'name':'行业id','is1':False,'islot':True},
u'dy':{'name':'地域','is1':False,'islot':False},
u'dyid': {'name':'地域id','is1':False,'islot':False},
u'tc':{'name':'题材','is1':False,'islot':True},
u'tcid':{'name':'题材id','is1':False,'islot':True},
u'sz':{'name':'上涨','is1':True,'islot':False},
u'xd': {'name':'下跌','is1':True,'islot':False},
u'zd':{'name':'震荡','is1':True,'islot':False},
u'lz':{'name':'量增','is1':True,'islot':False},
u'lj':{'name':'量减','is1':True,'islot':False},
u'lhb':{'name':'龙虎榜','is1':True,'islot':False},
u'gpzt':{'name':'股票涨停','is1':True,'islot':False},
u'gpsz':{'name':'股票上涨','is1':True,'islot':False},
u'gpxd':{'name':'股票下跌','is1':True,'islot':False},
u'gpdt':{'name':'股票跌停','is1':True,'islot':False},
u'gpcp':{'name':'股票持平','is1':True,'islot':False},
u'wl': {'name':'无量','is1':True,'islot':False},
u'cjdm': {'name':'成交低迷','is1':True,'islot':False},
u'cjwh': {'name':'成交温和','is1':True,'islot':False},
u'cjhy': {'name':'成交活跃','is1':True,'islot':False},
u'dl':{'name':'低量','is1':True,'islot':False},
u'fl': {'name':'放量','is1':True,'islot':False},
u'jl': {'name':'巨量','is1':True,'islot':False},
u'tl': {'name':'天量','is1':True,'islot':False},
u'lr':{'name':'资金流入','is1':True,'islot':False},
u'lc':{'name':'资金流出','is1':True,'islot':False},
u'jigou':{'name':'机构','is1':False,'islot':True},
u'jigouid': {'name':'机构id','is1':False,'islot':True},
u'renming':{'name':'研报作者','is1':False,'islot':False},
u'pj':{'name':'评级','is1':False,'islot':False},
u'pjchange':{'name':'评级改变','is1':False,'islot':False},
u'gg':{'name':'公告','is1':False,'islot':True},
u'ggid':{'name':'公告id','is1':False,'islot':True},}


TAG_CLASS = {
'10010100':[u'szzb',u'zxb',u'cyb',u'rzrq',u'st',u'jyg',u'czg',u'bmg'],
'10020100':[u'sz',u'xd',u'zd'],
'10020200':[u'lz',u'lj',u'gpzt',u'gpsz',u'gpxd',u'gpdt',u'gpcp',u'wl',u'cjdm',u'cjwh',u'cjhy',u'dl',u'fl',u'jl',u'tl',],
'10020300':[u'lr',u'lc'],
'10020400':[u'lhb'],
}


TICAI_CLASS = {
'31':'10010300',
'32':'10010200',
'33':'10010400',
}


TAG_TABLE ={
"10010500":"STK_TAG",
"10010400": "STK_TAG_FIX",
"10010200": "STK_TAG_FIX",
"10010300": "STK_TAG_FIX",
"10010100": "STK_TAG_FIX",
"10030200": "STK_TAG_BASE",
"10030104": "STK_TAG_BASE",
"10030102": "STK_TAG_BASE",
"10030101": "STK_TAG_BASE",
"10040101": "STK_TAG_FIN",

"10020500":"STK_TAG",
}

TAG_DEC={
"10010500":{"转义":{"蓝筹股":"比较偏爱蓝筹股","白马股":"比较偏爱白马股","成长股":"比较偏爱成长股"}},
"10020500":{"转义":{"热门股":"您似乎喜欢追热点"}},
"10010100": {"转义":{"上海主板":"大部分股票投资到了上海主板","深圳主板":"大部分股票投资到了深圳主板","中小板":"大部分股票投资到了中小板","创业板":"大部分股票投资到了创业板"}},
"10010400": {"拼接":"主要精力在抄"},
"10030200":{"拼接":"比较敏感"},
"10010200":{"拼接":"热衷于"},
"10030101":{"拼接":"你的观点相似于分析师"},
"10040101":{"转义":{"净利润较差":"可惜了，选的股大部分净利润都不太好","净利润一般":"可惜了，选的股大部分净利润都不太好","净利润良好":"欣慰的是，选的股大部分净利润都还行","净利润优秀":"欣慰的是，选的股大部分净利润都还行","净利润数据欠缺":"遗憾，您选的股大部分净利润数据还未找到"}},

#"10010500":{"直达":{}},
#"10020500":{"直达":{}},
#"10010100": {"直达":{}},
#"10010400": {"直达":{}},
#"10030200":{"直达":{}},
#"10010200":{"直达":{}},
#"10030101":{"直达":{}},
#"10040101":{"直达":{}},


}






DATABASES = {
    'dbcenter_local' : {
        'engine':'oracle',
        'host':'172.16.8.20',
        'port':1521,
        'sid':'dbcenter',
        'user':'upreader',
        'passwd':'reader_2017'
        },

    'dbcenter_line' : {
        'engine':'oracle',
        'host':'10.252.223.108',
        'port':1521,
        'sid':'upwhdb',
        'user':'readonly',
        'passwd':'anegIjege'
        },
		
    'mysql_test' : {
        'engine':'mysql',
        'host':'172.16.8.128',
        'user':'upchina', 
        'passwd':'upchina2016', 
        'db':'db_sectag', 
        'charset':'utf8'
        },
            
    'mysql_online' : {
        'engine':'mysql',
        'host':'rm-bp12k6652qw82t97g.mysql.rds.aliyuncs.com',
        'user':'oemoip', 
        'passwd':'bvu0wyt23fg', 
        'db':'db_fund_port', 
        'charset':'utf8'
        },

    'mysql_hot_test' : {
        'engine':'mysql',
        'host':'192.168.6.170',
        'user':'root', 
        'passwd':'root', 
        'db':'upchina_theme', 
        'charset':'utf8'
        },
		}

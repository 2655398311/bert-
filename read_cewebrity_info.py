# -*- coding:utf-8 -*-
'''
@Author: fhj
@Blog: https://i.csdn.net/#/uc/profile
@E-mail: 2655398311@qq.com
@File: read_cewebrity_info.py
@CreateTime:2020/5/8 16:24
'''

import pandas as pd
import redis
import json
import numpy as np
from clickhouse_driver import Client
import os
'''创建钉钉播报机器人'''
# 1、构建url
# 2、构建一下请求头部
import urllib.request

def bobao(content):
    #红人标签预测播报链接
    url = 'https://oapi.dingtalk.com/robot/send?access_token=1eb5a88de0f94c9e68d3b4a2d86b3c19cc38269b2b7d071f8855d74fa58a2c46'
    header = {"Content-Type": "application/json", "Charset": "UTF-8"}
    # 3、构建请求数据
    data = {
        "msgtype": "text",
        "text": {"content": content},
        "at": {
             "atMobiles": [
                 False
             ],
             "isAtAll": False
         }  # @全体成员（在此可设置@特定某人）
    }
    # 4、对请求的数据进行json封装
    sendData = json.dumps(data)  # 将字典类型数据转化为json格式
    sendData = sendData.encode("utf-8")  # python3的Request要求data为byte类型

    # 5、发送请求
    request = urllib.request.Request(url=url, data=sendData, headers=header)

    # 6、将请求发回的数据构建成为文件格式

    opener = urllib.request.urlopen(request)
    # 7、打印返回的结果
    print(opener.read())

drop_file = '正在删除上次分类博主博文文件!'.center(50)
bobao(content=drop_file)
path = '/home/chenfan/blog_floder'
for i in os.listdir(path):
    path_file = os.path.join(path, i)
    if os.path.isfile(path_file):
        os.remove(path_file)
    else:
        for f in os.listdir(path_file):
            path_file2 = os.path.join(path_file, f)
            if os.path.isfile(path_file2):
                os.remove(path_file2)

client2 = Client(host='10.228.83.251', port='19000', user='default', database='putao', password='nEB7+b3X')

'''redis链接'''
redis_pool = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
redis_conn = redis.Redis(connection_pool=redis_pool)
data_set_name = 'bert_sql'
def read_sql():
    click_house = client2.execute('select * from putao.d_weibo_cewebrity_info')
    col = client2.execute('DESCRIBE TABLE putao.d_weibo_cewebrity_info')
    col = pd.DataFrame(col)
    data = pd.DataFrame(click_house, columns=col[0].tolist())
    data_list = [data.ix[i].to_dict() for i in data.index.values]
    return data_list
data_a= read_sql()

import random
count = 0
for i in data_a:
    msg_name = str(i)
    if redis_conn.sismember(data_set_name, msg_name):
        continue
    else:
        redis_conn.sadd(data_set_name, msg_name)
    count+=1
    click_house = client2.execute("select * from putao.f_weibo_blog where platform_cid = '{}'".format(i['platform_cid']), types_check=True)
    col = client2.execute('DESCRIBE TABLE putao.f_weibo_blog')
    col = pd.DataFrame(col)
    data = pd.DataFrame(click_house,columns=col[0].tolist())
    print(data)
    data.to_csv(r'/home/chenfan/blog_floder/{}.csv'.format(str(i['platform_cid'])))
add_bozhu = '新增红人个数%s!'%(count)
bobao(content=add_bozhu)
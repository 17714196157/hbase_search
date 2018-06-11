import happybase
import time
from pymysql import connect
import random
# ./hbase thrift start &  hbase要启动thrift服务端口
import uuid
area_province = {
    '北京': 11, '上海': 31, '天津': 12, '重庆': 50, '江苏': 32, '广东': 44, '河北': 13,
    '河南': 41, '安徽': 34, '浙江': 33, '福建': 35, '甘肃 ': 62, '广西': 45, '贵州': 52,
    '云南': 53, '内蒙古': 15, '江西': 36, '湖北': 42, '四川': 51, '宁夏': 64, '青海': 63, '山东': 37, '陕西': 61,
    '山西': 14, '香港': 81, '海南': 46, '黑龙江': 23
}
company_name = ['中仁酒店管理有限公司','北控鑫晶沁太阳能发电有限公司','房地产开发有限公司',
                '璐逸石材有限公司','建筑工程有限公司','文化传媒有限公司','伟业建设有限公司',
               ]


pool = happybase.ConnectionPool(size=3, host='192.168.1.117',timeout=None, autoconnect=True, transport='buffered', protocol='binary')
# 获取连接
with pool.connection() as connection:
    table = connection.table('COMPANY')
    print(dir(table))

    t1 = time.time()
    query_str = "SingleColumnValueFilter ('company', 'name', =, 'substring:科技')"
    query = table.scan(row_start=None, row_stop=None, row_prefix=None, filter=query_str, limit=10)
    row_node_list = []
    for key, value_dict in query:
        print("key=", key)
        print("value--------------")
        for item_key in value_dict:
            print("item_key=", item_key)
            print("value=", value_dict[item_key])

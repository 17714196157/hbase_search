import os
import pandas as pd
import yaml
import redis
from service.actions.opt import init_map_field
from service.actions.opt import init_map_field_opt_search

BASE_PATH = os.path.dirname(os.path.dirname(__file__))
# print(BASE_PATH)
config_path = os.path.join(BASE_PATH, 'config')
type_db = "es"  # 使用后台版本  es 或者 hbase



# 读取每个网页字段与标准字段映射关系 file_map.xlsx
if "df" not in vars():
    # 初始化df配置表
    # 将yaml文件数据合成为一个DF表格，方便对接后面的程序
    with open(os.path.join(config_path, "file_map.yaml"), mode='r', encoding='utf-8') as f:
        res_file_map = yaml.load(f)
        df_file_map = pd.DataFrame(res_file_map)
    df = df_file_map
    df['hbase字段名'] = df.index.tolist()
    df.index = range(0, df.shape[0])
    df['default'] = df['default'].astype("str")
    # 将配置信息写入文件，方便查看
    df.to_excel("file_map.xlsx", index=False)

    # 初始化column_name_list
    column_name_list = df['hbase字段名'].values.tolist()
    column_name_list.insert(0, "ID")
    column_name_list.append("WEB_SOURCE")
    # print(column_name_list)

    map_field = init_map_field(df, type_db)
    map_field.update({"ID": "ID"})
    map_field.update({"AREA": "PROVINCE"})
    if type_db == "es":
        map_field.update({"WEB_SOURCE": "WEB_SOURCE"})
    else:
        map_field.update({"WEB_SOURCE": "A.WEB_SOURCE"})
    # print("map_field:", map_field)

    map_field_opt_search = init_map_field_opt_search(df,  type_db)
    map_field_opt_search.update({"ID": "="})
    map_field_opt_search.update({"A.WEB_SOURCE": "="})
    # print("map_field_opt_search:", map_field_opt_search)

    database_url = "http://localhost:8765/"  # phoenixdb_connect使用的phoenix QueryServer的服务地址

    # 初始化redis 链接池
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db='0')
    # 初始化 Elasticsearch 链接池
    es_host = "192.168.1.45"
    es_auth = ('elastic', '123456')
    # es = Elasticsearch("http://192.168.1.45:9200", http_auth=('elastic', '123456'))

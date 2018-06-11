import phoenixdb.cursor
import jpype
import jaydebeapi
import pandas as pd
import os
import redis
import json
from functools import wraps
from elasticsearch import Elasticsearch
import time

servie_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
df = pd.read_excel(os.path.join(servie_path, os.path.join('config', 'file_map.xlsx')), encoding='utf-8',
                   eindex_col=False)
pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db='0')

def init_map_field(df):
    """
    字段名映射成对应那个列族的那个字段
    file_map.xlsx 是需要输入的字段映射关系
    :return:
    """
    map_field={}
    for n in df.index:
        hbase_filename = "{f}.{name}".format(f=df.at[n, 'hbase列族'], name=df.at[n, 'hbase字段名'])\
                        if df.at[n, 'hbase列族'] != "rowkey" else df.at[n, 'hbase字段名']
        map_field.update({df.at[n, 'hbase字段名']: hbase_filename})
    return map_field

def init_map_field_opt_search(df):
    """
    字段名映射成对应那个列族的那个字段
    file_map.xlsx 是需要输入的字段映射关系
    :return:
    """
    map_opt_search = {}
    for n in df.index:
        hbase_filename = "{f}.{name}".format(f=df.at[n, 'hbase列族'], name=df.at[n, 'hbase字段名'])\
                        if df.at[n, 'hbase列族'] != "rowkey" else df.at[n, 'hbase字段名']
        opt_search = "like" if df.at[n, 'hbase列族'] != "rowkey" else "="
        map_opt_search.update({hbase_filename: opt_search})
    return map_opt_search

def cache_func_redis(timeout=100):
    # pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db='0')
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = args[-1]
            # lst_dct = sorted([{k: kwargs[k]} for k in kwargs], key=lambda d:d.keys()[0])
            # lst = [str(d.values()[0]) for d in lst_dct]
            k = '_'.join([func.__name__, str(args[0])])
            r = redis.StrictRedis(connection_pool=pool)
            d = r.get(k)
            if d:
                # print("命中缓存   k=" + str(k))
                logger.info("命中缓存   k=" + str(k))
                d = d.decode()
                # print(d,type(d))
                res = json.loads(d)['res']
                return res
            else:
                # print("未命中缓存   k=" + str(k))
                logger.info("未命中缓存   k=" + str(k))
                res = func(*args, **kwargs)
                d = json.dumps({
                    'res': res
                })
                r.set(k, d)
                r.expire(k, timeout)

                return res
        return wrapper
    return decorator

def cache_flag(key):
    r = redis.StrictRedis(connection_pool=pool)
    d = r.get(key)
    if d:
        return True
    else:
        return False

map_field = init_map_field(df)
map_field.update({"ID": "ID"})
map_field.update({"WEB_SOURCE": "A.WEB_SOURCE"})
map_field.update({"AREA": "PROVINCE"})      # area也当成省份字段
print("map_field:", map_field)

map_field_opt_search = init_map_field_opt_search(df)
map_field_opt_search.update({"ID": "="})
map_field_opt_search.update({"A.WEB_SOURCE": "="})
print("map_field_opt_search:", map_field_opt_search)

column_name_list = df['hbase字段名'].values.tolist()
column_name_list.insert(0, "ID")
column_name_list.append("WEB_SOURCE")
print("column_name_list:", column_name_list)




@cache_func_redis(timeout=36000)
def es_search(search_condition, es, nskip=1, per_page=10, logger=None):
    t1 = time.time()
    list_match = []
    for key, value in search_condition.items():
        {"match": {key: value}}
        list_match.append(list_match)
    body = {
        "query": {
            "bool": {
                "should": list_match
            }
        }
    }
    result_search_list = es.search(body=body, index="db", doc_type="company", _source_include="_id",
                        search_type="dfs_query_then_fetch", sort="_id", from_=nskip, pre_filter_shard_size=per_page)
    result_search_list = result_search_list["hits"]["hits"]
    all_n = result_search_list["hits"]["total"]
    t2 = time.time()
    logger.info("make  result_search_list cost time =" + str(t2 - t1))
    return result_search_list, all_n


def db_search(search_dict, page=1, per_page=10, startn=0, logger=None):
    """
    :param search_dict: search_dict: 交集条件 同时满足字段值包含 给定字符串 ,字段必须在表内存在
    :param page:   第几页
    :param per_page:  每页数据量
    :param startn:  数据起始偏移量
    :param quick: None 表示需要知道总数， 其他表示不需要知道
    :param logger: 日志对象
    :return:
    """
    # logger.info("begin db_search")
    # logger.info("search_dict=" + str(search_dict))
    # logger.info("db_search startn quick "+str(startn)+" "+str(quick))
    t1 = time.time()
    search_condition_list = []
    for field_name in search_dict:
        field_value = search_dict.get(field_name, "").strip()
        db_field_name = map_field.get(field_name.upper().strip(), None)

        if db_field_name is not None and field_value != "":
            search_condition_list.append({db_field_name: field_value})

    search_condition = []

    if search_condition is not None:
        try:
            skip_n = startn + (page - 1) * per_page
            cache_flag_tmp = cache_flag('_'.join([es_search.__name__, str(search_condition)]))
            if not cache_flag_tmp:
                es = Elasticsearch("http://192.168.1.45:9200", http_auth=('elastic', '123456'))
                result_search_list, all_n = es_search(search_condition, es, nskip=skip_n, per_page=per_page,logger=logger)
    else:
        result_search_list = []
        all_n = 0
    t2 = time.time()
    logger.info("db_search cost all time:" + str(t2-t1))
    return result_search_list, all_n


if __name__ == "__main__":
    pass

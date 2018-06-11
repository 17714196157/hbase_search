import phoenixdb.cursor
import jpype
import jaydebeapi
import pandas as pd
import os
import redis
import json
from functools import wraps
from functools import lru_cache
import subprocess
from threading import Timer
import time

class QueryServer(object):
    @staticmethod
    def restart_queryserver():
        try:
            subprocess.Popen("/home/hadoop/phoenix/bin/queryserver.py stop;", shell=True,
                             stdout=subprocess.PIPE,
                             cwd="/home/hadoop/phoenix/bin/")
            time.sleep(3)
            subprocess.Popen("export HBASE_CONF_DIR=/home/hadoop/alihbase/conf ;/home/hadoop/phoenix/bin/queryserver.py start;", shell=True,
                             stdout=subprocess.PIPE,
                             cwd="/home/hadoop/phoenix/bin/")
            flag_queryserver = QueryServer.get_status()
            n = 0
            while flag_queryserver != "1" and n < 10:
                print("ERROR flag_queryserver has delete queryserver start fail", n)
                time.sleep(2)
                n += 1
                flag_queryserver = QueryServer.get_status()
                # raise KeyError("ERROR flag_queryserver has delete")
            if flag_queryserver != "1":
                print("ERROR flag_queryserver has delete queryserver start fail", n)

        except Exception as e:
            print(str(e))

    @staticmethod
    def get_status():
        try:
            flag_queryserver = subprocess.Popen('''lsof -i:8765 | grep LISTEN | wc -l''', shell=True,
                                                stdout=subprocess.PIPE, cwd="/home/hadoop/phoenix/bin/").stdout.read()
            flag_queryserver = bytes.decode(flag_queryserver).strip()
            return flag_queryserver
        except Exception as e:
            return "0"

    @staticmethod
    def run_time_queryserver():
        # print("开始探测 queryserver是否存在")
        flag_queryserver = QueryServer.get_status()
        if flag_queryserver.strip() != "1":
            QueryServer.restart_queryserver()
        t = Timer(interval=60, function=QueryServer.run_time_queryserver)
        t.start()

servie_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
df = pd.read_excel(os.path.join(servie_path, os.path.join('config', 'file_map.xlsx')), encoding='utf-8',
                   eindex_col=False)

# database_url = "http://localhost:8765/"
database_url = "http://localhost:8765/"
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


class phoenixdb_connect(object):
    def __init__(self, database_url, logger=None):
        try:
            logger.info("phoenixdb_connect 尝试建立链接")
            self.conn = phoenixdb.connect(database_url, max_retries=1, autocommit=True)
        except Exception as e:
            logger.info("QueryServer 异常，重启它，phoenixdb_connect 尝试建立链接" )
            QS = QueryServer()
            QS.restart_queryserver()
            logger.info("QueryServer 已重启")
            # self.conn = phoenixdb.connect(database_url, max_retries=1, autocommit=True)
            raise KeyError("重启QueryServer服务线程安全问题")
        logger.info("phoenixdb_connect 建立链接结束 " + str(type(self.conn)))

    def run_sql(self, sql_str="select * from COMPANY where ID in (select /*+ Index(COMPANY NAME_INDEX)*/ ID from COMPANY where A.NAME like '%网%'  order by ID  limit 100 )"):
        # print("run sql :", sql_str)
        # t1 = time.time()
        self.cursor.execute(sql_str)
        # cursor.execute("select * from COMPANY  limit 10")
        # t2 = time.time()
        # print("run sql cost time=", t2-t1)

    def close_connect(self):
        # self.cursor.close()
        self.conn.close()


class jdbc_connect(object):
    def __init__(self,hbase_conf_dir=r"/home/hadoop/alihbase/conf/", phoenix_client_jar=r'/home/hadoop/phoenix/phoenix-4.11.0-AliHBase-1.1-0.3-client.jar',
            zkhost='hb-bp1mek3731612ox1h-002.hbase.rds.aliyuncs.com,hb-bp1mek3731612ox1h-001.hbase.rds.aliyuncs.com,hb-bp1mek3731612ox1h-003.hbase.rds.aliyuncs.com',
            driver='org.apache.phoenix.jdbc.PhoenixDriver'):
        args = '-Djava.class.path=%s;%s' % (phoenix_client_jar, hbase_conf_dir)
        jvm_path = jpype.getDefaultJVMPath()
        jpype.startJVM(jvm_path, args)
        self.conn = jaydebeapi.connect(driver, [
            'jdbc:phoenix:{zkhost}:2181'.format(zkhost=zkhost),
            '', ''], phoenix_client_jar,libs="phoenix_client_jar;hbase_conf_dir")
        self.cursor = self.conn.cursor()

    def run_sql(self, sql_str="select * from COMPANY where ID in (select /*+ Index(COMPANY NAME_INDEX)*/ ID from COMPANY where A.NAME like '%网%'  order by ID  limit 100 )"):
        # print("run sql :", sql_str)
        # t1 = time.time()
        self.cursor.execute(sql_str)
        # cursor.execute("select * from COMPANY  limit 10")
        # t2 = time.time()
        # print("run sql cost time=", t2-t1)

    def close_connect(self):
        self.cursor.close()
        self.conn.close()


# global db_object
# db_object = jdbc_connect()
# db_object = jdbc_connect(phoenix_client_jar= r"/root/apache-phoenix-4.13.1-HBase-1.2-bin/phoenix-4.13.1-HBase-1.2-client.jar", zkhost='master,slave1,slave2')
# db_object = phoenixdb_connect(database_url=r'http://localhost:8765/')
# db_object = phoenixdb_connect(database_url=r'http://192.168.1.117:8765/')


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
def count_search_dict(search_condition, db_object, logger):
    t1 = time.time()
    try:
        sql_count = "select count(*) from COMPANY {search_condition}".format(search_condition=search_condition)
        # logger.info(" sql_count =" + str(sql_count))
        db_object.cursor.execute(sql_count)
        for key, value in db_object.cursor.fetchone().items():
            all_n = value
    except Exception as e:
        all_n = 0
        logger.error("ERROR 计算总数的条件 count_search_dict:" + str(e))
    t2 = time.time()
    logger.info("count()=" + str(all_n) + " 计算总数 cost time =" + str(t2 - t1))
    return all_n


@cache_func_redis(timeout=36000)
def result_search(search_condition_limit, db_object, logger):
    trun1 = time.time()
    sql_str = "select * from COMPANY {search_condition_limit}".format(
        search_condition_limit=search_condition_limit)
    logger.info("sql_str=" + sql_str)
    db_object.cursor.execute(sql_str)
    trun2 = time.time()
    logger.info("cursor.execute sql cost time =" + str(trun2 - trun1))

    t1 = time.time()
    result_search_list = []
    for item in db_object.cursor.fetchall():
        result_search_list.append(item)
    rse_df = pd.DataFrame(result_search_list, columns=column_name_list)
    result_search_list = rse_df.to_dict('records')
    t2 = time.time()
    logger.info("make  result_search_list cost time =" + str(t2 - t1))
    return result_search_list




def db_search(search_dict, page=1, per_page=10, startn=0, quick=None, startid=0, logger=None):
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
    db_object = None

    search_condition_list = []
    for field_name in search_dict:
        field_value = search_dict.get(field_name, "").strip()
        db_field_name = map_field.get(field_name.upper().strip(), None)

        if db_field_name is not None and field_value != "":
            opt_search = map_field_opt_search.get(db_field_name, "like")
            if opt_search == "like":
                search_condition_list.append("{db_field_name} {opt_search} '%{value}%'".format(
                    opt_search=opt_search,
                    db_field_name=db_field_name,
                    value=field_value))

            elif opt_search == "=":
                search_condition_list.append("{db_field_name} {opt_search} '{value}'".format(
                    opt_search=opt_search,
                    db_field_name=db_field_name,
                    value=field_value))
            else:
                pass

    if len(search_condition_list) != 0:
        search_condition_list = sorted(search_condition_list)
        search_condition = "where {}".format(" and ".join(search_condition_list))
        # logger.info("db_search search_condition:" + str(search_condition))
    else:
        search_condition = None
        # logger.info("db_search search_condition is null ")

    if search_condition is not None:
        try:
            cache_flag_tmp = cache_flag('_'.join([count_search_dict.__name__, str(search_condition)]))
            if not cache_flag_tmp:
                db_object = phoenixdb_connect(database_url=database_url, logger=logger)
                db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
            else:
                db_object = None

            if quick is None:
                # logger.info("start count_search_dict cache_info" + str(count_search_dict.cache_info()))
                all_n = count_search_dict(search_condition, db_object, logger)
                # logger.info("end count_search_dict cache_info" + str(count_search_dict.cache_info()))
            else:
                logger.info("db_search 不需要返回 数据总数")
                all_n = 0

            skip_n = startn + (page - 1) * per_page

            search_condition_limit = "{search_condition} and ID > {startid} order by ID limit {per_page} offset {offset}".format(
                per_page=str(per_page), search_condition=search_condition, offset=str(skip_n), startid=startid)

            cache_flag_tmp = cache_flag('_'.join([result_search.__name__, str(search_condition_limit)]))

            if not cache_flag_tmp and db_object is None:
                db_object = phoenixdb_connect(database_url=database_url, logger=logger)
                db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
            else:
                pass
                # db_object = None

            # logger.info("start result_search cache_info" + str(result_search.cache_info()))
            result_search_list = result_search(search_condition_limit, db_object, logger)
            # logger.info(" end result_search cache_info" + str(result_search.cache_info()))

        except Exception as e:
            logger.error("ERROR 查询" + str(e))
            result_search_list = []
            all_n = 0
    else:
        result_search_list = []
        all_n = 0
    t2 = time.time()

    logger.info(str(db_object is None) + " db_search cost all time:" + str(t2-t1))

    if db_object is not None:
        try:
            db_object.cursor.close()
            db_object.conn.close()
            del db_object
        except Exception as e:
            logger.info(" db_object close fail" + str(e))

    return result_search_list, all_n

@lru_cache(maxsize=32)
def db_area(logger=None):
    logger.info("begin return PROVINCE list")
    db_object = phoenixdb_connect(database_url=database_url, logger=logger)
    db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    t1 = time.time()
    sql_str = "select DISTINCT PROVINCE from COMPANY"
    db_object.cursor.execute(sql_str)
    result_search_list = []

    for index, item in enumerate(db_object.cursor.fetchall()):
        # print(item)
        for key in item:
            result_search_list.append(item[key])
    t2 = time.time()
    logger.info("return PROVINCE list cost time="+str(t2-t1))
    db_object.cursor.close()
    db_object.conn.close()
    del db_object
    return result_search_list


@lru_cache(maxsize=128)
def db_CITY(PROVINCE, logger=None):
    logger.info("begin  CITY list  for PROVINCE:"+str(PROVINCE))
    if PROVINCE is None:
        return []

    db_object = phoenixdb_connect(database_url=database_url, logger=logger)
    db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    t1 = time.time()
    sql_str = "select DISTINCT CITY from COMPANY where PROVINCE=\'{PROVINCE}\' ".format(PROVINCE=PROVINCE)
    db_object.cursor.execute(sql_str)
    result_search_list = []

    for index, item in enumerate(db_object.cursor.fetchall()):
        # print(item)
        for key in item:
            result_search_list.append(item[key])
    t2 = time.time()
    logger.info("return CITY list cost time="+str(t2-t1))
    db_object.cursor.close()
    db_object.conn.close()
    del db_object
    return result_search_list

@lru_cache(maxsize=128)
def db_URBAN_AREA(PROVINCE, CITY, logger=None):
    logger.info("begin  URBAN_AREA list  for CITY:"+str(CITY))
    if PROVINCE is None or CITY is None:
        return []

    db_object = phoenixdb_connect(database_url=database_url, logger=logger)
    db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    t1 = time.time()
    sql_str = "select DISTINCT URBAN_AREA from COMPANY where PROVINCE=\'{PROVINCE}\' and  CITY=\'{CITY}\'  ".format\
        (PROVINCE=PROVINCE, CITY=CITY)
    db_object.cursor.execute(sql_str)
    result_search_list = []

    for index, item in enumerate(db_object.cursor.fetchall()):
        # print(item)
        for key in item:
            result_search_list.append(item[key])
    t2 = time.time()
    logger.info("return URBAN_AREA list cost time="+str(t2-t1))
    db_object.cursor.close()
    db_object.conn.close()
    del db_object
    return result_search_list

@lru_cache(maxsize=128)
def db_source(logger=None):
    logger.info("begin  source list ")

    db_object = phoenixdb_connect(database_url=database_url, logger=logger)
    db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    t1 = time.time()
    sql_str = "select DISTINCT web_source from COMPANY"
    db_object.cursor.execute(sql_str)
    result_search_list = []

    for index, item in enumerate(db_object.cursor.fetchall()):
        # print(item)
        for key in item:
            result_search_list.append(item[key])
    t2 = time.time()
    logger.info("return source list cost time="+str(t2-t1))
    db_object.cursor.close()
    db_object.conn.close()
    del db_object
    return result_search_list


def db_company_detail(company_id_list, logger=None):
    logger.info("begin db_company_detail id="+str(company_id_list))
    db_object = phoenixdb_connect(database_url=database_url, logger=logger)
    db_object.cursor = db_object.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    t1 = time.time()
    try:
        company_id_list_or = []
        for id in company_id_list:
            id_str = "ID={}".format(id)
            company_id_list_or.append(id_str)
        sql_str = "select * from COMPANY where {company_id}".format(company_id=' or '.join(company_id_list_or))
        print(sql_str)


        result_search = []
        for index, item in enumerate(db_object.cursor.fetchall()):
            item_series = pd.Series(item, index=column_name_list)
            result_search.append(item_series.to_dict())

    except Exception as e:
        logger.error("ERROR  db_company_detail id=" + str(e))
        result_search = {}
    t2 = time.time()
    logger.info("return company_detail list cost time="+str(t2-t1))
    db_object.cursor.close()
    db_object.conn.close()
    del db_object
    return result_search


if __name__ == "__main__":
    pass

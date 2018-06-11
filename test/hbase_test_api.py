import phoenixdb.cursor
import jpype
import jaydebeapi
import time
import pandas as pd
import os


class jdbc_connect(object):
    def __init__(self, hbase_conf_dir=r"/home/hadoop/alihbase/conf/hbase-site.xml", phoenix_client_jar=r'/home/hadoop/phoenix/phoenix-4.11.0-AliHBase-1.1-0.3-client.jar',
            zkhost='hb-bp1mek3731612ox1h-002.hbase.rds.aliyuncs.com,hb-bp1mek3731612ox1h-001.hbase.rds.aliyuncs.com,hb-bp1mek3731612ox1h-003.hbase.rds.aliyuncs.com',
            driver='org.apache.phoenix.jdbc.PhoenixDriver'):
        # args = '-Djava.class.path=%s;%s' % (phoenix_client_jar, hbase_conf_dir)
        # jvm_path = jpype.getDefaultJVMPath()
        # jpype.startJVM(jvm_path, args)
        driver_args = {
            "hbase.client.scanner.caching": "2000",
            "phoenix.mutate.batchSize": "2000"
        }
        lib = "{phoenix_client_jar};{hbase_conf_dir}".format(phoenix_client_jar=phoenix_client_jar, hbase_conf_dir=hbase_conf_dir)
        self.conn = jaydebeapi.connect(driver, 'jdbc:phoenix:{zkhost}:2181'.format(zkhost=zkhost), jars=phoenix_client_jar, libs=lib)

        self.cursor = self.conn.cursor()

    def run_sql(self, sql_str="select * from COMPANY where ID in (select /*+ Index(COMPANY NAME_INDEX)*/ ID from COMPANY where A.NAME like '%网%'  order by ID  limit 100 )"):
        print("run sql :", sql_str)
        t1 = time.time()
        self.cursor.execute(sql_str)
        # cursor.execute("select * from COMPANY  limit 10")
        t2 = time.time()
        print("run sql cost time=", t2-t1)

    def close_connect(self):
        self.cursor.close()
        self.conn.close()

class phoenixdb_connect(object):
    def __init__(self, database_url=r'http://localhost:8765/'):
        self.conn = phoenixdb.connect(database_url, autocommit=True)
        self.cursor = self.conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
        self.conn.set_session()

    def run_sql(self, sql_str="select * from COMPANY where ID in (select /*+ Index(COMPANY NAME_INDEX)*/ ID from COMPANY where A.NAME like '%网%'  order by ID  limit 100 )"):
        print("run sql :", sql_str)
        t1 = time.time()
        self.cursor.execute(sql_str)
        # cursor.execute("select * from COMPANY  limit 10")
        t2 = time.time()
        print("run sql cost time=", t2-t1)

    def close_connect(self):
        self.cursor.close()
        self.conn.close()


# db_object = jdbc_connect()
# db_object = jdbc_connect(phoenix_client_jar= r"/root/apache-phoenix-4.13.1-HBase-1.2-bin/phoenix-4.13.1-HBase-1.2-client.jar", zkhost='master,slave1,slave2')
# db_object = phoenixdb_connect(database_url=r'http://192.168.1.117:8765/')
print("11111111111")
db_object = phoenixdb_connect(database_url=r'http://localhost:8765/')
print("222222222222")
servie_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.sep + "service"
print(servie_path)
# df = pd.read_excel('file_map.xlsx', encoding='utf-8', eindex_col=False)
df = pd.read_excel(os.path.join(servie_path, os.path.join('config', 'file_map.xlsx')), encoding='utf-8', eindex_col=False)

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


map_field = init_map_field(df)
map_field_opt_search = init_map_field_opt_search(df)

column_name_list = df['hbase字段名'].values.tolist()
column_name_list.insert(0, "ID")
column_name_list.append("web_source")

def run():

    tcount1 = time.time()
    try:
        db_object.cursor.execute("select count(*) from COMPANY  where REG_CAPITAL = '0' and A.name like '合作社'")
        all_n = db_object.cursor.fetchone()[0]
    except Exception as e:
        all_n = 0
        print("count_search_dict ERROR :" + str(e))
    tcount2 = time.time()
    print(str(all_n) + " count() cost time =" + str(tcount2 - tcount1))

    sql_str = "select * from COMPANY  where REG_CAPITAL = '0' limit 10000"

    t1 = time.time()
    trun1 = time.time()
    db_object.cursor.execute(sql_str)
    trun2 = time.time()
    print("cursor.execute sql cost time =" + str(trun2 - trun1))

    result_search_list = []
    for item in db_object.cursor.fetchall():
        result_search_list.append(item)
    rse_df = pd.DataFrame(result_search_list, columns=column_name_list)
    result_search_list = rse_df.to_dict('records')

    t2 = time.time()
    print("make  result_search_list cost time =" + str(t2 - t1))



def runqq():
    sql_str = "select * from COMPANY where A.NAME like '%合作社%' and REG_CAPITAL = '0'  limit 5000 offset 5000"
    trun1 = time.time()
    res_df = pd.read_sql_query(sql_str, db_object.conn)
    trun2 = time.time()
    result_list = res_df.to_dict('records')
    print("read_sql_query sql cost time =" + str(trun2 - trun1))
    print(type(result_list), result_list[0])


if __name__ == "__main__":
    import profile
    profile.run("run()", "prof.txt")  # 结果输出到 文件
    import pstats
    p = pstats.Stats("prof.txt")
    p.sort_stats("time").print_stats()
    p.sort_stats("time").print_callers()

    # from line_profiler import LineProfiler
    # lp = LineProfiler()
    # lp_wrapper = lp(run)
    # lp_wrapper()
    # lp.print_stats()
    #


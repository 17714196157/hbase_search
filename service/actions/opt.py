
def init_map_field(df, type_db):
    """
    函数功能： 接口输入字段 映射成 数据库db里的字段名
    字段名映射成对应那个列族的那个字段
    df 是需要输入的字段映射关系
    :return:
    """
    map_field ={}
    for n in df.index:
        if type_db == "hbase":
            hbase_filename = "{f}.{name}".format(f=df.at[n, 'hbase列族'], name=df.at[n, 'hbase字段名'])\
                            if df.at[n, 'hbase列族'] != "rowkey" else df.at[n, 'hbase字段名']
        else:
            hbase_filename = df.at[n, 'hbase字段名']
        map_field.update({df.at[n, 'hbase字段名']: hbase_filename})
    return map_field


def init_map_field_opt_search(df, type_db):
    """
    函数功能： 每个字段搜索使用的语句不同，初始化每个字段搜索语句
    规则：hbase 搜索过程中 ，枚举值就精确搜索， 非枚举值就模糊匹配
    df 是需要输入的字段映射关系
    :return:
    """
    map_opt_search = {}
    for n in df.index:
        if type_db == "hbase":
            hbase_filename = "{f}.{name}".format(f=df.at[n, 'hbase列族'], name=df.at[n, 'hbase字段名'])\
                            if df.at[n, 'hbase列族'] != "rowkey" else df.at[n, 'hbase字段名']
            opt_search = "like" if df.at[n, 'hbase列族'] != "rowkey" else "="
        else:
            hbase_filename = df.at[n, 'hbase字段名']
            opt_search = "="
        map_opt_search.update({hbase_filename: opt_search})
    return map_opt_search



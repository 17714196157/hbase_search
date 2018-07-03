import time
from service.config.setting import map_field
from functools import lru_cache
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from service.config.setting import es_auth, es_host
from elasticsearch import helpers


def es_search(search_condition, es, nskip=1, per_page=10, logger=None):
    t1 = time.time()
    list_match = []
    logger.debug("search_condition: {}".format(search_condition))
    for node_dict in search_condition:
        item = {"match_phrase": node_dict}
        list_match.append(item)
    body = {
        "query": {
            "bool": {
                "must": list_match
            }
        }, "from": nskip, "size": per_page
    }
    logger.debug("search_condition = {}".format(str(body)))
    try:
        data_result_dict = es.search(body=body, index="db", doc_type="company", sort="_id")
        # print(data_result_dict)
        result_search_list = [x.get('_source', None) for x in data_result_dict["hits"]["hits"]]
        all_n = data_result_dict["hits"]["total"]
        t2 = time.time()
        logger.debug("all_n = {}".format(str(all_n)))
        logger.info("es_search cost time = {}".format(str(t2 - t1)))
        return result_search_list, all_n
    except TransportError as e:
        # time.sleep(2)
        # es = Elasticsearch(host=[es_host], http_auth=es_auth)
        t2 = time.time()
        logger.error("ERROR Elasticsearch 链接 {}".format(str(e)))
        logger.info("es_search cost time = {}".format(str(t2 - t1)))
        return [], 0


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
    logger.info("begin db_search")
    logger.info("search_dict=" + str(search_dict))
    logger.info("db_search startn quick "+str(startn)+" "+str(quick))
    search_condition_list = []
    for field_name in search_dict:
        field_value = search_dict.get(field_name, "").strip()
        db_field_name = map_field.get(field_name.upper().strip(), None)

        if db_field_name is not None and field_value != "":
            search_condition_list.append({db_field_name: field_value})

    search_condition = search_condition_list

    if search_condition is not None:
        try:
        # if True:
            es = Elasticsearch(hosts=[es_host], http_auth=es_auth)
            skip_n = startn + (page - 1) * per_page
            result_search_list, all_n = es_search(search_condition, es, nskip=skip_n, per_page=per_page, logger=logger)
        except Exception as e:
            logger.error("error es_search"+str(e))
            result_search_list = []
            all_n = 0
    else:
        result_search_list = []
        all_n = 0
    return result_search_list, all_n


@lru_cache(maxsize=32)
def db_area(logger=None):
    """
    返回省份的列表
    :param logger:
    :return:
    """
    logger.info("begin return PROVINCE list")
    es = Elasticsearch(hosts=[es_host], http_auth=es_auth)
    body = {"size": 1, "aggs": {"PROVINCE": {
        "terms": {"field": "PROVINCE.keyword"}
    }}
            }
    data_result_dict = es.search(body=body, index="db", doc_type="company")
    result_search_list = [x.get('key', None) for x in data_result_dict["aggregations"]["PROVINCE"]["buckets"]]
    return result_search_list


@lru_cache(maxsize=128)
def db_CITY(PROVINCE, logger=None):
    """
    返回省份下所有城市的列表
    :param PROVINCE:
    :param logger:
    :return:
    """
    logger.info("begin return CITY list")
    if PROVINCE == None or PROVINCE == "":
        return []
    es = Elasticsearch(hosts=[es_host], http_auth=es_auth)
    body = {"size": 0,
            "query": {
                "match": {
                    "PROVINCE": "{}".format(PROVINCE)
                }
            },
            "aggs": {
                "CITY": {
                    "terms": {"field": "CITY.keyword"}
                }
            }
            }
    data_result_dict = es.search(body=body, index="db", doc_type="company")
    result_search_list = [x.get('key', None) for x in data_result_dict["aggregations"]["CITY"]["buckets"]]
    return result_search_list


@lru_cache(maxsize=128)
def db_URBAN_AREA(PROVINCE, CITY, logger=None):
    """
    返回城市下所有街道的列表
    :param PROVINCE:
    :param logger:
    :return:
    """
    logger.info("begin return URBAN_AREA list")
    if CITY == None or CITY == "":
        return []

    es = Elasticsearch(hosts=[es_host], http_auth=es_auth)
    body = {"size": 0,
            "query": {
                "match": {
                    "CITY": "{}".format(CITY)
                }
            },
            "aggs": {
                "URBAN_AREA": {
                    "terms": {"field": "URBAN_AREA.keyword"}
                }
            }
            }
    data_result_dict = es.search(body=body, index="db", doc_type="company")
    result_search_list = [x.get('key', None) for x in data_result_dict["aggregations"]["URBAN_AREA"]["buckets"]]
    return result_search_list

@lru_cache(maxsize=128)
def db_source(logger=None):
    """
    返回数据来源的列表
    :param logger:
    :return:
    """
    logger.info("begin return db_source list")
    es = Elasticsearch(hosts=[es_host], http_auth=es_auth)
    body = {"size": 0,
            "aggs": {
                "WEB_SOURCE": {
                    "terms": {"field": "WEB_SOURCE.keyword"}
                }
            }
}
    data_result_dict = es.search(body=body, index="db", doc_type="company")
    result_search_list = [x.get('key', None) for x in data_result_dict["aggregations"]["WEB_SOURCE"]["buckets"]]
    return result_search_list

if __name__ == "__main__":
    pass

import redis
import json
from functools import wraps
from service.config.setting import pool


def cache_func_redis(timeout=100):
    """
    函数结果缓存的装饰器
    :param timeout:
    :return:
    """
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

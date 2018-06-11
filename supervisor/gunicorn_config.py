import sys
import os
import multiprocessing
import platform

# __file__：当前文件路径
# os.path.dirname(file): 某个文件所在的目录路径
# os.path.join(a, b, c,....): 路径构造 a/b/c
# os.path.abspath(path): 将path从相对路径转成绝对路径
# os.pardir: Linux下相当于"../"
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))

# 最好不要写死，用os获得相对于此配置文件的app所在的路径
path_of_current_dir = parent_dir

# print path_of_current_dir
sys.path.insert(0, path_of_current_dir)

# worker_class = 'gthread'
worker_class = 'gevent'

# workers也要根据实际情况修改
workers = 10  # multiprocessing.cpu_count() * 2 + 1

chdir = path_of_current_dir

worker_connections = 1000
timeout = 600    # 超时时间
max_requests = 2000  # 到达max_requests时 worker会在处理好这个请求后自动重启
graceful_timeout = 120

loglevel = 'debug'

reload = True
debug = False

bind = "%s:%s" % ("0.0.0.0", 10003)

access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s %(T)s'

if platform.system() == 'Windows':
    errorlog = r'D:\error.log'
    accesslog = r'D:\access.log'

else:
    errorlog = '/home/log_sellbot/hbase_big_data/gunicorn_error.log'
    accesslog = '/home/log_sellbot/hbase_big_data/gunicorn_access.log'

capture_output = True


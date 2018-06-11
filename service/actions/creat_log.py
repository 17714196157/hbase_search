# -*- coding:utf-8 -*-

"""
File Name : 'creat_log'.py
Description: 日志创建基础模块
Author: 'btows'
Date: '18-1-4' '上午10:05'
"""
import os
import logging
from logging.handlers import TimedRotatingFileHandler


def creat_app_log(log_path='/home/log_sellbot/hbase_big_data/', level=logging.INFO):
    """
    创建app的日志handle
    :param: 日志路径:
    :return: 日志logger
    """
    LOGGING_MSG_FORMAT = '[%(asctime)s] [%(filename)s[line:%(lineno)d]] [%(levelname)s] [%(message)s]'
    LOGGING_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    logging.basicConfig(level=level, format=LOGGING_MSG_FORMAT, datefmt=LOGGING_DATE_FORMAT)
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    LOG_PATH = os.path.join(log_path, "hbase_db_search.log")
    fileHandler = logging.handlers.WatchedFileHandler(LOG_PATH)
    fileHandler.setFormatter(logging.Formatter(LOGGING_MSG_FORMAT))
    logger = logging.getLogger()
    logger.addHandler(fileHandler)
    return logger


if __name__ == '__main__':
    # logger = creat_app_log('/home/changzuxian/project/audio-download/service/')
    # logger.info("test")
    pass

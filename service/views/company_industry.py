# -*- coding:utf-8 -*-

"""
File Name : 'company_area'.py
Description:
Author: 'weicheng'
Date: '2018/4/2' '下午2:53'
"""
from flask import Blueprint
from flask import request
from flask import jsonify
from flask_restful import Resource
from flask import current_app
import time
from service.config.setting import type_db
if type_db == "es":
    from service.models import dbtool_Elasticsearch as dbtool
else:
    from service.models import dbtool_hbase as dbtool


company_industry_entry = Blueprint('company_industry', __name__)

# 接口
class CompanyIndustry(Resource):
    def post(self):
        t1 = time.time()
        logger = current_app.app_logger
        logger.info("in post company_industry")
        result = dbtool.db_industry(logger)
        if len(result) != 0:
            res_result = {**{"result": result}}
        else:
            res_result = {**{"result": None}}
        t2 = time.time()
        logger.info("post company_industry cost time:" + str(t2-t1))
        return jsonify(res_result)

    def get(self):
        return ""
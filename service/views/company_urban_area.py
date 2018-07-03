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
from service.config.setting import type_db
if type_db == "es":
    from service.models import dbtool_Elasticsearch as dbtool
else:
    from service.models import dbtool_hbase as dbtool
company_urban_area_entry = Blueprint('company_urban_area', __name__)


# 接口
class CompanyUrbanArea(Resource):
    def post(self):
        logger = current_app.app_logger
        logger.info("in post company_urban_area")
        content = request.get_json()
        PROVINCE = content.get('PROVINCE', None)
        CITY = content.get('CITY', None)
        if CITY is not None and CITY.strip() == "":
            CITY = None
        if PROVINCE is not None and PROVINCE.strip() == "":
            PROVINCE = None

        result = dbtool.db_URBAN_AREA(PROVINCE, CITY, logger)
        if len(result) != 0:
            res_result = {**{"result": result}}
        else:
            res_result = {**{"result": None}}
        return jsonify(res_result)

    def get(self):
        return ""
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

company_detail_entry = Blueprint('company_detail', __name__)


# 接口
class CompanyDetail(Resource):
    def post(self):
        logger = current_app.app_logger
        logger.info("begin Post CompanyDetail")
        content = request.get_json()
        try:
            company_id_list = content['company_id']
            company_id_list = [str(x) for x in company_id_list if x.isdigit()]
            logger.info("request id_list=" + str(company_id_list))
            res_result = dbtool.db_company_detail(company_id_list, logger=logger)
        except Exception as e:
            logger.error("ERROR Post CompanyDetail e=" + str(e))
            res_result = {}
        return jsonify(res_result)

    def get(self):
        return ""
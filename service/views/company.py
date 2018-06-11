from flask import Blueprint
from flask import request
from flask import jsonify
from flask_restful import Resource
from flask import current_app
from service.models import dbtool_hbase as dbtool
import time
company_entry = Blueprint('company', __name__)


# 接口
class CompanyApi(Resource):
    def post(self):
        tres_result1 = time.time()
        logger = current_app.app_logger
        # logger.info("in post company_api")
        content = request.get_json()
        # logger.info("request content"+str(content))

        page = content.get('page', 1)
        per_page = content.get('per_page', 10)
        startn = content.get('startn', 0)
        quick = content.get('quick', None)
        startid = content.get('startid', None)

        try:
            content.pop('page')
        except Exception as e:
            # logger.info("request content not have same field:" + str(e))
            pass
        try:
            content.pop('per_page')
        except Exception as e:
            # logger.info("request content not have same field:" + str(e))
            pass

        try:
            content.pop('startn')
        except Exception as e:
            # logger.info("request content not have same field:" + str(e))
            pass
        try:
            content.pop('quick')
        except Exception as e:
            # logger.info("request content not have same field:" + str(e))
            pass

        try:
            content.pop('startid')
        except Exception as e:
            # logger.info("request content not have same field:" + str(e))
            pass

        if str(startn).isdigit():
            startn = int(startn)
        else:
            startn = 0

        if str(page).isdigit():
            page = int(page)
        else:
            page = 1

        if str(per_page).isdigit():
            per_page = int(per_page)
        else:
            per_page = 1


        if str(startid).isdigit():
            startid = int(startid)
        else:
            startid = 0



        result, all_n = dbtool.db_search(content, page, per_page, startn, quick, startid, logger=logger)
        if len(result) != 0:
            res_result = {**{"result": result}, **{"total_num": all_n}}
        else:
            res_result = {**{"result": None}, **{"total_num": all_n}}
        tres_result2 = time.time()
        logger.info("post total 耗时" + str(tres_result2-tres_result1))
        return jsonify(res_result)

    def get(self):
        return ""
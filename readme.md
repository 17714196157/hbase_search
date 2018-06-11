#接口说明文档
## 依赖库
jaydebeapi
phoenixdb
json
pandas
flask
redis

## 数据库字段说明
hbase字段名	hbase列族	index_table
ID    rowkey    FALSE
PROVINCE	rowkey	FALSE
CITY	rowkey	FALSE
REG_CAPITAL	rowkey	FALSE
REG_TIME	A	TRUE
URBAN_AREA	A	FALSE
NAME	A	TRUE
TEL	A	TRUE
INDUSTRY	A	TRUE
LAGEL_PERSON	A	FALSE
REGISTER_CAPITAL	A	FALSE
REGISTER_TIME	A	FALSE
ADDRESS	A	FALSE
EMAIL	A	FALSE
BUSINESS_LICENSE	A	FALSE
STATUS	A	FALSE
WEB_SOURCE  A FALSE

## company接口
请求url   http://47.98.36.72:10002/search/company
请求消息体
```python
  {
    "PROVINCE":"江苏",
    "page":"1",
    "per_page":"10000",
    "startn":"100"
}
```
理论上所以数据库字段都可以作为搜索条件；
接口开发字段如下，大小写不区分：
    page页面号不太，默认为1；
    per_page每页展示数据量，默认为10；
    startn数据返回时的起始偏移量，默认为0；
    quick字段： 判断前端是否需要返回数据总数，统计总数需要几秒时间
        1.不太字段默认需要count
        2.携带字段无论值都不返回总数；
    city是城市；
    PROVINCE是省份；
    REG_CAPITAL 企业规模 0表示未知（其他），1~5是枚举值
    REG_TIME  企业成立时间  0表示未知（其他），1~5是枚举值
    name  企业名

性能： hbase 版本 分页做的有限制 15万数据分页 时间在4s内，已到了极限
    枚举值对应说明：
    REG_TIME  = {'1': [-1, 2018],
                          '2': [2017, 2013],
                          '3': [2012, 2008],
                          '4': [2007, 2003],
                          '5': [2002, -1],
                          }

    REG_CAPITAL= {'1': [100, 0],
                          '2': [200, 100],
                          '3': [500, 200],
                          '4': [1000, 500],
                          '5': [-1,1000],
                          }

响应消息体
```python
{
  "result": [
    {
      "ADDRESS": "\u6df1\u5733\u6df1\u5733\u5e02\u7f57\u6e56\u533a\u7530\u8d1d\u56db\u8def\u5e7f\u53d1\u5927\u53a65\u697c516\u5ba4",
      "BUSINESS_LICENSE": "91440300311707097C",
      "CITY": "\u6df1\u5733",
      "EMAIL": "inlovegift@163.com",
      "ID": 1,
      "INDUSTRY": "\u73e0\u5b9d\u5de5\u827a\u54c1\u516c\u53f8",
      "LAGEL_PERSON": "\u5f20\u7389|\u738b\u521a",
      "NAME": "\u6df1\u5733\u5e02\u83f2\u8513\u73e0\u5b9d\u6709\u9650\u516c\u53f8",
      "PROVINCE": "\u5e7f\u4e1c",
      "REGISTER_CAPITAL": "500",
      "REGISTER_TIME": "2014",
      "REG_CAPITAL": "0",
      "REG_TIME": "0",
      "STATUS": "在业",
      "TEL": "\u672a\u63d0\u4f9b",
      "URBAN_AREA": "\u7f57\u6e56\u533a",
      "WEB_SOURCE": "shunqi"
    }
  ],
  "total_num": 4248946
}
```
## 请求城市列表接口
请求url  http://47.98.36.72:10002/search/company_city
请求消息体
```python
  {
    "PROVINCE":"江苏"
}
```

## 请求街道区域列表接口
请求url  http://47.98.36.72:10002/search/company_urban_area
请求消息体
```python
  {
    "PROVINCE":"江苏","CITY":"南京"
}
```



## company_detail接口
请求url   http://47.98.36.72:10002/search/company_detail
请求消息体
```python
{"company_id":["2362193", "2483051", "604871"]}
```

响应消息体
```python
{
  "2362193": {
    "area": "浙江",
    "company_id": 483135,
    "lagel_person": "刘兵",
    "name": "义乌华邮科技园投资开发有限公司",
    "register_capital": "10000.000000万人民币",
    "register_time": "2016-10-13",
    "tel": "0579-85603777",
    "update_time": "2018-04-03"
  },
  "2483051": {
    "area": "浙江",
    "company_id": 827932,
    "lagel_person": "明安龙",
    "name": "义乌华邮信息文化研究院有限公司",
    "register_capital": "1000.000000万人民币",
    "register_time": "2015-11-23",
    "tel": "0579-85603777",
    "update_time": "2018-04-03"
  },
  "604871": {
    "area": "浙江",
    "company_id": 1595405,
    "lagel_person": "殷志远",
    "name": "义乌华邮资产运营管理有限公司",
    "register_capital": "1000.000000万人民币",
    "register_time": "2016-10-14",
    "tel": "18678808058",
    "update_time": "2018-04-03"
  }
}
```


## company_area接口
请求url   http://47.98.36.72:10002/search/company_area
请求消息体
```python
```

响应消息体
```python
{
  "result": [
    "云南",
    "内蒙古",
    "吉林",
    "四川",
    "宁夏",
    "安徽",
    "山东",
    "山西",
    "广东",
    "广西",
    "新疆",
    "江苏",
    "江西",
    "河北",
    "河南",
    "浙江",
    "海南",
    "湖北",
    "湖南",
    "甘肃",
    "福建",
    "西藏",
    "贵州",
    "辽宁",
    "陕西",
    "青海",
    "黑龙江"
  ]
}
```

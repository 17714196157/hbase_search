import requests
import time
from concurrent.futures import  ThreadPoolExecutor

def run(data):
    # data  = {"reg_capital": "0","page":"{}".format(i),"per_page":"1000"}
    print(data)
    response = requests.post(url="http://localhost:10102/search/company", json=data)
    response.encoding = "utf-8"
    print(response.status_code)

executor = ThreadPoolExecutor(15)
future_list = []
for i in range(500, 1000):
    data = {"reg_capital": "0", "page": "{}".format(i), "per_page": "3000"}
    future = executor.submit(run, data)
    future_list.append(future)

all_n = len(future_list)
n = 0
while n != all_n:
    n = 0
    for future in future_list:
        if future.done():
            n = n+1
    time.sleep(3)
    schedule = int(n*100/all_n)
    print("schedule {}".format(schedule))

executor.shutdown()
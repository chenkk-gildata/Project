import json
import random
import time
import pandas as pd

import requests

gsdm = '001220'

base_url = "https://eipo.szse.cn/api/report/ShowReport/data"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/141.0.0.0',
    'Referer': 'https://www.szse.cn/',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive'
}
datas = []
for page in range(0,250):
    get_url = f"?SHOWTYPE=JSON&CATALOGID=1906_ipoxjgk_2_snapshot&TABKEY=tab1&txtJCorDH={gsdm}&PAGENO={page}"
    url = base_url + get_url
    response = requests.get(base_url+get_url, headers=headers)
    text = json.loads(response.text)
    pagecount = text[0]['metadata']['pagecount']
    for data in text[0]['data']:
        datas.append(data)
    print(f"采集成功 第 {page} / {pagecount} 页！")
    if page <= pagecount:
        page += 1
        time.sleep(random.randint(5, 10))
    else:
        break

if datas:
    df = pd.DataFrame(datas)
    df.to_excel(f"{gsdm}.xlsx", index=False)




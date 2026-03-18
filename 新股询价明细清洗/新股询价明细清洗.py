import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

excel_path = "新股询价明细清洗.xlsx"
base_url = "https://eipo.szse.cn/api/report/ShowReport/data"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/141.0.0.0',
    'Referer': 'https://www.szse.cn/',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive'
}

def process_row(result):
    gsdm = result[1]
    tzzmc = result[2]
    psdxmc = result[3]
    sbjg = result[4]
    get_url = f"?SHOWTYPE=JSON&CATALOGID=1906_ipoxjgk_2&TABKEY=tab2&txtJCorDH={gsdm}&txttzz={tzzmc}&txttzzpzdx={psdxmc}"
    url = base_url+get_url
    try:
        response = requests.get(base_url+get_url, headers=headers)
        text = json.loads(response.text)
        url_values = text[0]["data"]
        if url_values:
            print(f"{url} - 返回成功！")
            row_results = []
            for value in url_values:
                if value["gdmc"] == tzzmc and value["psdxmc"] == psdxmc:
                    if float(value["sbjg"]) != float(sbjg):
                        # 添加GPDM字段
                        value_with_gpdm = dict(value)
                        value_with_gpdm["GPDM"] = gsdm
                        row_results.append(value_with_gpdm)
            time.sleep(random.randint(5,10))
            return row_results
        else:
            return []
    except Exception as e:
        print(f"处理 {gsdm} 时出错: {str(e)}")
        time.sleep(random.randint(5,10))
        return []  # 返回空列表而不是异常对象

#读取清洗excel里的内容
results = pd.read_excel(excel_path,"Sheet2", dtype=str)
excel_out = []

# 使用线程池并发处理，最大并发数为3
with ThreadPoolExecutor(max_workers=3) as executor:
    # 提交所有任务
    future_to_row = {executor.submit(process_row, row): row for row in results.itertuples()}
    
    # 获取结果
    for future in as_completed(future_to_row):
        row_results = future.result()
        excel_out.extend(row_results)

if excel_out:
    df = pd.DataFrame(excel_out)
    # 确保GPDM列在第一列位置
    if "GPDM" in df.columns:
        # 获取所有列名
        cols = df.columns.tolist()
        # 移除GPDM列，然后将其插入到第一位置
        cols.remove("GPDM")
        cols = ["GPDM"] + cols
        df = df[cols]
    df.to_excel("report.xlsx", index=False)




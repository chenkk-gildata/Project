#  周一到周五，7:00、12:30、15:40、23:00

import pymysql
from datetime import datetime, timedelta
import requests
import json


# 连接数据库
def mysql_query():
    conn = pymysql.connect(host="10.105.0.100", user="datacenter", password="Datacenter#1", database="task_platform")
    if conn:
        print("Connected to MySQL")
    cursor = conn.cursor()
    sql = """
        select a.name
        # , b.product_status
             , date(b.release_date)
        , count(*) num
        from task_pool_config a
        join task_fixed_time_job_20250611 b on a.id = b.pool_config_id
        and locate('定时任务-权益数据部-新股组', a.combination_name) = 1
        where a.is_task_node = 1 and a.pool_task_type = 3
        and a.is_effect = 1 -- and a.is_check_inside = 1
        
        -- 当日 2024-09-29， 从09-26 到 09-28
        and date(b.release_date) >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
        and date(b.release_date) <= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        and TIMESTAMPDIFF(MINUTE, release_date, now()) >= 30
        
        and b.product_status in (1, 2, 7)
        -- 生产状态：1待处理、2处理中、3已发布、4不处理、7待修改 具体见数据字典
        group by a.name, date(b.release_date)
    ;
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print('Query executed successfully!')

    return result


# 查询结果转换
def content_trans(result):
    data = []
    if result:

        for i in result:
            data.append(
                {
                    'task_pool_name': i[0],
                    'task_pool_date': i[1],
                    'task_pool_num': i[2]
                }
            )

    return data


# 消息转换
def msg_trans(result):
    start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    end_time = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if result:
        header = '## <font color=#FF7F24>新股组-定时任务3日内未完成且超时30分钟提醒</font>\n'
    else:
        header = '## <font color=#63B8FF>新股组-定时任务3日内未完成且超时30分钟提醒</font>\n'
    sub_header = f'> 区间：{start_time} 至 {end_time}\n'
    body = ''
    count_pool = len(result)
    count_task = 0
    if result:
        for i in result:
            task_pool_name = i['task_pool_name']
            # task_pool_status = i['task_pool_status']
            task_pool_num = i['task_pool_num']
            task_pool_date = i['task_pool_date']
            count_task += task_pool_num
            body += f'+  {task_pool_date}, 【{task_pool_name}】, 剩 {task_pool_num} 条。\n'
    else:
        body += '+  暂无异常\n'
    if result:
        footer = f'>  <font color=#FF7F24>任务池数: **{count_pool}** ; 未完成任务量： **{count_task}**</font>'
    else:
        footer = f'>  <font color=#63B8FF>任务池数: {count_pool} ; 未完成任务量： {count_task}</font>'

    msg = f'{header}{sub_header}{body}{footer}'

    return msg


# dingding
def dingding(message):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6'  # test
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=4c99871c2b31daeefd8b1ad463bc4e3da949cd8dd82b231b2da26cad49280e70'  # 特别
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=80eb142db1d2b17ad952536fdc5d8582d2eea8866c2a06e8dce6369e6c5669bb'  # 新特别
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=4297446269879a5d72671f5c14a647aaa0f25e58a35c64e7470ec12ea29f472c'  # 新股组
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '定时任务监控',
            'text': message,
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功！")
    else:
        print("消息发送失败！")


if __name__ == '__main__':
    result = mysql_query()
    data = content_trans(result)
    msg = msg_trans(data)
    dingding(msg)

## 港股组个人任务池
import pymysql
from datetime import datetime
import requests
import json


# 查询任务中心
def tp_check():
    conn = pymysql.connect(host="10.105.0.100", user="datacenter", password="Datacenter#1", database="task_platform")
    cursor = conn.cursor()
    sql = """
          select
                c.real_name
                ,b.name
                ,count(a.id) as count_num
                 ,concat(
                        max(timestampdiff(MINUTE , a.update_time, now())) DIV 60,
                '小时',
                        max(timestampdiff(MINUTE , a.update_time, now())) MOD 60,
                    '分钟') as max_delay
                from task_job_20241013 a
                join task_pool_config b on a.pool_config_id = b.id and b.id in (
                    1549267881756315649,
                    1549268354819186690,
                    1549272563430051841,
                    1662987538991730689,
                    1662987714482806785,
                    1662987736858021890,
                    1662990248373702658,
                    1705410991943462914,
                    1782242909324083201,
                    1797896193936142338
                    )
                join task_user c on a.product_user = c.user_name
                where a.product_status in (2, 7)  -- 生产状态：1待处理、2处理中、3已发布、4不处理、7待修改 具体见数据字典 
                and a.is_delete = 0
                group by a.product_user, b.name
                order by count_num desc;
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


# 转字典列表
def result_to_dict(tp_result):
    matched_results = []
    if tp_result:
        for tp in tp_result:
            matched_results.append(
                {
                    'product_user': tp[0],
                    'task_pool': tp[1],
                    'task_count': tp[2],
                    'task_delay': tp[3]
                }
            )
    return matched_results


def dingding(message):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6' # test
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=61d6e7f8a1762f611256121d59c9fcf44af577981ddc4c176eced5861e14aff5'  # 港股
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '任务池个人任务',
            'text': message,
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功！")
    else:
        print("消息发送失败！")


# markdown转换
def message_trans(data):
    print(data)
    date = datetime.now().strftime('%Y-%m-%d %H:%M')
    if data:
        header = '## <font color=#ff6700>港股个人任务领取留存时间异常</font>\n'
    else:
        header = '## <font color=#00BFFF>港股个人任务领取留存时间异常</font>\n'
    sub_header = f'>  执行时间： {date}\n'
    count_staff = len(data)
    count_task = 0
    body = ''
    if data:

        for i in data:
            user = i['product_user']
            task_pool = i['task_pool']
            task_count = i['task_count']
            task_delay = i['task_delay']
            count_task += task_count
            body += (f'+  **<font color=#FF3030>{user}</font>**, 在【{task_pool}】中, 有<font color=#FF6347> **{task_count}** </font>条任务留存, '
                     f'最长留存<font color=#FF6347> **{task_delay}** </font>。\n')
    else:
        body += '+  暂无异常\n'

    footer = f'>  异常人次:<font color=#FF0000> **{count_staff}** </font>; 异常任务量：<font color=#FF0000> **{count_task}** </font>'
    msg = f'{header}{sub_header}{body}{footer}'

    return msg


if __name__ == '__main__':
    a = tp_check()
    jyp_result = result_to_dict(a)
    msg = message_trans(jyp_result)
    dingding(msg)
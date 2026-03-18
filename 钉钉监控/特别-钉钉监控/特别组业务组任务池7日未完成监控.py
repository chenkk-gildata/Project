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
        with a as (select a.name
                , b.product_status
                , count(b.job_unique_id) num
           from task_pool_config a
                    join task_job_20241013 b on a.id = b.pool_config_id
               and locate('生产任务-股票任务', a.combination_name) = 1
               and a.is_effect = 1 -- and a.is_check_inside = 1
               and (
                    locate('生产任务-股票任务-A股特别岗-衍生', a.combination_name) = 1
                        or
                    locate('生产任务-股票任务-A股特别岗-特别', a.combination_name) = 1
                    )
               and b.create_time > date_add(now(), interval -7 day)
               and b.product_status in (1, 7)
           -- 生产状态：1待处理、2处理中、3已发布、4不处理、7待修改 具体见数据字典
           group by a.name, b.product_status
        )
        , b as (
            select a.name
            , b.item_value status
            , a.num
            from a
                join data_dict b on a.product_status = b.item_code and b.dict_value = '生产流程状态'
        )
        , c as (
            select concat('{挑表任务池}', a.name) name
            , b.product_status
            , count(b.job_unique_id) num
            from task_pool_config a
            join task_pick_table_job_20240301 b on a.id = b.pool_config_id
            and a.id in  (1777525914654584833,1831839314843467777)  # 1825503727278559233,
            and b.is_delete = 0 and b.create_time > date_add(now(), interval -7 day)
                       and b.product_status in (1, 7)
            group by a.name, b.product_status
        )
        select b.name,
               b.num
        from b
        union
        select
            c.name, c.num
        from c
        group by name
        order by num desc;
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
                    # 'task_pool_status': i[1],
                    'task_pool_num': i[1]
                }
            )

    return data


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

# 消息转换
def msg_trans(result):
    start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M')
    end_time = (datetime.now() - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M')
    if result:
        header = '## <font color=#FF7F24>特别组任务池7日内任务未完成提醒</font>\n'
    else:
        header = '## <font color=#63B8FF>特别组任务池7日内任务未完成提醒</font>\n'
    sub_header = f'> 区间：{start_time} 至 {end_time}\n'
    body = ''
    count_pool = len(result)
    count_task = 0
    if result:
        for i in result:
            task_pool_name = i['task_pool_name']
            # task_pool_status = i['task_pool_status']
            task_pool_num = i['task_pool_num']
            count_task += task_pool_num
            body += f'+  【{task_pool_name}】,剩 {task_pool_num} 条。\n'
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
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=3050a412c9039d5d3471f64b9b6b23463d6c79bc5cb6fe217ea915542855cf8c'  # test.md
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=4c99871c2b31daeefd8b1ad463bc4e3da949cd8dd82b231b2da26cad49280e70'  # 特别
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=80eb142db1d2b17ad952536fdc5d8582d2eea8866c2a06e8dce6369e6c5669bb'  # 新特别
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '任务池业务组任务',
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

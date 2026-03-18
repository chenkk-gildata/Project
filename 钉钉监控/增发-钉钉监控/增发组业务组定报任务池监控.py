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
        /*生产状态：1待处理、2处理中、3已发布、4不处理、7待修改*/
        with t1 as (
        select
            a.name
            , sum(date(b.release_date) = date(now())) as today_total_created
            , sum((date(b.release_date) = date(now())) and (b.product_status in (3, 4))) today_total_done
            , sum((date(b.release_date) = date(now())) and (b.product_status = 1)) today_total_untreated
            , sum((date(b.release_date) = date(now())) and (b.product_status in (2, 7))) today_total_processing
        from task_pool_config a
        join task_job_20241013 b on a.id = b.pool_config_id
        and a.pid = 1562313453559910402 -- 增发定报父节点
        and a.is_effect = 1
        and a.is_delete = 0
        and a.is_task_node = 1
        and b.create_time >= timestampadd(day, -3, date(now()))
        group by a.name
        )
        , t2 as (
        select
            a.name
            , count(*) 3_day_total_untreated
        from task_pool_config a
        join task_job_20241013 b on a.id = b.pool_config_id
        and a.pid = 1562313453559910402 -- 增发定报父节点
        and a.is_effect = 1
        and a.is_delete = 0
        and a.is_task_node = 1
        and b.create_time >= '2024-03-01' and b.product_status = 1 and b.create_time <= date(timestampadd(day, -2, now()))
        group by a.name
        )
        select
            t1.name
            , t1.today_total_created
            , t1.today_total_done
            , t1.today_total_processing
            , t1.today_total_untreated
            , ifnull(t2.3_day_total_untreated, 0)
        from t1
        left join t2 on t1.name = t2.name
        order by t1.name
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
                    'today_total_created': i[1],
                    'today_total_done': i[2],
                    'today_total_untreated': i[3],
                    'today_total_processing': i[4],
                    'three_day_total_untreated': i[5]
                }
            )

    return data


# 消息转换
def msg_trans(result):
    # start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M')
    end_time = (datetime.now() - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M')
    # now = datetime.now()
    if result:
        header = '## <font color=#FF7F24>增发组定报任务完量及完成情况</font>\n'
    else:
        header = '## <font color=#63B8FF>增发组定报任务完量及完成情况</font>\n'
    sub_header = f'> 提取时间截止：{end_time}\n'
    body = ''
    # count_pool = len(result)
    footer = ''
    total_created = 0
    total_done = 0
    total_untreated = 0
    total_processing = 0
    t3_total_untreated = 0
    if result:
        for i in result:
            task_pool_name = i['task_pool_name']
            today_total_created = i['today_total_created']
            today_total_done = i['today_total_done']
            today_total_untreated = i['today_total_untreated']
            today_total_processing = i['today_total_processing']
            three_day_total_untreated = i['three_day_total_untreated']

            total_created += today_total_created
            total_done += today_total_done
            total_untreated += today_total_untreated
            total_processing += today_total_processing
            t3_total_untreated += three_day_total_untreated

            body += (f'+  【{task_pool_name}】\n\n当日任务量：{today_total_created},已完成：{today_total_done},未完成：{today_total_untreated},处理中：{today_total_processing}')
            if three_day_total_untreated:
                body += (f'\n\n>  <font color=#FF0000>**超3天未完成量：{three_day_total_untreated}**</font>\n\n')
            else:
                body += (f'\n\n>  超3天未完成量：{three_day_total_untreated}\n\n')
    else:
        body += '+  暂无异常\n'
    if result:
        footer = (f'***   \n\n>  <font color=#FF7F24>**合计：当日生成任务总量： {total_created}**</font>\n\n'
                  f'>  <font color=#FF7F24>**合计：当日完成任务总量： {total_done}**</font>\n\n'
                  f'>  <font color=#FF7F24>**合计：当日未完成任务总量： {total_untreated}**</font>\n\n'
                  f'>  <font color=#FF7F24>**合计：当日处理中任务总量： {total_processing}**</font>\n\n'
                  f'>  <font color=#FF0000>**合计：超3日未完成任务总量： {t3_total_untreated}**</font>\n\n@18801667559')
    else:
        footer = (f'***   \n\n>  <font color=#FF0000>合计：超3日未完成任务总量： {t3_total_untreated}</font>'
                  f'\n\n @18801667559')

    msg = f'{header}{sub_header}{body}{footer}'

    return msg


# dingding
def dingding(message):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6'  # test
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=c2ddf6b2532e6ee1414277971981f7c2cdfaf3e26c7264b71552aa0a861ab75b' # 增发组
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '任务池业务组任务',
            'text': message,
        },
        "at": {
            "atMobiles": [18801667559],
        },
        "isAtAll": False
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

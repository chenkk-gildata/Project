## 特别组个人任务池
import pymysql
import pymssql
from datetime import datetime
from apscheduler.schedulers.background import BlockingScheduler
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
        join task_pool_config b on a.pool_config_id = b.id and (locate('生产任务-股票任务-特别组-衍生', b.combination_name ) = 1 or 
        locate('生产任务-股票任务-特别组-特别', b.combination_name ) = 1)
        join task_user c on a.product_user = c.user_name
        where a.product_status = 2
            and a.create_time > date_add(now(), interval -7 day )
            and a.update_time > date_add(now(), interval -4 day )
        group by a.product_user, b.name
        having max(timestampdiff(MINUTE , a.update_time, now())) >= 15
        union
        select
            c.real_name
            ,b.name
            ,count(a.id) as count_num
             ,concat(
                    max(timestampdiff(MINUTE , a.update_time, now())) DIV 60,
            '小时',
                    max(timestampdiff(MINUTE , a.update_time, now())) MOD 60,
                '分钟') as max_delay
            from task_pick_table_job_20240301 a
            join task_pool_config b on a.pool_config_id = b.id and b.id in (1777525914654584833,1825503727278559233,1831839314843467777)
            join task_user c on a.product_user = c.user_name
            where a.product_status = 2
                and a.create_time > date_add(now(), interval -7 day )
                and a.update_time > date_add(now(), interval -4 day )
            group by a.product_user, b.name
            having max(timestampdiff(MINUTE , a.update_time, now())) >= 15
            order by count_num desc
        ;"""
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


# jyp信息查询
def jyp_check():
    conn = pymssql.connect(host='10.106.22.51', user='GILDATA\\chenad', password='Andy01404@@@', database='JYPRIME')
    cursor = conn.cursor()
    sql = """
    select XM, NBJLZH
    from usrSJCBYGZLB
    where YWZB = 149 and SFZZ = 1 and LCYWG = 1001
    """
    cursor.execute(sql)
    result = cursor.fetchall()

    cursor.close()
    conn.close()
    return result


# 结果匹配
def staff_match(tp_result, jyp_result):
    matched_results = []
    if tp_result:
        for tp in tp_result:
            # print(tp)
            for jyp in jyp_result:
                if tp[0] == jyp[1]:
                    matched_results.append(
                        {
                            'product_user': jyp[0].encode('latin-1').decode('gbk'),
                            'task_pool': tp[1],
                            'task_count': tp[2],
                            'task_delay': tp[3]
                        }
                    )
    return matched_results


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



# 调度执行
def schedule_task():
    scheduler = BlockingScheduler()
    scheduler.add_job(tp_check, 'cron', day_of_week='mon-fri', hour='7-24', minute='0', second='0')
    scheduler.start()


def dingding(message):
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=3050a412c9039d5d3471f64b9b6b23463d6c79bc5cb6fe217ea915542855cf8c' # test.md
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=80eb142db1d2b17ad952536fdc5d8582d2eea8866c2a06e8dce6369e6c5669bb'  # 特别
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
        header = '## <font color=#ff6700>个人任务领取留存时间异常(15分钟)</font>\n'
    else:
        header = '## <font color=#00BFFF>个人任务领取留存时间异常(15分钟)</font>\n'
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
            body += f'+  {user}, 在【{task_pool}】中, 有 {task_count} 条任务留存, 最长留存 {task_delay}。\n'
    else:
        body += '+  暂无异常\n'

    footer = f'>  异常人次: {count_staff}; 异常任务量： {count_task}'
    msg = f'{header}{sub_header}{body}{footer}'

    return msg


if __name__ == '__main__':
    a = tp_check()
    # b = jyp_check()
    # jyp_result = staff_match(a, b)
    jyp_result = result_to_dict(a)
    msg = message_trans(jyp_result)
    dingding(msg)
import pymysql
import pymssql
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
        from task_job_20210422 a
        join task_pool_config b on a.pool_config_id = b.id and locate('生产任务-股票任务-A股增发岗', b.combination_name ) = 1
        join task_user c on a.product_user = c.user_name
        where a.product_status = 2
            and a.create_time > date_add(now(), interval -7 day )
            and a.update_time > date_add(now(), interval -4 day )
        group by c.real_name, b.name
        having max(timestampdiff(MINUTE , a.update_time, now())) >= 60
        
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
        from task_job_20241013 a
        join task_pool_config b on a.pool_config_id = b.id and locate('生产任务-股票任务-A股增发岗', b.combination_name ) = 1
        join task_user c on a.product_user = c.user_name
        where a.product_status = 2
            and a.create_time > date_add(now(), interval -7 day )
            and a.update_time > date_add(now(), interval -4 day )
        group by c.real_name, b.name
        having max(timestampdiff(MINUTE , a.update_time, now())) >= 60
        
        -- order by count_num desc, max(timestampdiff(MINUTE , a.update_time, now())) desc
        ;
        """
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
    where YWZB = 149 and SFZZ = 1   and LCYWG = 1003
    """
    cursor.execute(sql)
    result = cursor.fetchall()

    cursor.close()
    conn.close()
    return result

# 本地数据库
def local_check():
    conn = pymysql.connect(host="127.0.0.1", user="root", password="Andy01404", database="equity_department")
    cursor = conn.cursor()
    sql = 'select 任务池 task_name, 任务时效 task_time from zengfa_task_pool_timely'
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    return result


# tp jyp local结果匹配
def staff_match(tp_result, jyp_result, local_result):
    matched_results = []
    # if tp_result:
    #     for tp in tp_result:
    #         for jyp in jyp_result:
    #             if tp[0] == jyp[1]:
    #                 matched_results.append(
    #                     {
    #                         'product_user': jyp[0].encode('latin-1').decode('gbk'),
    #                         'task_pool': tp[1],
    #                         'task_count': tp[2],
    #                         'task_delay': tp[3]
    #                     }
    #                 )
    if tp_result:
        for tp in tp_result:
            # for jyp in jyp_result:
            #     if tp[0] == jyp[1]:
            matched_results.append(
                {
                    'product_user': tp[0],
                    'task_pool': tp[1],
                    'task_count': tp[2],
                    'task_delay': tp[3]
                }
            )
    if matched_results:
        for matched in matched_results:
            matched['pool_time'] = ''
            for local in local_result:
                if matched['task_pool'] == local[0]:
                    matched['pool_time'] = local[1]

    return matched_results


def dingding(message):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6'
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=c2ddf6b2532e6ee1414277971981f7c2cdfaf3e26c7264b71552aa0a861ab75b' # 增发组
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
    date = datetime.now().strftime('%Y-%m-%d %H:%M')
    if data:
        header = '## <font color=#ff6700>个人任务领取留存时间异常(60分钟)</font>\n'
    else:
        header = '## <font color=#00BFFF>个人任务领取留存时间异常(60分钟)</font>\n'
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
            if i['pool_time']:
                t = i['pool_time']
                pool_time = f'\n+ + <font color=#FF7F50>时效要求： {t}</font>'
            else:
                pool_time = ''
            count_task += task_count
            body += (f'+  {user}, 在【{task_pool}】中, 有 <font color=#FF6347>{task_count}</font> 条任务留存, '
                     f'最长留存 <font color=#FF6347>{task_delay}</font>。{pool_time}\n')
    else:
        body += '+  暂无异常\n'

    footer = f'>  异常人次: {count_staff}; 异常任务量： {count_task}'
    msg = f'{header}{sub_header}{body}{footer}'
    # print(msg)
    return msg


if __name__ == '__main__':
    a = tp_check()
    # b = jyp_check()
    c = local_check()
    jyp_result = staff_match(a, None, c)
    msg = message_trans(jyp_result)
    dingding(msg)

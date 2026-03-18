import pymssql
from datetime import datetime
import requests
import json


# 组错误情况
def team_errors_query():
    '''
    :return: result1 = 离职人员,5,4
    result2 =
    '''
    # 链接数据库
    conn = pymssql.connect(host="10.102.25.11",port='8080', user="WebResourceNew_Read", password="New_45ted", database="JYPLE")
    cursor = conn.cursor()

    sql1 = """
        SELECT
        A.YWRY AS '责任人员',
        SUM(CASE
            WHEN A.GGRQ > '2023-10-01' THEN 1
            ELSE 0
        END) AS '日增错误数量',
        SUM(CASE
            WHEN A.GGRQ <= '2023-10-01' THEN 1
            ELSE 0
        END) AS '历史错误数量'
        , CAST(GETDATE() - 1 AS date) as last
        FROM
            [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB A
        WHERE
            A.YWX = '港股'
        AND A.JHRQ >= CONCAT(DATEPART(YEAR, DATEADD(MONTH, -1 , GETDATE())), '-' ,DATEPART(MONTH, DATEADD(MONTH, -1 , GETDATE())), '-01')
        AND A.JHRQ < CONCAT(DATEPART(YEAR, GETDATE()), '-' ,DATEPART(MONTH, GETDATE()), '-01')
        GROUP BY
        YWRY;
    """

    cursor.execute(sql1)
    result1 = cursor.fetchall()


    sql4 = """
    select convert(date,CONCAT(DATEPART(YEAR, DATEADD(MONTH, -1 , GETDATE())), '-' ,DATEPART(MONTH, DATEADD(MONTH, -1 , GETDATE())), '-01')) '上月第一天' -- 上月第一天
    , convert(date, dateadd(day ,-1,CONCAT(DATEPART(YEAR, GETDATE()), '-' ,DATEPART(MONTH, GETDATE()), '-01'))) '上月最后天' -- 上月最后天
    """
    cursor.execute(sql4)
    result4 = cursor.fetchall()

    cursor.close()
    conn.close()

    return result1, result4

# 消息转换
def message_trans(result):
    sql_result1 = result[0]
    sql_result2 = result[1]

    date = datetime.now().strftime('%Y-%m-%d %H:%M')
    sub_header = f'>  执行时间： {date}\n'
    body = ''

    if sql_result1:
        header1 = '## <font color=#3A5FCD>港美组-上月错误情况统计</font>\n'
        body += header1
        body += f'>  <font color=#3A5FCD>统计区间： {sql_result2[0][0]} 至 {sql_result2[0][1]}</font>\n'
        his_calc = 0
        new_calc = 0

        for i in sql_result1:
            staff = i[0].encode('latin-1').decode('gbk')
            record_daily = i[1]
            record_his = i[2]
            his_calc += record_his
            new_calc += record_daily
            body += (f'+  {staff}, 日增 <font color=#FF6347>{record_daily}</font> 条, 历史 <font color=#FF6347>{record_his}</font> 条\n')

        body += f'\n<font color=#3A5FCD>【合计】昨日日增错误：{new_calc}条；历史错误：{his_calc}条</font>\n'
        body += '>  <font color=#838B83>点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list) </font>\n\n'
        body += f'>  <font color=#838B83>执行时间： {date}</font>\n'
        body += '## <font color=#3A5FCD>打的赢，霸得蛮，高质量发展！</font>\n'

    else:
        header1 = '## <font color=#3A5FCD>港美组-上月错误情况统计</font>\n'
        body += header1
        body += f'>  <font color=#3A5FCD>统计日期： {sql_result1[0][-1]}</font>\n'
        body += '+  “有则改之，无则加勉。” ——宋·朱熹《论语集注·学而篇第一》\n\n '
        body += '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list) \n\n'
        body += f'>  执行时间： {date}\n'
        body += '## <font color=#3A5FCD>打的赢，霸得蛮，高质量发展！</font>\n'


    # body += sub_header

    #     body += '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list)'
    # else:
    #     body += '+  “人非圣贤，孰能无过？过而能改，善莫大焉。” ——《论语》\n\n '
    #     body += '>  点击跳转至平台：[填报皆已完成](http://10.6.1.131/webDataProduction/incorrectReport/list)'


    msg = f'{body}' + '\n\n'
    # msg += '\n\n## <font color=	#1E90FF>芯动数据，质造未来！</font>\n\n##  <font color=	#1E90FF>抓住每条数据，管好每项质量！</font>\n'

    return msg

# 钉钉
def dingding(msg):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6' # 测试
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=4297446269879a5d72671f5c14a647aaa0f25e58a35c64e7470ec12ea29f472c'  # 新股组
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=d6c7b38861b83b71da4c4be43bd714f7cd45a4e31071736fbbfbd3492fdde468'  # 增发组
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=a667a1c67e207f98e95b8b2ac96fc0359babfc8f6029a84fc49944b3dbd06327'  # 港美组
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '数据质量-月度稽核错误数据分析',
            'text': msg,
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功！")
    else:
        print("消息发送失败！")


# 判断是否是当月最早工作日
def workday_decide():
    # 链接数据库
    conn = pymssql.connect(host="10.102.0.99", user="WebResourceNew_Read", password="New_45ted", database="JYPLE")
    cursor = conn.cursor()
    sql = """
        select
        convert(date, min(RQ)) 'min_workday'
        , convert(date, getdate()) 'today'
        , case when convert(date, getdate()) = convert(date, min(RQ)) then 'yes'
        else 'not' end as 'flag'
        from [10.106.22.51].JYPRIME.dbo.usrQQGZRB
        where RQ between  '2024-12-01' and '2024-12-31'
        and SFGZR = 1
        and GJHDQDM = 144000000
    """
    cursor.execute(sql)
    result1 = cursor.fetchall()
    cursor.close()
    conn.close()

    if result1[0][2].encode('latin-1').decode('gbk') == 'yes':
        return True
    else:
        return False


if __name__ == '__main__':

    flag = workday_decide()
    # flag = True
    if flag:
        a = team_errors_query()
        b = message_trans(a)
        print(b)
        dingding(b)

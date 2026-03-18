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
            A.YWRY AS 责任人员,
            SUM(CASE
                WHEN SFYCS = '新增' THEN 1
                ELSE 0
            END) AS 新增,
            SUM(CASE
                WHEN SFYCS = '优化' THEN 1
                ELSE 0
            END) AS 优化,
            SUM(CASE
                WHEN SFYCS = '已有' THEN 1
                ELSE 0
            END) AS 已有,
            SUM(CASE
                WHEN SFYCS = '无' or SFYCS is null  THEN 1
                ELSE 0
            END) AS 无或未填写
        FROM
            [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB A
        WHERE
            A.YWX = 'A股-新股组'
            AND CAST(A.JHRQ AS date) between CAST(GETDATE() - 7 AS date) and CAST(GETDATE() - 1 AS date)
        GROUP BY
        YWRY
        ;
    """

    cursor.execute(sql1)
    result1 = cursor.fetchall()


    sql4 = """
    select convert(date, dateadd(day ,-datepart(weekday , getdate())+1,getdate())) 上周日 -- 上周日
    , convert(date, dateadd(day ,-datepart(weekday , getdate())-5,getdate())) 上周一 -- 上周一
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
    d1 = sql_result2[0][1].strftime('%Y-%m-%d')
    d2 = sql_result2[0][0].strftime('%Y-%m-%d')

    date = datetime.now().strftime('%Y-%m-%d %H:%M')
    sub_header = f'\n\n>  执行时间： {date}\n\n'
    body = ''

    if sql_result1:
        header1 = '## <font color=#ff6700>新股组周度改进措施落实情况</font>\n'
        body += header1
        body += f'>  统计日期：{d1} 至 {d2}\n\n'

        for i in sql_result1:
            staff = i[0].encode('latin-1').decode('gbk')
            data1 = i[1]
            data2 = i[2]
            data3 = i[3]
            data4 = i[4]

            body += (f'+  {staff}, 新增 <font color=#FF6347>{data1}</font>,'
                     f'优化 <font color=#FF6347>{data2}</font>,'
                     f'已有 <font color=#FF6347>{data3}</font>,'
                     f'无或未填<font color=#FF6347>{data4}</font>;\n')

    else:
        header1 = '## <font color=#00BFFF>新股组周度改进措施落实情况</font>\n'
        body += header1
        body += f'>  统计日期：{sql_result2[1]} 至 {sql_result2[0]}\n'
        body += '+  “有则改之，无则加勉。” ——宋·朱熹《论语集注·学而篇第一》\n\n '


    body += sub_header

    #     body += '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list)'
    # else:
    #     body += '+  “人非圣贤，孰能无过？过而能改，善莫大焉。” ——《论语》\n\n '
    #     body += '>  点击跳转至平台：[填报皆已完成](http://10.6.1.131/webDataProduction/incorrectReport/list)'


    msg = f'{body}' + '\n\n' + '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list) \n\n'
    msg += '\n\n## <font color=#1E90FF>芯动数据，质造未来！</font>\n\n##  <font color=	#1E90FF>抓住每条数据，管好每项质量！</font>\n'

    return msg

# 钉钉
def dingding(msg):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6' # 测试
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=4297446269879a5d72671f5c14a647aaa0f25e58a35c64e7470ec12ea29f472c'  # 新股组
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=d6c7b38861b83b71da4c4be43bd714f7cd45a4e31071736fbbfbd3492fdde468'  # 增发组
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '周度稽核错误任务填写数据分析',
            'text': msg,
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功！")
    else:
        print("消息发送失败！")



if __name__ == '__main__':
    a = team_errors_query()
    b = message_trans(a)
    print(b)
    dingding(b)

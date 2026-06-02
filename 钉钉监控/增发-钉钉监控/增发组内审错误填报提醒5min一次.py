import pymssql
from datetime import datetime
import requests
import json


# 组错误情况
def team_errors_query():
    '''
    :return: 离职人员,5,4
    '''
    # 链接数据库
    conn = pymssql.connect(host="10.102.25.11",port='8080', user="WebResourceNew_Read", password="New_45ted", database="JYPLE")
    cursor = conn.cursor()

    sql = """
        -- 错误未及时修正提醒
        select
            SJSJK, 
            case when c.AD is not null then c.AD else a.SJKB end SJKB, 
            CWMS, 
            PTID, 
            TZRY, 
            YWRY, 
            b.LXDH
from [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB a
    join [10.101.0.212].JYPRIME.dbo.usrSJCBYGZLB b on a.TZRY = b.XM and b.SFZZ=1
	join [10.101.0.212].JYPRIME.dbo.dscmdTABLES c on a.SJKB = c.AB
        where  (YWX = 'A股-增发组')
        and (SCSFJS is null or TBSFRK is null)
        and SJJHRQ >= '2026-01-01'
        and BZSM like '%内审%'
        --and FBSJ > DATEADD(MINUTE,-5,GETDATE())
        ;
    """
    cursor.execute(sql)
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    return result

# 消息转换
def message_trans(single_record):
    date = datetime.now().strftime('%Y-%m-%d %H:%M')

    header = '内审错误填报-消息提醒 \n\n'
    sub_header = f'【告警时间】： {date} \n\n'

    SJSJK = single_record[0].encode('latin-1').decode('gbk')
    SJKB = single_record[1].encode('latin-1').decode('gbk')
    CWMS = single_record[2].encode('latin-1').decode('gbk')
    TZRY = single_record[4].encode('latin-1').decode('gbk')
    YWRY = single_record[5].encode('latin-1').decode('gbk')
    LXDH = single_record[6].encode('latin-1').decode('gbk')
    
    reportId = single_record[3]
    
    body = f'【告警内容】：[你有一个错误待确认：{SJSJK}-{SJKB}] \n\n'
    body += f'+  错误描述：{CWMS} \n\n'
    body += f'+  跳转链接：http://10.6.1.131/webDataProduction/incorrectReport/list?reportId={reportId} \n\n'
    body += f'+  通知修改人员：{TZRY} \n\n'
    body += f'+  问题责任人：{YWRY} \n\n'
    body += f'@{LXDH} \n\n'

    msg = f'{header}{sub_header}{body}'

    return msg, LXDH

# 钉钉
def dingding(msg, phone_number):
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=3050a412c9039d5d3471f64b9b6b23463d6c79bc5cb6fe217ea915542855cf8c' # 测试
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=d6c7b38861b83b71da4c4be43bd714f7cd45a4e31071736fbbfbd3492fdde468'  # 增发组
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '每日内审错误修正提醒',
            'text': msg,
        },
        'at': {
            'atMobiles': [phone_number],
            'isAtAll': False
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功！")
    else:
        print("消息发送失败！")


if __name__ == '__main__':
    sql_results = team_errors_query()
    if sql_results:
        for record in sql_results:
            msg, phone = message_trans(record)
            print(msg)
            dingding(msg, phone)
    else:
        print("没有查询到数据")

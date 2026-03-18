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
            YWRY staff, count(ID) record
        from [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB A
        where  A.YWX='港股'  AND JHRQ>='2024-12-20'
        and A.SCSFJS is null 
        group by A.YWRY
        ;
    """
    cursor.execute(sql)
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    return result

# 消息转换
def message_trans(sql_result):
    date = datetime.now().strftime('%Y-%m-%d %H:%M')
    if sql_result:
        header = '## <font color=#FF6347>港美组-稽核错误未及时修正提醒</font>\n'
    else:
        header = '## <font color=#00BFFF>港美组-稽核错误未及时修正提醒</font>\n'
    sub_header = f'>  执行时间： {date}\n'
    count_staff = len(sql_result)
    count_task = 0
    body = ''
    if sql_result:
        for i in sql_result:
            staff = i[0].encode('latin-1').decode('gbk')
            record = i[1]
            body += (f'+  {staff}, 剩余 <font color=#FF6347>{record}</font> 条未及时修正。 \n')
        body += '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list) \n\n'
        body += '## <font color=#FF3030>打的赢，霸得蛮，高质量发展！</font>\n'
        # body += ">  144数据库查询语句： \n select YWX team, PTID from dbo.usrNBSYJHCWTB where (YWX = 'A股-特别组' or YWX = 'A股-衍生组') and (SCSFJS is null or TBSFRK is null) and SJJHRQ >= '2024-08-01'"
    else:
        body += '+  “人非圣贤，孰能无过？过而能改，善莫大焉。” ——《论语》\n\n '
        body += '>  点击跳转至平台：[填报皆已完成](http://10.6.1.131/webDataProduction/incorrectReport/list)'
        body += '## <font color=#00B2EE>打的赢，霸得蛮，高质量发展！</font>\n'


    # footer = f'>  异常人次: {count_staff}; 异常任务量： {count_task}'
    # msg = f'{header}{sub_header}{body}{footer}'
    msg = f'{header}{sub_header}{body}\n\n'

    return msg

# 钉钉
def dingding(msg):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6' # 测试
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=4c99871c2b31daeefd8b1ad463bc4e3da949cd8dd82b231b2da26cad49280e70'  # 特别组
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=b564c3ed36ddd91f76e92a1fb01af7d23aed2ca4d556700a337fc1c99f1bb7cf'  # 新特别组
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=a667a1c67e207f98e95b8b2ac96fc0359babfc8f6029a84fc49944b3dbd06327'  # 港美组
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '数据质量-每日稽核错误任务未及时修正提醒',
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

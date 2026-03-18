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
        -- 稽核错误未填写改进措施 组错误情况
        with a as ( -- 历史
            select
                YWRY staff, '历史' period, count(ID) record
            from [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB
            where  (YWX = 'A股-增发组')
            and SJJHRQ between convert(date, dateadd(day ,-datepart(weekday , getdate())-5,getdate())) -- 上周一
            and convert(date, dateadd(day ,-datepart(weekday , getdate())+1,getdate())) -- 上周日
            and SCSFJS = '是' and GGRQ < '2023-10-01'
            group by YWRY
        )
        , b as ( -- 日增
            select
                YWRY staff , '日增' period, count(ID) record
            from [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB
            where  (YWX = 'A股-增发组')
            and SJJHRQ between convert(date, dateadd(day ,-datepart(weekday , getdate())-5,getdate())) -- 上周一
            and convert(date, dateadd(day ,-datepart(weekday , getdate())+1,getdate())) -- 上周日
            and SCSFJS = '是' and GGRQ >= '2023-10-01'
            group by YWRY
        )
        select
            (case when a.staff is null then b.staff else a.staff end) as staff
        , (case when b.staff is null then 0 else b.record end) as daily_record
        , (case when a.staff is null then 0 else a.record end) as his_record
        from b
        full join a on b.staff = a.staff
        ;
    """
    cursor.execute(sql1)
    result1 = cursor.fetchall()

    sql2 = """
        -- 稽核错误未填写改进措施 无改进措施
        select
            YWRY staff , count(YWRY) record
        from [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB
        where  (YWX = 'A股-增发组')
        and SJJHRQ between convert(date, dateadd(day ,-datepart(weekday , getdate())-5,getdate()))
        and convert(date, dateadd(day ,-datepart(weekday , getdate())+1,getdate())) -- 上周日
        and SCSFJS = '是'
        and (WTFL is null or SFYBMCS is null)
        group by YWRY
        ;
    """
    cursor.execute(sql2)
    result2 = cursor.fetchall()

    sql3 = """
        -- 稽核错误未填写改进措施 无效改进措施
        select
            YWRY staff , count(YWRY) record
        from [10.101.1.144].FSCSJ.dbo.usrNBSYJHCWTB
        where  (YWX = 'A股-增发组')
        and SJJHRQ between convert(date, dateadd(day ,-datepart(weekday , getdate())-5,getdate()))
        and convert(date, dateadd(day ,-datepart(weekday , getdate())+1,getdate())) -- 上周日
        and SCSFJS = '是'
        and CSQK1 is null
        group by YWRY
        ;
    """
    cursor.execute(sql3)
    result3 = cursor.fetchall()

    sql4 = """
    select convert(date, dateadd(day ,-datepart(weekday , getdate())+1,getdate())) 上周日 -- 上周日
    , convert(date, dateadd(day ,-datepart(weekday , getdate())-5,getdate())) 上周一 -- 上周一
    """
    cursor.execute(sql4)
    result4 = cursor.fetchall()

    cursor.close()
    conn.close()

    return result1, result2, result3, result4

# 消息转换
def message_trans(result):
    sql_result1 = result[0]
    sql_result2 = result[1]
    sql_result3 = result[2]
    sql_result4 = result[3]


    date = datetime.now().strftime('%Y-%m-%d %H:%M')
    sub_header = f'>  执行时间： {date}\n'
    body = ''

    if sql_result1:
        header1 = '## <font color=#ff6700>增发组上周错误情况</font>\n'
        body += header1
        body += f'>  取值区间： {sql_result4[0][1]} 至 {sql_result4[0][0]}\n'
        his_calc = 0
        new_calc = 0

        for i in sql_result1:
            staff = i[0].encode('latin-1').decode('gbk')
            record_daily = i[1]
            record_his = i[2]
            his_calc += record_his
            new_calc += record_daily
            body += (f'+  {staff}, 日增 <font color=#FF6347>{record_daily}</font> 条, 历史 <font color=#FF6347>{record_his}</font> 条\n')

        body += f'\n<font color=#1E90FF>  【上周合计】日增错误：{new_calc}条；历史错误：{his_calc}条</font>\n'

    else:
        header1 = '## <font color=#00BFFF>增发组-上周错误情况</font>\n'
        body += header1
        body += f'>  取值区间： {sql_result4[0][1]} 至 {sql_result4[0][0]}\n'
        body += '+  “有则改之，无则加勉。” ——宋·朱熹《论语集注·学而篇第一》\n\n '

    if sql_result2:
        header2 = '## <font color=#ff6700>增发组-截止上周无改进措施</font>\n'
        body += header2
        body += f'>  截止日期：  {sql_result4[0][0]}\n'
        for i in sql_result2:
            staff = i[0].encode('latin-1').decode('gbk')
            record = i[1]
            body += (f'+  {staff}, 剩余 <font color=#FF6347>{record}</font> 条\n')
    else:
        header2 = '## <font color=#00BFFF>增发组-截止上周无改进措施</font>\n'
        body += header2
        body += f'>  截止日期：  {sql_result4[0][0]}\n'
        body += '+  “过而不能知，是不智也；知而不能改，是不勇也。” ——北宋·李觏《易论第九》\n\n '

    if sql_result3:
        header3 = '## <font color=#ff6700>增发组-截止上周无效改进措施</font>\n'
        body += header3
        body += f'>  截止日期：  {sql_result4[0][0]}\n'
        for i in sql_result3:
            staff = i[0].encode('latin-1').decode('gbk')
            record = i[1]
            body += (f'+  {staff}, 剩余 <font color=#FF6347>{record}</font> 条\n')
    else:
        header3 = '## <font color=#00BFFF>增发组-截止上周无效改进措施</font>\n'
        body += header3
        body += f'>  截止日期：  {sql_result4[0][0]}\n'
        body += '+  “人非圣贤，孰能无过，过而能改，善莫大焉。” ——《左传·宣公二年》\n\n '

    body += sub_header

    #     body += '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list)'
    # else:
    #     body += '+  “人非圣贤，孰能无过？过而能改，善莫大焉。” ——《论语》\n\n '
    #     body += '>  点击跳转至平台：[填报皆已完成](http://10.6.1.131/webDataProduction/incorrectReport/list)'


    msg = f'{body}' + '\n\n' + '>  点击跳转至平台：[WEB平台填写入口](http://10.6.1.131/webDataProduction/incorrectReport/list)'
    msg += '\n\n## <font color=#1E90FF>芯动数据，质造未来！\n ## 抓住每条数据，管好每项质量！</font>\n'

    return msg

# 钉钉
def dingding(msg):
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=94741174a9fbd352511f46dc09b0283670633d6941e49de8cf77c342e25a36b6' # 测试
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token=d6c7b38861b83b71da4c4be43bd714f7cd45a4e31071736fbbfbd3492fdde468'  # 增发组
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

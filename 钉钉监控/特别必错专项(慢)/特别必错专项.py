"""
特别必错专项(慢)
1. 从数据库中读取融资融券数据
2. 对数据进行计数
3. 发送SQL处理结果到钉钉机器人
"""

import os
import pyodbc
import requests
import json
import glob
import time
import concurrent.futures
import multiprocessing
import threading
from threading import Lock
from datetime import datetime


# 数据库连接配置
server: str = os.getenv('DB_SERVER', '10.102.25.11,8080')
username: str = os.getenv('DB_USERNAME', 'WebResourceNew_Read')
password: str = os.getenv('DB_PASSWORD', 'New_45ted')
driver: str = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')

# 钉钉机器人Webhook
dingding_webhook = "https://oapi.dingtalk.com/robot/send?access_token=b43258adac00289bd50e93bdd31a81578baad3f068bfcfd53354264801fd6bb5"
# 测试
#dingding_webhook = "https://oapi.dingtalk.com/robot/send?access_token=3050a412c9039d5d3471f64b9b6b23463d6c79bc5cb6fe217ea915542855cf8c"

# 负责人映射
responsible_persons = {
    "许豆豆": "15940201885",
    "陈鑫怡": "19802521750"
}

# 线程安全的计数器和锁
success_count = 0
total_count = 0
counter_lock = Lock()

# 最大并发线程数设置：CPU核心数的2倍，但不超过8个
MAX_WORKERS = min(multiprocessing.cpu_count() * 3, 12)

def connect_to_database():
    """连接到数据库"""
    try:
        conn_str = f'DRIVER={{{driver}}};SERVER={server};UID={username};PWD={password}'
        conn = pyodbc.connect(conn_str, timeout=30)
        return conn
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        return None

def is_trading_day():
    """判断当前日期是否为交易日"""
    try:
        # 连接数据库
        conn = connect_to_database()
            
        cursor = conn.cursor()
        
        # 获取当前日期
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 查询交易日历表，判断当前日期是否为交易日
        query = f"""
            SELECT 1
            FROM [10.101.0.212].JYPRIME.dbo.usrJYR
            WHERE ZQSC=83 AND RQ='{current_date}' AND SFJYR=1
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        # 关闭连接
        conn.close()
        
        # 如果查询结果大于0，则是交易日
        if result and result[0] > 0:
            return True
        else:
            return False
            
    except Exception as e:
        print(f"判断交易日时发生错误: {str(e)}，默认为交易日")
        return True

def execute_sql_file(file_path):
    """执行SQL文件并返回结果数量和执行时间，每个线程使用独立的数据库连接"""
    try:
        # 每个线程创建独立的数据库连接
        conn = connect_to_database()
        if not conn:
            return False, None, 0, "无法连接到数据库"
            
        cursor = conn.cursor()
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        # 记录开始时间
        start_time = time.time()

        
        # 创建一个标志来控制查询是否超时
        query_timed_out = threading.Event()
        
        # 定义超时处理函数
        def timeout_handler():
            """查询超时处理函数"""
            query_timed_out.set()
            # 尝试取消查询（通过关闭连接）
            try:
                conn.close()
            except:
                pass
        
        # 创建5分钟定时器
        timer = threading.Timer(300, timeout_handler)  # 300秒 = 5分钟
        timer.start()
        
        try:
            # 执行SQL查询
            cursor.execute(sql_content)
            
            # 如果查询在5分钟内完成，取消定时器
            timer.cancel()
            
            # 检查是否因为超时而被标记
            if query_timed_out.is_set():
                return False, None, 300, "SQL查询执行超时（超过5分钟）"
            
            # 记录结束时间
            end_time = time.time()
            execution_time = end_time - start_time
            
            # 获取结果数量
            if cursor.description:
                rows = cursor.fetchall()
                count = len(rows)
            else:
                count = 0
                
            # 关闭连接
            conn.close()
            return True, count, execution_time, None
        except Exception as e:
            # 取消定时器
            timer.cancel()
            # 检查是否因为超时而被标记
            if query_timed_out.is_set():
                return False, None, 300, "SQL查询执行超时（超过5分钟）"
            # 其他异常
            raise e
            
    except pyodbc.OperationalError as e:
        error_msg = f"SQL查询超时或操作错误: {str(e)}"
        print(f"{file_path} - {error_msg}")
        return False, None, 0, error_msg
    except Exception as e:
        error_msg = f"执行SQL文件失败: {str(e)}"
        print(f"{file_path} - {error_msg}")
        return False, None, 0, error_msg

def process_sql_file(sql_file):
    """处理单个SQL文件的函数，用于并发执行"""
    global success_count, total_count
    
    sql_filename = os.path.basename(sql_file).replace(".sql","")
    
    # 执行SQL文件
    status, count, execution_time, error = execute_sql_file(sql_file)

    # 格式化执行时间为"M分S秒"
    minutes = int(execution_time // 60)
    seconds = int(execution_time % 60)
    formatted_time = f"{minutes}分{seconds}秒"
    
    if status:
        status_text = "校验成功"
        # 使用线程安全的方式更新计数器
        with counter_lock:
            success_count += 1
            total_count += count
        
        # 查询正常且结果为0，不发送钉钉消息
        if count == 0:
            print(f"{sql_filename}:查询结果:{count}({formatted_time})")
            return sql_filename, status_text, count, formatted_time, False
    else:
        status_text = f"失败 - {error}"
    
    # 获取负责人
    # 负责人显示只包含姓名
    responsible_person_names = ", ".join([name for name in responsible_persons.keys()])
    # 负责人联系方式用于艾特
    responsible_person_phones = " ".join([f"@{phone}" for phone in responsible_persons.values()])
    
    # 发送钉钉消息
    send_result = send_dingtalk_message(sql_filename, status_text, count, formatted_time, responsible_person_names, responsible_person_phones)
    return sql_filename, status_text, count, formatted_time, send_result

def send_dingtalk_message(sql_filename, status, count, formatted_time, responsible_person_names, responsible_person_phones):
    """发送消息到钉钉机器人"""
    
    msg_header = f"## <font color='red'>校验名：{sql_filename}</font>\n"
    msg_person = f"+  负责人：{responsible_person_names}\n"
    msg_usetime = f"+  耗时：{formatted_time}\n"
    msg_status = f"+  状态：{status}\n"
    msg_count = f"+  数量：{count}\n"
    
    # 使用联系方式进行艾特，并添加换行符
    msg_at = f"\n{responsible_person_phones}"
    
    # 构建消息内容
    message_text = f"{msg_header}{msg_person}{msg_usetime}{msg_status}{msg_count}{msg_at}"

    # 构建钉钉消息格式
    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": "特别必错专项(慢)",
            "text": message_text
        },
        "at": {
            "atMobiles": [phone.replace("@", "") for phone in responsible_person_phones.split()],
            "isAtAll": False
        }
    }

    # 发送HTTP请求
    headers = {'Content-Type': 'application/json'}
    response = requests.post(dingding_webhook, data=json.dumps(data), headers=headers, timeout=10)

    if response.status_code == 200:
        result = response.json()
        if result.get('errcode') == 0:
            print(f"{sql_filename}:查询结果:{count} ({formatted_time}) - 钉钉消息发送成功")
            return True
        else:
            print(f"{sql_filename} - 钉钉消息发送失败: {result.get('errmsg')}")
            return False
    else:
        print(f"{sql_filename} - 钉钉消息发送失败: HTTP {response.status_code}")
        return False

def main():
    """主函数 - 使用并发处理SQL文件"""
    # 首先判断当前日期是否为交易日
    if not is_trading_day():
        print("非交易日，程序退出")
        return
    
    # 获取当前目录下的所有SQL文件
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_files = glob.glob(os.path.join(current_dir, "*.sql"))
    
    if not sql_files:
        print("未找到SQL文件")
        return
    
    # 重置全局计数器
    global success_count, total_count
    with counter_lock:
        success_count = 0
        total_count = 0
    
    print(f"找到 {len(sql_files)} 个SQL文件，开始并发处理...")
    
    # 确定工作线程数：不超过SQL文件数量和最大线程数
    workers = min(len(sql_files), MAX_WORKERS)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        # 提交所有任务
        futures = {executor.submit(process_sql_file, sql_file): sql_file for sql_file in sql_files}
        
        # 等待所有任务完成
        for future in concurrent.futures.as_completed(futures):
            sql_file = futures[future]
            try:
                sql_filename, status_text, count, formatted_time, send_result = future.result()
            except Exception as e:
                sql_filename = os.path.basename(sql_file).replace(".sql","")
                print(f"{sql_filename}: 处理过程中发生异常 - {str(e)}")


if __name__ == "__main__":
    main()
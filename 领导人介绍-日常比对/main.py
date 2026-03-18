import concurrent.futures
import json
import os
import queue
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from typing import Optional
from LDRJS_mappings import FIELD_MAPPING,POSITION_MAPPING

import pandas as pd
import pyodbc
import requests
from openai import OpenAI

# 用于文件选择对话框
import tkinter as tk
from tkinter import filedialog


# 数据库连接配置
SERVER = '10.102.25.11,8080'  # 服务器名称或IP地址
USERNAME = 'WebResourceNew_Read'  # 登录用户名
PASSWORD = 'New_45ted'  # 登录密码
DRIVER = 'ODBC Driver 17 for SQL Server'  # ODBC驱动版本

SQL_QUERY = '''
SELECT A.ID,B.GPDM,A.XM,CASE WHEN A.XB=1 THEN '男' WHEN A.XB=2 THEN '女' ELSE '' END AS XB,
       A.CSRQ,C.MS XL,D.MS GJ,E.ZWMC,E.ZW,CASE WHEN E.CZYF=1 THEN '在任' ELSE '离任' END AS CZYF,BZ
FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJS A
    JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
     ON A.INBBM=B.INBBM AND B.ZQSC IN (18,83,90) AND B.ZQLB IN (1,2,41)
    LEFT JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB C
     ON A.XL=C.DM AND C.LB=1154
    LEFT JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB D
     ON A.GJ=D.DM AND D.LB=1023
    JOIN (
        SELECT ZWMC,ZW,CZYF,RID,
               ROW_NUMBER() OVER (PARTITION BY RID, ZWMC, ZW ORDER BY RZQSR DESC) as rn
        FROM [10.101.0.212].JYPRIME.dbo.usrGSZYLDRJSRZQK
    ) E
     ON A.ID=E.RID AND E.rn = 1
WHERE B.GPDM = ? AND A.XM = ?
'''

DEFAULT_WORKERS = 16  # 默认并行工作线程数，可在源码中修改


class DataProcessor:
    """领导人介绍日常比对处理类"""

    def __init__(self):
        self.data_cache = {}  # 用于缓存历史数据
        self.processed_count = 0
        self.total_files = 0
        self.lock = threading.Lock()  # 用于线程安全的计数器
        self.file_status = {}  # 用于跟踪每个文件的处理状态
        self.field_mapping = FIELD_MAPPING
        self.position_mapping = POSITION_MAPPING
        self.client = None  # AI客户端实例
        self.uploaded_files = []  # 存储已上传的文件信息
        self.upload_lock = threading.Lock()  # 用于文件上传的线程锁
        self.file_queue = queue.Queue()  # 用于存储已上传文件的队列
        self.upload_complete = threading.Event()  # 标记上传是否完成

    def _initialize_client(self):
        """初始化AI客户端"""
        if self.client is None:
            self.client = OpenAI(
                api_key="sk-c88c51dd13074e6ebc14bf8339568c3f",
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            )
        return self.client

    # ==================== 并行处理相关方法 ====================

    def process_all_files_parallel(self, max_workers: int = 4, specified_dir=None):
        """并行处理所有下载的文件，支持传入指定目录"""
        latest_dir = self.get_download_dir(specified_dir=specified_dir)
        if not latest_dir:
            print("未找到下载目录")
            return []

        pdf_files = list(Path(latest_dir).glob("*.pdf"))
        if not pdf_files:
            print("未找到PDF文件")
            return []

        self.total_files = len(pdf_files)
        self.processed_count = 0
        all_results = []
        # 初始化文件状态
        self.file_status = {str(file): "pending" for file in pdf_files}
        # 重置上传完成标志
        self.upload_complete.clear()
        # 清空队列
        while not self.file_queue.empty():
            try:
                self.file_queue.get_nowait()
            except queue.Empty:
                break

        print(f"开始上传 {self.total_files} 个文件，并使用 {max_workers} 个工作线程并发处理...")

        # 启动文件上传线程
        upload_thread = threading.Thread(
            target=self._upload_files_to_queue,
            args=(pdf_files,)
        )
        upload_thread.start()

        # 使用线程池并发处理已上传的文件
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            processed_files = set()  # 用于跟踪已处理的文件
            results_lock = threading.Lock()  # 用于保护results_list的线程锁

            # 启动消费者线程，从队列中获取已上传的文件并处理
            def process_queue():
                while True:
                    try:
                        # 尝试从队列获取文件，设置超时以避免永久阻塞
                        file_info = self.file_queue.get(timeout=1)
                        if file_info is None:  # 收到结束信号
                            break
                        
                        # 提交处理任务，将results_list和results_lock传递给处理函数
                        executor.submit(self.process_uploaded_file_with_cleanup, file_info, all_results)
                        
                    except queue.Empty:
                        # 队列为空，检查是否上传完成
                        if self.upload_complete.is_set():
                            break
                        continue

            # 启动消费者线程
            consumer_threads = []
            for _ in range(max_workers):
                consumer = threading.Thread(target=process_queue)
                consumer.start()
                consumer_threads.append(consumer)

            # 等待上传线程完成
            upload_thread.join()

            # 上传完成后，向队列发送结束信号
            for _ in range(max_workers):
                self.file_queue.put(None)

            # 等待所有消费者线程完成
            for consumer in consumer_threads:
                consumer.join()

        print(f"处理完成! 共处理 {len(all_results)}/{self.total_files} 个文件")
        return all_results

    def _upload_files_to_queue(self, pdf_files):
        """上传文件到队列"""
        total_files = len(pdf_files)
        
        for index, pdf_file in enumerate(pdf_files, 1):
            try:
                file_id = self.upload_pdf_files_with_retry(str(pdf_file))
                if file_id:
                    file_info = {
                        'file_path': str(pdf_file),
                        'file_name': pdf_file.name,
                        'file_id': file_id
                    }
                    # 将文件信息放入队列
                    self.file_queue.put(file_info)
                    print(f"↑ 文件上传成功({index}/{total_files}): {pdf_file.name}")
                else:
                    print(f"✗ 文件上传失败({index}/{total_files}): {pdf_file.name}")
            except Exception as e:
                print(f"✗ 文件上传异常({index}/{total_files}): {pdf_file.name} - {e}")
        
        # 标记上传完成
        self.upload_complete.set()

    def process_uploaded_file_with_cleanup(self, file_info, results_list):
        """处理已上传的文件，并在处理完成后清理文件"""
        file_name = file_info['file_name']
        file_id = file_info['file_id']
        
        try:
            print(f"开始处理文件: {file_name}")
            start_time = time.time()
            
            result = self.process_uploaded_file(file_info)
            
            processing_time = time.time() - start_time
            print(f"文件处理完成: {file_name}, 耗时: {processing_time:.2f}秒")
            
            # 无论结果如何，都计数并打印结果
            with self.lock:
                self.processed_count += 1
            
            if result is not None:
                if result:  # 结果不为空列表
                    # 使用锁保护results_list.append操作
                    with self.lock:
                        results_list.append(result)
                    print(f"✓ 成功处理文件({self.processed_count}/{self.total_files}): {file_name}")
                else:
                    # AI提取成功但后续处理返回空列表
                    print(f"✗ 处理失败(后续处理返回空)({self.processed_count}/{self.total_files}): {file_name}")
            else:
                # AI提取数据异常，跳过处理
                print(f"△ 跳过处理({self.processed_count}/{self.total_files}): {file_name}")
            
            return result
        except Exception as e:
            # 异常情况也要计数
            with self.lock:
                self.processed_count += 1
            print(f"✗ 处理异常({self.processed_count}/{self.total_files}): {file_name} - {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            # 无论处理成功与否，都删除上传的文件
            try:
                self.delete_pdf_files(file_id)
                print(f"已清理上传的文件: {file_name}")
            except Exception as e:
                print(f"清理文件{file_name}时出错: {str(e)}")

    def process_uploaded_file(self, file_info):
        """处理已上传的文件"""
        file_name = file_info['file_name']
        file_id = file_info['file_id']

        stock_code_sql = file_name.split('-')[0]
        try:
            simplified_name = '-'.join(file_name.split('-')[4:]) if '-' in file_name else file_name
        except IndexError:
            simplified_name = file_name
            
        try:
            # 调用AI提取数据
            system_prompt = self.load_prompt_from_md()
            extracted_data = self._call_ai_service(system_prompt, file_id)
            
            if not extracted_data:
                print(f"△ AI提取为空或超时: {file_name}")
                return None

            # 检查 extracted_data 结构是否有效
            if isinstance(extracted_data, dict):
                data_list = extracted_data.get("extracted_data")
                if not data_list or not isinstance(data_list, list) or len(data_list) == 0:
                    print(f"△ AI提取数据无效: {file_name}")
                    return None
            else:
                # 如果不是字典格式，也跳过
                print(f"△ AI提取格式错误: {file_name}")
                return None

            file_results = []

            for data_item in extracted_data.get("extracted_data", []):
                stock_leader = data_item.get("领导人姓名", "")

                if not stock_code_sql or not stock_leader:
                    print(f"跳过无效数据项: 股票代码={stock_code_sql}, 领导人姓名={stock_leader}")
                    continue

                # 获取数据库数据
                sql_data = self.get_data_from_db(stock_code_sql, stock_leader)

                # 进行数据比对
                comparison_results = self.compare_data(sql_data, [data_item])

                # 构建结果对象
                result = {
                    "stock_code": stock_code_sql,
                    "stock_leader": stock_leader,
                    "extracted_data": data_item,
                    "sql_data": sql_data,
                    "comparison_results": comparison_results,
                    "processing_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "file_name": simplified_name,
                    "full_file_path": file_info['file_path']
                }
                file_results.append(result)

            return file_results if file_results else []

        except Exception as e:
            print(f"处理{file_name}时出错: {e}")
            return []

    def process_all_files(self, max_workers: int = 4, specified_dir=None):
        """并行处理所有文件，支持传入指定目录"""
        return self.process_all_files_parallel(max_workers, specified_dir)

    def cleanup_resources(self):
        """清理资源"""
        print("正在清理资源...")
        
        # 取消所有未完成的任务
        if hasattr(self, 'futures') and self.futures:
            print(f"取消 {len(self.futures)} 个未完成的任务...")
            for future in self.futures:
                future.cancel()
        
        # 关闭线程池
        if hasattr(self, 'executor') and self.executor:
            print("关闭线程池...")
            self.executor.shutdown(wait=False)
        
        print("资源清理完成")

    # ==================== 数据提取相关方法 ====================

    def upload_pdf_files_with_retry(self, pdf_file_path: str, max_retries: int = 3) -> Optional[str]:
        """带重试机制的文件上传方法"""
        client = self._initialize_client()
        
        for attempt in range(max_retries):
            try:
                file_object = client.files.create(
                    file=Path(pdf_file_path),
                    purpose="file-extract"
                )
                return file_object.id
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"文件上传失败 (尝试 {attempt + 1}/{max_retries}): {e}, 5秒后重试...")
                    time.sleep(5)
                else:
                    print(f"文件上传最终失败: {e}")
                    return None

    def delete_pdf_files(self, file_id: str) -> bool:
        """删除上传的文件"""
        client = self._initialize_client()
        
        try:
            client.files.delete(file_id)
            return True
        except Exception as e:
            print(f"删除文件失败 {file_id}: {e}")
            return False

    def load_prompt_from_md(self, md_file_path: str = "prompt_LDRJS.md") -> str:
        """从MD文件加载提示词"""
        try:
            if os.path.exists(md_file_path):
                with open(md_file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            return ""
        except Exception as e:
            return ""

    def _call_ai_service(self, system_prompt, file_id: str) -> Dict[str, Any]:
        """调用AI服务进行数据提取"""
        client = self._initialize_client()
        
        try:
            # 设置超时时间为120秒
            completion = client.chat.completions.create(
                model="qwen-long",
                messages=[
                    {"role": "system", "content": f"fileid://{file_id}"},
                    {"role": "user", "content": system_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.5,  # 低随机性，确保输出确定性
                top_p=0.3,  # 窄采样范围，减少创造性输出
                timeout=120  # 设置超时时间为120秒
            )

            json_response = completion.choices[0].message.content
            extracted_data = json.loads(json_response)

            return extracted_data
        except Exception as e:
            print(f"AI服务调用超时或失败: {e}")
            return None

    # ==================== 数据库操作相关方法 ====================

    def get_data_from_db(self, stock_code, stock_leader):
        """从正式数据库获取数据"""
        conn = None
        try:
            # print(f"正在查询数据库: 股票代码={stock_code}, 领导人={stock_leader}")
            # start_time = time.time()
            
            conn = pyodbc.connect(SERVER=SERVER, UID=USERNAME, PWD=PASSWORD, DRIVER=DRIVER)
            cursor = conn.cursor()

            # 第一次查询：使用原始stock_leader
            cursor.execute(SQL_QUERY, (stock_code, stock_leader))
            columns = [column[0] for column in cursor.description]
            sql_data = []

            for row in cursor.fetchall():
                sql_data.append(dict(zip(columns, row)))

            # 如果第一次查询无数据，尝试转换全角括号为半角括号后再次查询
            if not sql_data and any(char in stock_leader for char in ["（", "）"]):
                normalized_leader = stock_leader.replace("（", "(").replace("）", ")")        
                cursor.execute(SQL_QUERY, (stock_code, normalized_leader))
                
                for row in cursor.fetchall():
                    sql_data.append(dict(zip(columns, row)))

            # query_time = time.time() - start_time
            # print(f"数据库查询完成，返回{len(sql_data)}条记录，耗时: {query_time:.2f}秒")
            
            return sql_data

        except pyodbc.Error as e:
            print(f"数据库查询错误: {e}")
            print(f"SQLSTATE: {e.sqlstate if hasattr(e, 'sqlstate') else 'Unknown'}")
            return []
        except Exception as e:
            print(f"获取SQL数据失败: {e}")
            return []
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    print(f"关闭数据库连接时出错: {e}")

    # ==================== 数据比对相关方法 ====================

    def compare_data(self, sql_data_list, ai_data_list):
        """比对AI数据与SQL数据，使用XM作为主键，优化职位匹配逻辑"""
        # 反转映射，用于查找SQL字段对应的AI字段名
        reverse_mapping = {v: k for k, v in self.field_mapping.items()}

        comparison_results = []
        ai_data_dict_by_name = {}  # 以职位名称做主键
        ai_data_dict_by_code = {}  # 以职位代码做主键

        # 先构建AI数据的索引
        for item in ai_data_list:
            ai_xm = str(item.get("领导人姓名", "")).strip()
            ai_zwmc = str(item.get("职位名称", "")).strip()
            ai_zw = self.map_position(ai_zwmc)

            # 按名称索引
            name_key = (ai_xm, ai_zwmc)
            if name_key[0] and name_key[1]:
                if name_key not in ai_data_dict_by_name:
                    ai_data_dict_by_name[name_key] = []
                ai_data_dict_by_name[name_key].append(item)

            # 按代码索引
            if ai_zw:
                code_key = (ai_xm, ai_zw)
                if code_key not in ai_data_dict_by_code:
                    ai_data_dict_by_code[code_key] = []
                ai_data_dict_by_code[code_key].append(item)

        # 为每个AI数据项查找匹配的SQL数据
        for ai_data in ai_data_list:
            ai_xm = str(ai_data.get("领导人姓名", "")).strip()
            ai_zwmc = str(ai_data.get("职位名称", "")).strip()
            ai_zw = self.map_position(ai_zwmc)

            if not ai_xm:
                comparison_result = self._create_comparison_result({}, ai_data, "AI数据缺少领导人姓名")
                comparison_results.append(comparison_result)
                continue

            matched = False
            person_exists = False  # 标记是否找到对应的人员

            for sql_data in sql_data_list:
                # 构建SQL数据的主键
                sql_gpdm = str(sql_data.get("GPDM", "")).strip()
                sql_xm = str(sql_data.get("XM", "")).strip()
                sql_zwmc = str(sql_data.get("ZWMC", "")).strip()
                sql_zw = str(sql_data.get("ZW", "")).strip()  # 职位代码

                if not sql_gpdm or not sql_xm:
                    continue  # 跳过无效的SQL数据

                # 找到对应人员，标记为存在
                person_exists = True

                # 第一步：直接比对中文职位名称
                if sql_zwmc == ai_zwmc:
                    error_messages = self._compare_all_fields(sql_data, ai_data, reverse_mapping)
                    comparison_result = self._create_comparison_result(sql_data, ai_data, error_messages)
                    comparison_results.append(comparison_result)
                    matched = True
                    break

                # 第二步：尝试使用职位代码匹配
                # 如果职位无法匹配，直接返回错误结果
                if ai_zw is None:
                    comparison_result = self._create_comparison_result(sql_data, ai_data, "职位无法匹配")
                    comparison_results.append(comparison_result)
                    matched = True
                    break

                if ai_zw and sql_zw == ai_zw:
                    error_messages = self._compare_all_fields(sql_data, ai_data, reverse_mapping)
                    comparison_result = self._create_comparison_result(sql_data, ai_data, error_messages)
                    comparison_results.append(comparison_result)
                    matched = True
                    break

            # 第三步：根据匹配情况输出不同的错误信息
            if not matched:
                if not person_exists:
                    # 没有找到对应的人员（股票代码+姓名）
                    comparison_result = self._create_comparison_result({}, ai_data, "正式库无对应代码和领导人的数据，请检查")
                else:
                    # 找到了人员但没有匹配的职位
                    comparison_result = self._create_comparison_result({}, ai_data, "正式库无对应职位")
                comparison_results.append(comparison_result)

        return comparison_results

    def _compare_birthdate(self, sql_value, ai_value):
        """比较出生日期，只有当AI数据与SQL数据存在实质性差异时才输出错误
        规则：
        - 如果AI数据只是不如SQL数据详细，则不输出错误
        - 如果是完全不一致，则输出错误
        """
        try:
            # 如果AI比SQL更详细，则需要检查是否匹配
            # 例如：AI有年月，SQL只有年；或者AI有年月日，SQL只有年月
            # 这种情况需要报错
            if len(ai_value) > len(sql_value):
                return f"出生日期错误【正式库：{sql_value}】"

            # 解析SQL日期，提取年、月、日
            sql_parts = sql_value.split('-')
            sql_year = sql_parts[0] if len(sql_parts) > 0 else ''
            sql_month = sql_parts[1] if len(sql_parts) > 1 else ''
            sql_day = sql_parts[2] if len(sql_parts) > 2 else ''
            
            # 解析AI日期，提取年、月、日
            ai_parts = ai_value.split('-')
            ai_year = ai_parts[0] if len(ai_parts) > 0 else ''
            ai_month = ai_parts[1] if len(ai_parts) > 1 else ''
            ai_day = ai_parts[2] if len(ai_parts) > 2 else ''
            
            # 比较年
            if ai_year != sql_year:
                return f"出生日期错误【正式库：{sql_value}】"
            
            # 比较月（如果AI有月信息）
            if ai_month:
                if ai_month != sql_month:
                    return f"出生日期错误【正式库：{sql_value}】"
                
            # 比较日（如果AI有日信息）
            if ai_day:
                if ai_day != sql_day:
                    return f"出生日期错误【正式库：{sql_value}】"
            
            # 检查SQL是否比AI详细，但AI没有额外信息
            # 例如：AI只有年，SQL有年月；或者AI有年月，SQL有年月日
            # 这种情况不需要报错
            
            return ""
        except Exception as e:
            # 如果解析出错，默认返回错误
            return f"出生日期错误【正式库：{sql_value}】"
    
    def _compare_all_fields(self, sql_data, ai_data, reverse_mapping):
        """全字段比对"""
        error_messages = []

        # 定义所有需要比对的字段
        fields_to_compare = ["XB", "CSRQ", "XL", "GJ", "CZYF"]  # 包含所有字段

        for sql_field in fields_to_compare:
            ai_field = reverse_mapping.get(sql_field, sql_field)

            sql_value = sql_data.get(sql_field)
            ai_value = ai_data.get(ai_field)

            # 只有当AI值不为空时才进行比对
            if ai_value is not None and str(ai_value).strip():
                sql_value_str = str(sql_value) if sql_value is not None else ""
                ai_value_str = str(ai_value) if ai_value is not None else ""

                # 去除前后空格进行比较
                sql_value_clean = sql_value_str.strip()
                ai_value_clean = ai_value_str.strip()

                if sql_field == "CSRQ":
                    error_message = self._compare_birthdate(sql_value_clean, ai_value_clean)
                    if error_message:
                        error_messages.append(error_message)
                else:
                    if sql_value_clean != ai_value_clean:
                        error_messages.append(f"{ai_field}错误【正式库：{sql_value_clean}】")

        return error_messages

    def _create_comparison_result(self, sql_data, ai_data, error_description):
        """创建比对结果对象"""
        if isinstance(error_description, list):
            error_desc = "；".join(error_description) if error_description else ""
        else:
            error_desc = str(error_description) if error_description else ""

        gpdm = ai_data.get("股票代码", "") if ai_data.get("股票代码", "") else sql_data.get("GPDM", "")

        ldrxm = ai_data.get("领导人姓名", "") if ai_data.get("领导人姓名", "") else sql_data.get("XM", "")

        return {
            "ID": str(sql_data.get("ID")) if sql_data else "",
            "股票代码": gpdm,
            "领导人姓名": ldrxm,
            "性别": ai_data.get("性别", ""),
            "出生日期": ai_data.get("出生日期", ""),
            "学历": ai_data.get("学历", ""),
            "国籍": ai_data.get("国籍", ""),
            "职位名称": ai_data.get("职位名称", ""),
            "变动类型": ai_data.get("变动类型", ""),
            "状态": ai_data.get("状态", ""),
            "错误描述": error_desc,
            "备注": sql_data.get("BZ", "")
        }

    def map_position(self, ai_position):
        """映射AI职位到标准职位代码"""

        # 直接在映射字典中查找
        standard_zw = POSITION_MAPPING.get(ai_position)

        if standard_zw:
            return standard_zw

        # 如果没有找到映射，检查是否包含"委员"
        if "委员" in ai_position:
            return '699'  # 默认映射

        # 不包含"委员"且没有映射，返回None表示不进行比对
        return None

    # ==================== 工具方法 ====================
    def get_hashcodes_from_excel(self) -> List[str]:
        """从Excel文件读取MD5"""
        try:
            print("正在打开文件选择对话框...")
            
            # 创建根窗口
            root = tk.Tk()
            root.title("选择文件")
            # 先显示窗口，然后立即最小化
            root.update_idletasks()
            root.iconify()
            root.update()
            
            # 显示文件选择对话框
            excel_path = filedialog.askopenfilename(
                parent=root,
                title="选择包含MD5的Excel文件",
                filetypes=[("Excel Files", "*.xlsx *.xls"), ("All Files", "*")],
                initialdir="C:/"
            )
            
            print(f"文件选择结果: {excel_path}")
            
            # 销毁窗口
            root.destroy()
            
            if not excel_path:
                print("未选择任何文件")
                return []
            
            if not os.path.exists(excel_path):
                print(f"文件不存在: {excel_path}")
                return []
            
            df = pd.read_excel(excel_path)
            hashcode_columns = [col for col in df.columns if 
                                any(keyword in col.upper() for keyword in ['HASHCODE', 'MD5'])]
            
            if not hashcode_columns:
                column_name = df.columns[0]
                print(f"警告: 未找到包含'MD5'的列，默认使用第一列: {column_name}")
            else:
                column_name = hashcode_columns[0]
            
            hashcodes = df[column_name].dropna().astype(str).unique().tolist()
            hashcodes = [h.strip() for h in hashcodes if h.strip()]
            
            print(f"从Excel文件加载了 {len(hashcodes)} 个MD5 (列名: {column_name})")
            return hashcodes
        
        except Exception as e:
            print(f"读取Excel文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def create_download_dir(self, timestamp=None) -> str:
        """创建下载目录，使用指定时间戳或当前时间命名"""
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            main_dir = os.path.dirname(sys.executable)
        else:
            main_dir = os.path.dirname(os.path.abspath(__file__))
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        download_dir = os.path.join(main_dir, "files", timestamp)
        
        os.makedirs(download_dir, exist_ok=True)
        print(f"下载文件将保存到: {download_dir}")
        return download_dir
    
    def download_files(self, hashcodes: List[str], save_path: str) -> bool:
        """并发下载公告文件"""
        if not hashcodes:
            print("没有要下载的MD5")
            return False
        
        is_downloading = True
        success_count = 0
        fail_count = 0
        failed_files = []
        
        sql_template = '''
            SELECT C.GPDM,CONVERT(DATE,A.XXFBRQ) XXFBRQ,A.XXBT,B.MS,A.HASHCODE
            FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB B
                ON A.WJGS = B.DM AND B.LB = '1309'
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB C
                ON C.INBBM = A.INBBM AND C.ZQSC IN (83, 90, 18) AND C.ZQLB IN (1, 2, 41)
            WHERE A.HASHCODE = '{hashcode}'
        '''
        
        try:
            # 使用线程池进行并发下载
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # 存储future和hashcode的映射关系
                future_to_hashcode = {}
                
                for hashcode in hashcodes:
                    future = executor.submit(self._process_single_hashcode, hashcode, sql_template, save_path)
                    future_to_hashcode[future] = hashcode
                
                # 处理结果
                for future in concurrent.futures.as_completed(future_to_hashcode.keys()):
                    hashcode = future_to_hashcode[future]
                    try:
                        result = future.result(timeout=120)
                        if result:
                            success_count += 1
                            print(f"✓ 下载成功: {hashcode}")
                        else:
                            fail_count += 1
                            failed_files.append(hashcode)
                            print(f"✗ 下载失败: {hashcode} (未找到对应数据)")
                    except Exception as e:
                        fail_count += 1
                        failed_files.append(hashcode)
                        print(f"✗ 处理MD5 {hashcode} 时出错: {str(e)}")
            
            print(f"\n下载完成! 成功: {success_count}, 失败: {fail_count}")
            if failed_files:
                print("失败的MD5列表:")
                for h in failed_files:
                    print(f"  - {h}")
            
            return success_count > 0
            
        except Exception as e:
            print(f"下载过程中发生错误: {str(e)}")
            return False
        finally:
            is_downloading = False
    
    def _process_single_hashcode(self, hashcode: str, sql_template: str, save_path: str) -> bool:
        """处理单个hashcode的查询和下载"""
        try:
            sql_query = sql_template.format(hashcode=hashcode)
            data_list = self._query_data(sql_query)
            
            if data_list and len(data_list) > 0:
                # 由于一个MD5只对应一个文件，直接下载第一个结果
                return self._download_single_file(data_list[0], save_path)
            else:
                return False
                
        except Exception as e:
            print(f"处理MD5 {hashcode} 时出错: {str(e)}")
            return False
    
    def _query_data(self, sql_query: str) -> List[tuple]:
        """查询数据库获取文件信息"""
        result_list = []
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};UID={USERNAME};PWD={PASSWORD}"
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            conn.close()
            
            for item in result:
                result_list.append(item)
                
        except pyodbc.Error as e:
            print(f"数据库查询错误: {e}")
        
        return result_list
    
    def _download_single_file(self, app_id: tuple, save_path: str) -> bool:
        """下载单个文件"""
        try:
            url_template = 'http://10.6.1.131/rfApi/file/downloadWithAppId/{appId}?appId=rc-as'
            download_url = url_template.format(appId=app_id[4])
            
            # 添加请求头，模拟浏览器行为
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(download_url, headers=headers, timeout=60, stream=True)
            response.raise_for_status()
            
            # 生成文件名
            hz = str(app_id[3])
            rq = str(app_id[1])
            bt = app_id[2]
            filename = f"{app_id[0]}-{rq}-{bt}.{hz}"
            
            # 清理文件名中的特殊字符
            filename = re.sub(r'[\\/*?:"<>|]', '-', filename)
            file_path = os.path.join(save_path, filename)
            
            # 使用流式下载
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return True
            
        except Exception as e:
            print(f"下载文件失败: {app_id[4]} - {str(e)}")
            return False
    
    def get_download_dir(self, base_dir="./files", specified_dir=None):
        """获取用户指定的下载目录，支持使用指定目录"""
        try:
            if specified_dir:
                # 如果指定了目录，直接使用
                selected_dir = Path(specified_dir)
                if selected_dir.exists() and selected_dir.is_dir():
                    print(f"已选择目录: {selected_dir}")
                    return selected_dir
                else:
                    print(f"目录不存在或不是有效目录: {specified_dir}")
                    return None
            else:
                # 让用户输入目录路径
                dir_path = input("请输入下载目录路径: ").strip()

                # 检查用户输入的目录是否存在
                selected_dir = Path(dir_path)
                if selected_dir.exists() and selected_dir.is_dir():
                    print(f"已选择目录: {selected_dir}")
                    return selected_dir
                else:
                    print(f"目录不存在或不是有效目录: {dir_path}")
                    return None

        except Exception as e:
            print(f"获取目录时出错: {e}")
            return None

    def generate_report(self, results, timestamp=None):
        """生成Excel报告，支持使用指定时间戳命名"""
        if not results:
            print("没有处理结果可生成报告")
            return None

        try:
            report_data = []
            file_path_mapping = {}
            for file_results in results:
                if isinstance(file_results, list):
                    # 处理单个文件返回多个结果的情况
                    for result in file_results:
                        filename = result.get("file_name", "未知文件")
                        full_path = result.get("full_file_path", "")
                        file_path_mapping[filename] = full_path
                        for comparison in result["comparison_results"]:
                            # 添加文件名到比对结果中
                            comparison_with_filename = comparison.copy()
                            comparison_with_filename = {"公告来源": filename, **comparison_with_filename}
                            report_data.append(comparison_with_filename)
                else:
                    # 处理单个文件返回单个结果的情况（向后兼容）
                    filename = file_results.get("file_name", "未知文件")
                    full_path = file_results.get("full_file_path", "")
                    file_path_mapping[filename] = full_path
                    for comparison in file_results["comparison_results"]:
                        comparison_with_filename = comparison.copy()
                        comparison_with_filename = {"公告来源": filename, **comparison_with_filename}
                        report_data.append(comparison_with_filename)

            # 如果没有错误数据，不生成Excel文件
            if not report_data:
                print("数据无误！")
                return None

            df = pd.DataFrame(report_data)
            os.makedirs("report", exist_ok=True)
            # 使用指定的时间戳或当前时间
            if timestamp is None:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            report_path = f"report/report_{timestamp}.xlsx"
            
            # 使用xlsxwriter创建带有超链接的Excel文件
            writer = pd.ExcelWriter(report_path, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Sheet1', index=False)
            
            # 获取工作表对象
            worksheet = writer.sheets['Sheet1']
            
            # 找到"公告来源"列的索引
            header_row = df.columns.tolist()
            if "公告来源" in header_row:
                source_col_index = header_row.index("公告来源") + 1  # Excel列索引从1开始
                
                # 遍历数据行，为每个公告来源添加超链接
                for row_num in range(1, len(df) + 1):
                    filename = df.iloc[row_num - 1, source_col_index - 1]
                    full_path = file_path_mapping.get(filename, "")
                    if full_path:
                        # 转换为绝对路径的超链接格式
                        hyperlink = f'file:///{full_path.replace("\\", "/")}'
                        # 设置单元格内容为超链接
                        worksheet.write_url(row_num, source_col_index - 1, hyperlink, string=filename)
            
            # 保存并关闭Excel文件
            writer.close()
            
            print(f"报告已生成: {report_path}")
            return report_path

        except Exception as e:
            print(f"生成报告时出错: {e}")
            return None


def main():
    """主函数"""
    print("=" * 50)
    print("领导人介绍日常处理比对系统")
    print(f"工作线程数: {DEFAULT_WORKERS}")
    print("=" * 50)
    
    try:
        processor = DataProcessor()
        
        while True:
            print("\n请选择操作:")
            print("1. 下载并处理公告")
            print("2. 仅处理已下载的公告")
            print("3. 退出")

            choice = input("请输入选择 (1-3): ").strip()

            if choice == "1":
                print("\n开始下载并处理公告...")
                start_time = time.time()
                
                try:
                    # 获取当前时间戳，用于统一命名下载目录和报告文件
                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                    
                    # 1. 从Excel文件读取MD5
                    hashcodes = processor.get_hashcodes_from_excel()
                    if not hashcodes:
                        print("未获取到任何MD5，下载失败")
                        continue
                    
                    # 2. 创建下载目录，使用统一的时间戳
                    download_dir = processor.create_download_dir(timestamp)
                    
                    # 3. 下载文件
                    download_success = processor.download_files(hashcodes, download_dir)
                    if not download_success:
                        print("公告下载失败")
                        continue
                    
                    # 4. 处理文件
                    print("\n开始处理下载的公告...")
                    results = processor.process_all_files(max_workers=DEFAULT_WORKERS, specified_dir=download_dir)
                    
                    processing_time = time.time() - start_time
                    print(f"\n√ 公告处理完成，总耗时: {processing_time:.2f}秒")
                    
                    # 5. 生成报告，使用统一的时间戳
                    print("\n生成比对报告...")
                    processor.generate_report(results, timestamp)
                    
                except KeyboardInterrupt:
                    print("\n用户中断了程序执行")
                    # 尝试清理资源
                    try:
                        if hasattr(processor, 'cleanup_resources'):
                            processor.cleanup_resources()
                            print("已清理资源")
                    except Exception as e:
                        print(f"清理资源时出错: {e}")
                except Exception as e:
                    print(f"\n下载并处理公告时发生错误: {e}")
                    import traceback
                    traceback.print_exc()
                    
                    # 尝试清理资源
                    try:
                        if hasattr(processor, 'cleanup_resources'):
                            processor.cleanup_resources()
                            print("已清理资源")
                    except Exception as cleanup_error:
                        print(f"清理资源时出错: {cleanup_error}")

            elif choice == "2":
                print("\n开始处理已下载的公告...")
                start_time = time.time()
                
                try:
                    results = processor.process_all_files(max_workers=DEFAULT_WORKERS)
                    
                    processing_time = time.time() - start_time
                    print(f"\n√ 公告处理完成，总耗时: {processing_time:.2f}秒")
                    
                    # 生成报告
                    print("\n生成比对报告...")
                    processor.generate_report(results)
                    
                except KeyboardInterrupt:
                    print("\n用户中断了程序执行")
                    # 尝试清理资源
                    try:
                        if hasattr(processor, 'cleanup_resources'):
                            processor.cleanup_resources()
                            print("已清理资源")
                    except Exception as e:
                        print(f"清理资源时出错: {e}")
                except Exception as e:
                    print(f"\n处理文件时发生错误: {e}")
                    import traceback
                    traceback.print_exc()
                    
                    # 尝试清理资源
                    try:
                        if hasattr(processor, 'cleanup_resources'):
                            processor.cleanup_resources()
                            print("已清理资源")
                    except Exception as cleanup_error:
                        print(f"清理资源时出错: {cleanup_error}")

            elif choice == "3":
                print("程序退出")
                break

            else:
                print("无效选择，请重新输入")
                
    except Exception as e:
        print(f"程序初始化失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

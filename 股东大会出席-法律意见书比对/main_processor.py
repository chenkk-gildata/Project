import concurrent.futures
import decimal
import json
import os
import re
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from typing import Optional
from mappings import FIELD_MAPPING, ATTEND_TYPE_MAPPING, ATTEND_FIELD_MAPPING

import pandas as pd
import pyodbc
from openai import OpenAI

import cninfo_Crawling_FLYJS_usrGDDHCX as cj

# 数据库连接配置
SERVER = '10.102.25.11,8080'  # 服务器名称或IP地址
USERNAME = 'WebResourceNew_Read'  # 登录用户名
PASSWORD = 'New_45ted'  # 登录密码
DRIVER = 'ODBC Driver 17 for SQL Server'  # ODBC驱动版本

SQL_QUERY = '''
SELECT A.ID,B.GPDM,CONVERT(DATE,A.GDDHGGR) GDDHGGR,A.ND,A.JC,A.GDCXLB,A.LSSWS,
       A.CXGD,A.ZTAGGDRS,A.ZTHGGDRS,A.ZTQTGDRS,
       A.DBGF,A.ZTAGGDDBGF,A.ZTHGGDDBGF,A.ZTQTGDDBGF,
       A.ZB*100 ZB,A.ZTAGGDZB*100 ZTAGGDZB,A.ZTHGGDZB*100 ZTHGGDZB,A.ZTQTGDZB*100 ZTQTGDZB,
       A.ZXGDCXRS,A.ZXGDDBGF,A.ZXGDZB*100 ZXGDZB
FROM [10.101.0.212].JYPRIME.dbo.usrGDDHCX A
    JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
     ON A.INBBM=B.INBBM AND B.ZQSC IN (18,83,90) AND B.ZQLB IN (1,2,41)
WHERE B.GPDM = ? AND A.GDDHGGR = ? AND A.ND = ? AND A.JC = ? AND A.SFYX=1 AND A.GKBZ=3
'''

DEFAULT_WORKERS = 12  # 默认并行工作线程数，可在源码中修改


class DataProcessor:
    """股东大会法律意见书数据处理类"""

    def __init__(self):
        self.data_cache = {}  # 用于缓存历史数据
        self.processed_count = 0
        self.total_files = 0
        self.lock = threading.Lock()  # 用于线程安全的计数器
        self.file_status = {}  # 用于跟踪每个文件的处理状态
        self.field_mapping = FIELD_MAPPING
        self.attend_type_mapping = ATTEND_TYPE_MAPPING
        self.attend_field_mapping = ATTEND_FIELD_MAPPING
        
        # 生成session_id
        self.session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 上传进度相关属性
        self.uploaded_count = 0
        self.total_uploads = 0
        self.upload_lock = threading.Lock()  # 用于上传进度的线程安全
        
        # AI服务相关初始化
        self.client = OpenAI(
            api_key="sk-c88c51dd13074e6ebc14bf8339568c3f",
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        # 速率控制相关属性
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 最小请求间隔500ms
        # 文件上传并发控制
        self.upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        # ==================== 并行处理相关方法 ====================

    def process_all_files_parallel(self, max_workers: int = 12):
        """并行处理所有下载的文件"""
        latest_dir = self.get_latest_download_dir()
        if not latest_dir:
            print("未找到下载目录")
            return []

        pdf_files = list(Path(latest_dir).glob("*.pdf"))
        if not pdf_files:
            print("未找到PDF文件")
            return []

        self.total_files = len(pdf_files)
        self.total_uploads = len(pdf_files)  # 初始化总上传数
        self.processed_count = 0
        self.uploaded_count = 0  # 初始化已上传数
        all_results = []
        # 初始化文件状态
        self.file_status = {str(file): "pending" for file in pdf_files}

        print(f"开始并行处理 {self.total_files} 个文件，使用 {max_workers} 个工作线程...")
        print(f"文件上传将使用 {self.upload_executor._max_workers} 个并发线程")

        # 使用线程池并行处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_file = {
                executor.submit(self.process_file_worker, str(pdf_file)): pdf_file
                for pdf_file in pdf_files
            }

            # 处理完成的任务并显示进度
            for future in concurrent.futures.as_completed(future_to_file):
                pdf_file = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        all_results.append(result)
                        # 使用锁来安全地更新计数器和打印
                        with self.lock:
                            self.processed_count += 1
                            print(f"✓ 成功处理文件({self.processed_count}/{self.total_files}): {pdf_file.name}")
                    else:
                        with self.lock:
                            self.processed_count += 1
                            print(f"✗ 处理失败({self.processed_count}/{self.total_files}): {pdf_file.name}")
                except Exception as e:
                    with self.lock:
                        self.processed_count += 1
                        print(f"✗ 处理异常({self.processed_count}/{self.total_files}): {pdf_file.name} - {e}")

        print(f"处理完成! 共处理 {len(all_results)}/{self.total_files} 个文件")
        return all_results

    def process_file_worker(self, file_path: str):
        """处理单个文件的worker函数（线程安全）"""
        result = self.process_file(file_path)
        return result

    def process_all_files(self, parallel: bool = True, max_workers: int = 12):
        """处理所有文件，可选择并行或串行模式"""
        if parallel:
            return self.process_all_files_parallel(max_workers)
        else:
            return self.process_all_files_serial()

    def process_all_files_serial(self):
        """串行处理所有文件（保持原有逻辑）"""
        latest_dir = self.get_latest_download_dir()
        if not latest_dir:
            print("未找到下载目录")
            return []

        pdf_files = list(Path(latest_dir).glob("*.pdf"))
        if not pdf_files:
            print("未找到PDF文件")
            return []

        all_results = []
        total_files = len(pdf_files)
        self.total_uploads = total_files  # 初始化总上传数
        self.uploaded_count = 0  # 初始化已上传数

        for i, pdf_file in enumerate(pdf_files, 1):
            filename = pdf_file.name
            result = self.process_file(str(pdf_file))

            if result:
                all_results.append(result)
                print(f"✓ 成功处理文件({i}/{total_files})")
            else:
                print(f"✗ 处理失败({i}/{total_files}): {filename}")

        print(f"处理完成! 共处理 {len(all_results)}/{total_files} 个文件")
        return all_results

    def process_file(self, file_path: str):
        """处理单个PDF文件"""
        try:
            filename = os.path.basename(file_path)
            parts = filename.split('-')

            if len(parts) >= 4:
                stock_code = parts[0]
                stock_name = parts[1]
                stock_date = parts[2].replace('.', '-')
                stock_filename = '-'.join(parts[3:]).replace(' ', '')
            else:
                print(f"文件名格式异常: {filename}")
                return None

            # 从文件名中提取年度和届次（备用）
            stock_nd, stock_jc = self._extract_year_and_session(stock_filename)
            if not stock_nd:
                print(f"无法从文件名中提取年度: {filename}")
                return None

            # 判断会议类型（用于届次验证）
            meeting_type = self._get_meeting_type(stock_filename)

            # 调用AI提取数据
            extracted_data = self.extract_data_with_ai(file_path)
            if not extracted_data:
                return None

            # 从AI结果中提取届次，结合会议类型判断届次是否有效
            extracted_inner = extracted_data.get("extracted_data", {})
            ai_jc = extracted_inner.get("届次", "")
            
            if ai_jc and str(ai_jc).strip():
                # AI届次有值，直接使用
                stock_jc = str(ai_jc).strip()
            elif meeting_type == "1":
                # 年度股东大会，届次为空是正常的
                stock_jc = ""
            else:
                # 临时股东大会但AI届次为空，使用文件名解析值备用
                if stock_jc == '0':
                    print(f"临时股东大会AI届次为空且文件名解析失败: {filename}")
                # 保持stock_jc（文件名解析值）

            # 获取数据库数据（使用动态SQL）
            sql_data = self.get_data_from_db(stock_code, stock_date, stock_nd, stock_jc)

            # 进行数据比对
            comparison_results = self.compare_data(
                extracted_inner,
                sql_data
            )

            # 构建结果对象
            result = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "meeting_date": stock_date,
                "filename": filename,
                "extracted_data": extracted_inner,
                "sql_data": sql_data,
                "comparison_results": comparison_results,
                "processing_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            return result

        except Exception as e:
            print(f"处理文件时出错: {e}")
            return None

    def _get_meeting_type(self, filename: str) -> str:
        """从文件名判断会议类型"""
        if "年度" in filename and "临时" not in filename:
            return "1"  # 年度股东大会
        elif "出资人组" in filename:
            return "5"  # 出资人组会议
        else:
            return "3"  # 临时股东大会（默认）

    def _extract_year_and_session(self, filename: str):
        """从文件名中提取年度和届次"""
        stock_nd = None
        stock_jc = None

        # 使用正则表达式匹配年度（第YYYY年）
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            stock_nd = year_match.group(1)

        # 使用正则表达式匹配届次（第几次）
        jc_match = re.search(r'第([一二三四五六七八九十\d]+)次', filename)
        if jc_match:
            jc_text = jc_match.group(1)
            stock_jc = self.convert_chinese_number(jc_text)
        else:
            stock_jc = '0'  # 未提取到时返回0

        return stock_nd, stock_jc

    # ==================== 数据提取相关方法 ====================
    
    def _rate_limit_control(self):
        """智能速率控制"""
        import time
        import random
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            # 添加随机抖动，避免多个请求同时发送
            sleep_time += random.uniform(0.1, 0.3)
            time.sleep(sleep_time)

        self.last_request_time = time.time()
        self.request_count += 1

    def upload_file(self, file_path: str) -> str:
        """上传文件到AI服务"""
        filename = os.path.basename(file_path)
        try:
            # 打印上传开始信息
            with self.upload_lock:
                current_upload = self.uploaded_count + 1
            
            # 应用速率控制
            self._rate_limit_control()

            with open(file_path, 'rb') as f:
                file_object = self.client.files.create(
                    file=f,
                    purpose="file-extract"
                )
            
            # 更新上传进度
            with self.upload_lock:
                self.uploaded_count += 1
            print(f"✓ 文件上传成功 ({self.uploaded_count}/{self.total_uploads}): {filename}")
            
            return file_object.id
        except Exception as e:
            print(f"✗ 文件上传失败 ({self.uploaded_count + 1}/{self.total_uploads}): {filename} - {e}")
            # 如果是速率限制错误，增加等待时间
            if "429" in str(e) or "rate_limit" in str(e).lower():
                self.min_request_interval = min(self.min_request_interval * 1.5, 5.0)
                print(f"检测到速率限制，调整请求间隔为 {self.min_request_interval:.2f}秒")
            raise

    def delete_file(self, file_id: str) -> bool:
        """删除上传的文件"""
        try:
            self.client.files.delete(file_id)
            return True
        except Exception as e:
            print(f"删除文件失败 {file_id}: {e}")
            return False

    def extract_data_with_ai(self, pdf_file_path: str) -> Optional[Dict[str, Any]]:
        """调用AI接口提取股东大会决议公告数据"""
        try:
            # 使用上传线程池控制并发不超过2
            future = self.upload_executor.submit(self.upload_file, pdf_file_path)
            file_id = future.result()
            try:
                # 调用AI服务提取数据
                extracted_data = self._call_ai_service(file_id)
                
                # 将AI提取的JSON数据保存到日志文件
                try:
                    log_dir = os.path.join("logs")
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    filename = os.path.basename(pdf_file_path)
                    with open(os.path.join(log_dir, f"ai_extraction_data_{self.session_id}.log"), "a", encoding="utf-8") as f:
                        f.write(f"\n=== 股东大会法律意见书：{filename} ===\n")
                        f.write(f"{json.dumps(extracted_data, ensure_ascii=False, indent=2)}")
                        f.write(f"\n==========================================\n")
                except Exception as e:
                    print(f"保存AI提取数据到日志文件失败: {filename} - {e}")
                
                return extracted_data
            finally:
                # 确保文件被删除，无论AI调用是否成功
                self.delete_file(file_id)
        except Exception as e:
            print(f"AI数据提取失败: {e}")
            return None

    def get_resource_path(self, relative_path):
        """获取资源的绝对路径，适用于开发和打包后的环境"""
        try:
            # PyInstaller创建的临时文件夹路径
            base_path = sys._MEIPASS
        except Exception:
            # 开发环境下的路径
            base_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_path, relative_path)

    def load_prompt_from_md(self, md_file_path="prompt_GDDHCX.md"):
        """从MD文件加载提示词（优先从exe目录读取，便于用户修改）"""
        try:
            # 1. 优先从exe所在目录读取（打包后用户可修改）
            try:
                exe_dir = os.path.dirname(sys.executable)
                exe_path = os.path.join(exe_dir, md_file_path)
                if os.path.exists(exe_path):
                    with open(exe_path, 'r', encoding='utf-8') as f:
                        return f.read()
            except:
                pass
            
            # 2. 尝试当前工作目录
            if os.path.exists(md_file_path):
                with open(md_file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            # 3. 尝试脚本所在目录（开发环境）
            script_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(script_dir, md_file_path)
            if os.path.exists(script_path):
                with open(script_path, 'r', encoding='utf-8') as f:
                    return f.read()
            
            print(f"提示词文件 {md_file_path} 不存在，请检查")
            return ""
        except Exception as e:
            print(f"读取提示词文件失败: {e}")
            return ""

    def _call_ai_service(self, file_id: str) -> Dict[str, Any]:
        """调用AI服务进行数据提取"""
        system_prompt = self.load_prompt_from_md()

        # 应用速率控制
        self._rate_limit_control()

        completion = self.client.chat.completions.create(
            model="qwen-long",
            messages=[
                {"role": "system", "content": f"fileid://{file_id}"},
                {"role": "user", "content": system_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,  # 低随机性，确保输出确定性
            top_p=0.2  # 窄采样范围，减少创造性输出
        )

        json_response = completion.choices[0].message.content
        extracted_data = json.loads(json_response)

        return extracted_data

    # ==================== 数据库操作相关方法 ====================

    def get_data_from_db(self, stock_code, stock_date, stock_nd, stock_jc):
        """从正式数据库获取数据，动态处理届次查询条件"""
        try:
            conn = pyodbc.connect(SERVER=SERVER, UID=USERNAME, PWD=PASSWORD, DRIVER=DRIVER)
            cursor = conn.cursor()

            # 动态构建SQL和参数
            if stock_jc and str(stock_jc).strip():
                sql = SQL_QUERY
                params = (stock_code, stock_date, stock_nd, stock_jc)
            else:
                # 年度股东大会：JC为空，使用IS NULL
                sql = SQL_QUERY.replace("AND A.JC = ?", "AND A.JC IS NULL")
                params = (stock_code, stock_date, stock_nd)

            cursor.execute(sql, params)
            columns = [column[0] for column in cursor.description]
            sql_data = []

            for row in cursor.fetchall():
                sql_data.append(dict(zip(columns, row)))

            conn.close()
            return sql_data

        except Exception as e:
            print(f"获取SQL数据失败: {e}")
            return []

    # ==================== 数据比对相关方法 ====================

    def compare_data(self, current_data, sql_data_list):
        """比对AI数据与sql数据"""
        comparison_results = []

        for sql_data in sql_data_list:
            error_messages = []
            gdcxlb = sql_data.get("GDCXLB")

            # 预处理AI数据
            processed_data = self._preprocess_ai_data(current_data)

            # 比对律师事务所和经办律师
            self._compare_text_fields(processed_data, sql_data, error_messages)

            # 根据GDCXLB比对出席相关字段
            if gdcxlb in self.attend_field_mapping:
                self._compare_attendance_fields(
                    processed_data, sql_data, gdcxlb, error_messages
                )

            if error_messages:
                comparison_result = {
                    "ID": str(sql_data.get("ID")),
                    "GPDM": sql_data.get("GPDM"),
                    "GDDHGGR": sql_data.get("GDDHGGR"),
                    "ND": sql_data.get("ND"),
                    "JC": sql_data.get("JC"),
                    "GDCXLB": gdcxlb,
                    "错误描述": "；".join(error_messages)
                }
                comparison_results.append(comparison_result)

        return comparison_results

    def _compare_text_fields(self, processed_data, sql_data, error_messages):
        """比对文本字段（律师事务所、经办律师）"""
        for ai_field, db_field in self.field_mapping.items():
            current_value = processed_data.get(ai_field, "").replace(" ", "")
            db_value = sql_data.get(db_field, "").replace(" ", "")

            if self._is_empty_value(current_value):
                continue

            # 特殊处理经办律师字段，允许顺序不一致
            if ai_field == "经办律师":
                continue
            else:
                if current_value != db_value:
                    error_messages.append(f"{ai_field}错误【正式库:{db_value},AI:{current_value}】")

    def _compare_attendance_fields(self, processed_data, sql_data, gdcxlb, error_messages):
        """比列出席相关字段"""
        field_map = self.attend_field_mapping[gdcxlb]

        for db_field, ai_field in field_map.items():
            current_value = processed_data.get(ai_field, "")
            db_value = sql_data.get(db_field, "")

            if self._is_empty_value(current_value):
                continue

            self._compare_numeric_fields(current_value, db_value, ai_field, error_messages)

    def _compare_numeric_fields(self, current_value, db_value, field_name, error_messages):
        """比较数值字段"""
        if isinstance(db_value, (int, float, decimal.Decimal)) and isinstance(current_value, str):
            try:
                current_value_clean = re.sub(r'[^\d.]', '', current_value.replace('%', '').strip())
                current_value_float = float(current_value_clean)

                if isinstance(db_value, int):
                    if abs(current_value_float - db_value) > 0.01:
                        error_messages.append(f"{field_name}错误【正式库:{db_value},AI:{current_value}】")
                    return

                elif isinstance(db_value, (float, decimal.Decimal)):
                    if '%' in current_value:
                        if abs(current_value_float - float(db_value) * 100) > 0.01:
                            error_messages.append(f"{field_name}错误【正式库:{db_value},AI:{current_value}】")
                        return
                    else:
                        if abs(current_value_float - float(db_value)) > 0.01:
                            error_messages.append(f"{field_name}错误【正式库:{db_value},AI:{current_value}】")
                        return

            except (ValueError, AttributeError, decimal.InvalidOperation):
                if str(db_value) != current_value:
                    error_messages.append(f"{field_name}错误【正式库:{db_value},AI:{current_value}】")
                return

        if current_value != db_value:
            error_messages.append(f"{field_name}错误【正式库:{db_value},AI:{current_value}】")

    def _preprocess_ai_data(self, current_data):
        """预处理AI数据"""
        processed_data = current_data.copy()

        if '律师事务所' in processed_data and isinstance(processed_data['律师事务所'], str):
            processed_data['律师事务所'] = processed_data['律师事务所'].replace('（', '(').replace('）', ')')

        return processed_data

    def _is_empty_value(self, value):
        """判断是否为空值"""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == 'None':
            return True
        if isinstance(value, str) and value.strip() == '':
            return True
        if isinstance(value, (int, float)) and value == 0:
            return False  # 数值0不算空值
        return False

    # ==================== 工具方法 ====================

    def convert_chinese_number(self, chinese_num):
        """将汉字数字转换为阿拉伯数字"""
        chinese_to_digit = {
            '一': '1', '二': '2', '三': '3', '四': '4', '五': '5',
            '六': '6', '七': '7', '八': '8', '九': '9', '十': '10',
            '十一': '11', '十二': '12', '十三': '13', '十四': '14', '十五': '15'
        }

        if chinese_num.isdigit():
            return chinese_num

        return chinese_to_digit.get(chinese_num, chinese_num)

    def get_latest_download_dir(self, base_dir="./files"):
        """获取最新下载的目录"""
        try:
            directories = [d for d in Path(base_dir).iterdir() if d.is_dir()]
            if not directories:
                return None
            latest_dir = max(directories, key=os.path.getmtime)
            return latest_dir
        except Exception as e:
            print(f"获取最新目录时出错: {e}")
            return None

    def generate_report(self, results):
        """生成Excel报告"""
        if not results:
            print("没有处理结果可生成报告")
            return None

        try:
            report_data = []
            for result in results:
                for comparison in result["comparison_results"]:
                    report_data.append(comparison)

            # 如果没有错误数据，不生成Excel文件
            if not report_data:
                print("数据无误！")
                return None

            df = pd.DataFrame(report_data)
            os.makedirs("report", exist_ok=True)
            report_path = f"report/report_{self.session_id}.xlsx"
            df.to_excel(report_path, index=False)
            print(f"报告已生成: {report_path}")
            return report_path

        except Exception as e:
            print(f"生成报告时出错: {e}")
            return None


def run_crawler():
    """运行采集程序 - 直接调用类方式"""
    try:
        cj.main()
        return True
    except ImportError as e:
        print(f"导入爬虫模块失败: {e}")
        return False
    except Exception as e:
        print(f"运行采集程序时出错: {e}")
        return False


def main():
    """主函数"""
    processor = DataProcessor()

    print("=" * 50)
    print("股东大会法律意见书数据处理系统")
    print("=" * 50)

    while True:
        print("\n请选择操作:")
        print("1. 运行采集程序并处理数据")
        print("2. 仅处理已下载的文件")
        print("3. 退出")

        choice = input("请输入选择 (1-3): ").strip()

        if choice == "1":
            if not run_crawler():
                continue
            # 继续执行文件处理
            choice = "2"

        if choice == "2":
            print("正在处理文件……")
            results = processor.process_all_files(parallel=True, max_workers=DEFAULT_WORKERS)

            if results:
                processor.generate_report(results)

        elif choice == "3":
            print("程序退出")
            break

        else:
            print("无效选择，请重新输入")


if __name__ == "__main__":
    main()

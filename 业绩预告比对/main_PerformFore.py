"""
业绩预告AI比对系统
"""
import concurrent.futures
import json
import os
import threading
import time
import re
import openpyxl
import pandas as pd
import pyodbc
import requests
import traceback
import fitz

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from config_PerformFore import performance_objects, validate_config
from database_manager import get_db_manager
from ai_service_enhanced import get_ai_service
from logger_config import setup_logging, get_logger, get_session_id, get_file_only_logger, reset_session_id
from path_utils import get_output_dir, get_log_dir, get_report_dir, get_files_dir, get_resource_path

# 获取程序运行目录（兼容开发和打包环境）
main_dir = get_output_dir()

# 全局日志变量初始化
logger = None
file_only_logger = None

# 初始化日志的函数
def init_logging():
    """初始化日志"""
    global logger, file_only_logger
    if logger is None or file_only_logger is None:
        setup_logging()
        logger = get_logger(__name__)
        file_only_logger = get_file_only_logger(__name__)

# 不立即初始化日志，只在需要时初始化

# SQL查询语句
SQL_QUERY = '''
SELECT A.ID,B.GPDM,CONVERT(DATE,A.XXFBRQ) 信息发布日期,CONVERT(DATE,A.QSRQ) 预计起始日期,CONVERT(DATE,A.JZRQ) 预计截止日期,
       A.YJDXYSMC 预计对象原始名称,A.YJYJDX 业绩预计对象,A.YJFDQSPL 预计幅度起始,A.YJFDJZPL 预计幅度截止,
       A.QSZJE 起始增减额,A.JZZJE 截止增减额,A.QSJE 预计起始额,A.JZJE 预计截止额,A.SNTQJE 上年同期额,
       CASE WHEN A.ZBDW='FCC00000006T' THEN '元'
            WHEN A.ZBDW='FCC00000006V' THEN '万元'
            WHEN A.ZBDW='FCC00000006U' THEN '千元'
            WHEN A.ZBDW='FCC00000006W' THEN '百万元'
            WHEN A.ZBDW='FCC00000006X' THEN '千万元'
            WHEN A.ZBDW='FCC00000006Y' THEN '亿元'
           END 单位
FROM [10.106.22.60].JYFIN.dbo.usrCWJCZBZYCYJYGCJB A
    JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
        ON A.INBBM=B.INBBM AND B.ZQSC IN (83,90,18) AND B.ZQLB IN (1,2,41)
WHERE B.GPDM = ? AND A.XXFBRQ = ?
'''

# 数据库连接配置 - 用于资源中心公告下载
RESOURCE_DB_CONFIG = {
    'SERVER': '10.102.25.11,8080',
    'USERNAME': 'WebResourceNew_Read',
    'PASSWORD': 'New_45ted',
    'DRIVER': 'ODBC Driver 17 for SQL Server'
}


class DownloadManager:
    """资源中心公告下载管理器"""
    
    def __init__(self):
        self.is_downloading = False
        self.success_count = 0
        self.fail_count = 0
        self.failed_files = []
        self.logger = get_logger(__name__)
        
    def get_hashcodes(self) -> List[str]:
        """从Excel文件读取MD5"""
        print("\n将从Excel文件读取MD5")
        return self._get_hashcodes_from_excel()
    
    def _get_hashcodes_from_excel(self) -> List[str]:
        """从Excel文件读取MD5"""
        try:
            print("正在打开文件选择对话框...")
            
            # 确保tkinter正确导入
            import tkinter as tk
            from tkinter import filedialog
            
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
            traceback.print_exc()
            return []
    
    def create_download_dir(self) -> str:
        """创建下载目录，使用session_id命名"""
        session_id = get_session_id()
        download_dir = get_files_dir(session_id)
        
        print(f"下载文件将保存到: {download_dir}")
        return download_dir
    
    def download_files(self, hashcodes: List[str], save_path: str) -> bool:
        """并发下载公告文件"""
        if not hashcodes:
            print("没有要下载的MD5")
            return False
        
        self.is_downloading = True
        self.success_count = 0
        self.fail_count = 0
        self.failed_files = []
        
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
                            self.success_count += 1
                            print(f"✓ 下载成功: {hashcode}")
                        else:
                            self.fail_count += 1
                            self.failed_files.append(hashcode)
                            print(f"✗ 下载失败: {hashcode} (未找到对应数据)")
                    except Exception as e:
                        self.fail_count += 1
                        self.failed_files.append(hashcode)
                        print(f"✗ 处理MD5 {hashcode} 时出错: {str(e)}")
            
            print(f"\n下载完成! 成功: {self.success_count}, 失败: {self.fail_count}")
            if self.failed_files:
                print("失败的MD5列表:")
                for h in self.failed_files:
                    print(f"  - {h}")
            
            return self.success_count > 0
            
        except Exception as e:
            print(f"下载过程中发生错误: {str(e)}")
            return False
        finally:
            self.is_downloading = False
    
    def _process_single_hashcode(self, hashcode: str, sql_template: str, save_path: str) -> bool:
        """处理单个hashcode的查询和下载"""
        if not self.is_downloading:
            return False
        
        try:
            sql_query = sql_template.format(hashcode=hashcode)
            data_list = self._query_data(sql_query)
            
            if data_list and len(data_list) > 0:
                # 由于一个MD5只对应一个文件，直接下载第一个结果
                return self._download_single_file(data_list[0], save_path)
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"处理MD5 {hashcode} 时出错: {str(e)}")
            return False
    
    def _query_data(self, sql_query: str) -> List[tuple]:
        """查询数据库获取文件信息"""
        result_list = []
        conn_str = f"DRIVER={{{RESOURCE_DB_CONFIG['DRIVER']}}};SERVER={RESOURCE_DB_CONFIG['SERVER']};UID={RESOURCE_DB_CONFIG['USERNAME']};PWD={RESOURCE_DB_CONFIG['PASSWORD']}"
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            conn.close()
            
            for item in result:
                result_list.append(item)
                
        except pyodbc.Error as e:
            self.logger.error(f"数据库查询错误: {e}")
        
        return result_list
    
    def _download_single_file(self, app_id: tuple, save_path: str) -> bool:
        """下载单个文件"""
        if not self.is_downloading:
            return False
        
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
            import re
            filename = re.sub(r'[\\/*?:"<>|]', '-', filename)
            file_path = os.path.join(save_path, filename)
            
            # 使用流式下载
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return True
            
        except Exception as e:
            self.logger.error(f"下载文件失败: {app_id[4]} - {str(e)}")
            return False


class EnhancedDataProcessor:
    """业绩预告数据处理类"""

    def __init__(self):
        self.lock = threading.Lock()
        self.file_status = {}
        self.uploaded_file_ids = {}

        # 验证配置
        try:
            validate_config()
        except ValueError as e:
            print(f"配置验证失败: {e}")
            raise

    def process_all_files(self, pdf_files: List[Path]) -> List[Dict[str, Any]]:
        """处理所有PDF文件 - 优化版，增强流水线模式"""
        if not pdf_files:
            return []

        # 使用优化后的流水线模式
        return self._pipeline_upload_and_process(pdf_files)

    def _pipeline_upload_and_process(self, pdf_files: List[Path]):
        """
        优化的流水线处理：上传和处理并行进行，避免资源竞争
        
        主要优化点：
        1. 简化线程池配置和队列管理逻辑
        2. 减少初始化阶段的复杂检查
        3. 优化任务提交和处理流程
        4. 简化异常处理，确保资源正确释放
        5. 修复线程池过早关闭的问题
        """
        # 初始化变量
        all_results = []
        upload_queue = pdf_files.copy()  # 待上传文件队列
        failed_uploads = []  # 失败的上传文件列表

        # 简化线程池配置 - 减少线程数以降低资源竞争
        upload_workers = 2  # 上传线程数
        process_workers = 16  # 处理线程数
        
        upload_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=upload_workers,
            thread_name_prefix="Upload"
        )
        process_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=process_workers,
            thread_name_prefix="Process"
        )

        # 简化任务跟踪
        upload_futures = {}  # 上传任务字典 {future: pdf_file}
        process_futures = {}  # 处理任务字典 {future: pdf_file}

        # 计数器
        upload_count = 0
        completed_count = 0

        print(f"任务流配置: {upload_workers}个上传线程, {process_workers}个处理线程")

        try:
            # 主循环：处理上传和处理任务直到所有文件完成
            # 修复条件：确保所有上传任务和处理任务都完成
            while upload_queue or upload_futures or process_futures:
                # === 第一阶段：提交上传任务 ===
                # 简化条件：有待上传文件且未达并发上限
                while len(upload_futures) < upload_workers and upload_queue:
                    # 从上传队列取出文件
                    pdf_file = upload_queue.pop(0)

                    # 提交上传任务到线程池
                    future = upload_executor.submit(
                        self._upload_single_file_with_timeout,
                        pdf_file
                    )
                    upload_futures[future] = pdf_file
                    logger.debug(f"提交上传任务: {pdf_file.name}")

                # === 第二阶段：检查并处理完成的上传任务 ===
                if upload_futures:
                    # 使用as_completed处理完成的上传任务，提高效率
                    completed_uploads = []
                    try:
                        for future in concurrent.futures.as_completed(upload_futures, timeout=0.1):
                            completed_uploads.append(future)
                    except concurrent.futures.TimeoutError:
                        # 没有完成的任务，继续下一轮循环
                        pass
                    
                    # 处理每个完成的上传任务
                    for future in completed_uploads:
                        pdf_file = upload_futures.pop(future)

                        try:
                            # 获取上传结果
                            file_id = future.result()

                            if file_id:
                                # 上传成功
                                upload_count += 1
                                print(f"↑ 上传成功({upload_count}/{len(pdf_files)}): {pdf_file.name}")

                                # 立即提交处理任务
                                process_future = process_executor.submit(
                                    self._process_and_cleanup_single_file,
                                    pdf_file, file_id, pdf_file.name
                                )
                                process_futures[process_future] = pdf_file
                                logger.debug(f"提交处理任务: {pdf_file.name}")
                            else:
                                # 上传失败
                                failed_uploads.append(pdf_file.name)
                                print(f"- 上传失败 ({upload_count + 1}/{len(pdf_files)}): {pdf_file.name}")

                        except Exception as e:
                            # 上传异常
                            failed_uploads.append(pdf_file.name)
                            print(f"- 上传异常 ({upload_count + 1}/{len(pdf_files)}) {pdf_file.name}: {e}")

                # === 第三阶段：检查并处理完成的处理任务 ===
                if process_futures:
                    # 使用as_completed处理完成的处理任务，提高效率
                    completed_processes = []
                    try:
                        for future in concurrent.futures.as_completed(process_futures, timeout=0.1):
                            completed_processes.append(future)
                    except concurrent.futures.TimeoutError:
                        # 没有完成的任务，继续下一轮循环
                        pass
                    
                    # 处理每个完成的处理任务
                    for future in completed_processes:
                        pdf_file = process_futures.pop(future)

                        try:
                            # 获取处理结果
                            result = future.result()

                            if result:
                                # 处理成功
                                all_results.append(result)
                                status = "成功"
                            else:
                                # 处理失败
                                status = "失败"

                            completed_count += 1
                            print(
                                f"{'✓' if result else '✗'} 处理{status}({completed_count}/{len(pdf_files)}): {pdf_file.name}")

                        except Exception as e:
                            # 处理异常
                            completed_count += 1
                            print(f"✗ 处理异常({completed_count}/{len(pdf_files)}): {pdf_file.name} - {e}")

                # === 第四阶段：资源控制 ===
                # 减少休眠时间，提高响应速度
                time.sleep(0.05)

        except Exception as e:
            # 记录异常但继续执行finally块
            logger.error(f"流水线处理过程中发生异常: {e}", exc_info=True)
        
        finally:
            # === 资源清理阶段 ===
            logger.info("开始关闭线程池...")

            # 等待所有上传任务完成
            logger.info(f"等待 {len(upload_futures)} 个上传任务完成...")
            for future in list(upload_futures.keys()):
                try:
                    future.result(timeout=30)  # 给每个任务30秒完成时间
                except Exception as e:
                    logger.error(f"等待上传任务完成时出错: {e}")
                    future.cancel()

            # 等待所有处理任务完成
            logger.info(f"等待 {len(process_futures)} 个处理任务完成...")
            for future in list(process_futures.keys()):
                try:
                    future.result(timeout=120)  # 给每个处理任务120秒完成时间
                except Exception as e:
                    logger.error(f"等待处理任务完成时出错: {e}")
                    future.cancel()

            # 关闭线程池
            upload_executor.shutdown(wait=True)
            process_executor.shutdown(wait=True)

            logger.info("所有线程池已关闭")

        return all_results

    def _process_and_cleanup_single_file(self, pdf_file: Path, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个文件并清理资源 - 增强日志版"""
        process_start_time = time.time()
        logger.info(f"开始处理文件: {filename} (文件ID: {file_id})")

        try:
            # 处理文件
            result = self.process_file_with_uploaded_id(file_id, filename, str(pdf_file))

            # 记录处理结果
            process_duration = time.time() - process_start_time
            if result:
                file_only_logger.info(f"文件处理成功: {filename} (耗时: {process_duration:.2f}秒)")
                # 更新文件状态
                with self.lock:
                    self.file_status[pdf_file] = "completed"
            else:
                logger.warning(f"文件处理失败: {filename} (耗时: {process_duration:.2f}秒)")
                with self.lock:
                    self.file_status[pdf_file] = "failed"

            return result

        except Exception as e:
            # 记录处理异常
            process_duration = time.time() - process_start_time
            logger.error(f"处理文件异常: {filename} (耗时: {process_duration:.2f}秒) - 错误: {str(e)}", exc_info=True)
            with self.lock:
                self.file_status[pdf_file] = "error"
            return None

        finally:
            # 清理上传的文件
            try:
                self._cleanup_single_file(file_id)
                file_only_logger.debug(f"已清理上传文件: {filename} (文件ID: {file_id})")
            except Exception as e:
                logger.error(f"清理上传文件失败: {filename} (文件ID: {file_id}) - 错误: {str(e)}")

            # 从上传文件ID字典中移除
            with self.lock:
                self.uploaded_file_ids.pop(pdf_file, None)

    def _cleanup_single_file(self, file_id: str):
        """清理单个上传的文件"""
        try:
            get_ai_service().delete_file(file_id)
        except Exception as e:
            pass

    def _upload_single_file_with_timeout(self, pdf_file: Path) -> Optional[str]:
        """
        上传单个PDF文件到AI平台（简化的超时控制和重试机制）
        
        参数:
            pdf_file: PDF文件路径
            
        返回:
            file_id: 上传成功后的文件ID，失败返回None
        """
        max_retries = 1  # 减少重试次数，因为AI服务内部已有重试机制
        
        for retry_count in range(max_retries + 1):
            try:
                # 上传PDF文件
                file_id = get_ai_service().upload_file(pdf_file)

                # 存储上传的文件ID，用于后续清理
                with self.lock:
                    self.uploaded_file_ids[pdf_file] = file_id
                
                # 只在第一次尝试成功时记录日志
                if retry_count == 0:
                    file_size = pdf_file.stat().st_size / 1024 / 1024
                    file_only_logger.info(f"上传文件: {pdf_file.name} (大小: {file_size:.2f} MB, ID: {file_id})")

                return file_id

            except Exception as e:
                # 只在最后一次尝试失败时记录错误
                if retry_count == max_retries:
                    logger.error(f"文件上传失败: {pdf_file.name} - 错误: {str(e)}")
                    return None
                
                # 简化重试等待时间
                time.sleep(1)  # 固定等待1秒

    def process_file_with_uploaded_id(self, file_id: str, filename: str, file_path: str = "") -> Optional[Dict[str, Any]]:
        """处理单个PDF文件 - 简化错误处理和恢复机制"""
        try:
            # 从文件名提取股票代码和发布日期
            stock_code, publish_date, original_filename = self._parse_filename(filename)
            if not stock_code or not publish_date:
                logger.error(f"文件名格式错误: {filename}")
                return None

            # 使用AI服务提取数据 - 简化重试机制
            max_retries = 1
            ai_data_results = None
            
            for retry_count in range(max_retries + 1):
                try:
                    ai_data_results = get_ai_service().extract_data_from_file(file_id, self.load_prompt_from_md())
                    if ai_data_results and ai_data_results.get('extracted_data'):
                        break
                except Exception as e:
                    if retry_count == max_retries:
                        logger.error(f"AI数据提取失败: {filename} - {str(e)}")
                        return None
                    time.sleep(1)  # 简化重试等待
            
            if not ai_data_results or not ai_data_results.get('extracted_data'):
                logger.error(f"AI数据提取返回空结果: {filename}")
                return None
                
            ai_datas = ai_data_results.get('extracted_data')

            # 将AI提取的JSON数据保存到日志文件 - 简化错误处理
            try:
                log_dir = get_log_dir()
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== {filename} ===\n")
                    f.write(f"{json.dumps(ai_datas, ensure_ascii=False, indent=2)}\n")
            except Exception:
                pass  # 忽略日志保存错误，不影响主流程

            # 预处理AI提取的JSON数据
            ai_datas = self._processed_ai_data(ai_datas)
            # 查询数据库数据
            sql_datas = self._query_database(stock_code, publish_date)
            # 预处理数据库查询结果
            sql_datas = self._processed_sql_data(sql_datas)
            # 比对数据
            comparison_result = self._compare_data_with_keys(ai_datas, sql_datas, stock_code, publish_date, original_filename)
            # 生成处理结果
            result = {
                "stock_code": stock_code,
                "publish_date": publish_date,
                "file_path": file_path,
                "ai_datas": ai_datas,
                "sql_data": sql_datas,
                "comparison_result": comparison_result
            }

            # 记录处理成功
            logger.info(f"处理成功: {filename}")
            return result

        except Exception as e:
            # 记录错误信息
            logger.error(f"处理文件异常: {filename} - 错误: {str(e)}")
            return None

    def _match_performance_object_code(self, original_name: str) -> str:
        """
        根据原始名称匹配业绩对象编码
        
        Args:
            original_name: 预计对象原始名称
            
        Returns:
            匹配到的业绩对象编码，未匹配返回空字符串
        """
        if not original_name:
            return ""
        
        # 初步判断业绩类型，减少遍历范围
        target_type = ""
        # 优先检查每股收益，避免被收字误判
        if "每股" in original_name:
            target_type = "eps"
        # 然后检查收入，使用更精确的关键词
        elif any(keyword in original_name for keyword in ["收入", "营收"]):
            target_type = "income"
        # 最后检查利润
        elif any(keyword in original_name and keyword not in ["每股"] for keyword in ["利润", "亏损"]):
            target_type = "profit"
        else:
            target_type = "other"
        
        # 根据类型过滤业绩对象配置
        filtered_objects = [obj for obj in performance_objects if target_type == "" or obj.type == target_type]
        
        # 遍历过滤后的业绩对象配置，寻找匹配项
        for perf_obj in filtered_objects:
            # 检查排除关键词
            exclude_match = any(keyword in original_name for keyword in perf_obj.exclude_keywords)
            if exclude_match:
                continue
            
            # 使用正则匹配检查包含模式
            include_match = re.search(perf_obj.include_pattern, original_name)
            if include_match:
                return perf_obj.code
        
        return ""

    def _filter_ai_data_by_priority(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        根据优先级和必做标志过滤AI提取的数据
        
        过滤规则：
        1. 过滤无业绩预计对象编码或未匹配到配置的数据
        2. 按指标类型分组
        3. 对每个指标类型：
           - 如果存在"必做"的数据，保留所有"必做"数据
           - 如果不存在"必做"数据，只保留优先级最高的一个"非必做"数据
        
        Args:
            ai_datas: AI提取的数据列表（已包含业绩预计对象编码）
            
        Returns:
            过滤后的数据列表
        """
        # 创建业绩对象编码到配置的映射
        code_to_config = {obj.code: obj for obj in performance_objects}
        
        # 按指标类型分组
        type_groups = {}
        for data in ai_datas:
            code = data.get("业绩预计对象", "")
            if not code or code not in code_to_config:
                # 无业绩预计对象编码或未匹配到配置的数据，过滤
                continue
            
            config = code_to_config[code]
            type_groups.setdefault(config.type, []).append(data)
        
        # 对每个指标类型应用过滤逻辑
        filtered_datas = []
        for type_name, type_datas in type_groups.items():
            
            # 分离必做和非必做数据
            required_datas = []
            optional_datas = []
            
            for data in type_datas:
                code = data.get("业绩预计对象", "")
                if code in code_to_config and code_to_config[code].is_required:
                    required_datas.append(data)
                else:
                    optional_datas.append(data)
            
            # 如果存在必做数据，保留所有必做数据
            if required_datas:
                filtered_datas.extend(required_datas)
            # 如果不存在必做数据，只保留优先级最高的一个非必做数据
            elif optional_datas:
                # 按优先级排序
                optional_datas_with_priority = []
                for data in optional_datas:
                    code = data.get("业绩预计对象", "")
                    priority = code_to_config[code].priority if code in code_to_config else 999
                    optional_datas_with_priority.append((priority, data))
                
                # 排序并取优先级最高的一个
                optional_datas_with_priority.sort(key=lambda x: x[0])
                filtered_datas.append(optional_datas_with_priority[0][1])
        
        return filtered_datas

    def _validate_and_filter_data(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        校验和过滤数据
        
        过滤规则：
        1. 如果只有上年同期额字段有值，则过滤掉该条数据
        2. 如果业绩预计对象为"CBS00000004W"，则清空预计幅度起始/截止、起始/截止增减额、上年同期额字段
        3. 如果业绩预计对象为每股收益，则单位为"元"
        4. 如果预计起始额大于预计截止额，替换预计起始额和预计截止额
        
        Args:
            ai_datas: AI提取的数据列表
            
        Returns:
            校验过滤后的数据列表
        """
        validated_datas = []
        
        for data in ai_datas:
            data_copy = data.copy()
            
            # 规则1：如果除上年同期额字段外其他字段都为空，则过滤掉该条数据
            fields_to_check = ["预计幅度起始", "预计幅度截止", "起始增减额", "截止增减额", "预计起始额", "预计截止额"]
            if not any(str(data_copy.get(field, "")).strip() for field in fields_to_check):
                continue
            
            # 规则2：如果业绩预计对象为归母所有者权益，则清空特定字段
            if str(data_copy.get("业绩预计对象", "")).strip() == "CBS00000004W":
                data_copy["预计幅度起始"] = ""
                data_copy["预计幅度截止"] = ""
                data_copy["起始增减额"] = ""
                data_copy["截止增减额"] = ""
                data_copy["上年同期额"] = ""

            # 规则3：如果业绩预计对象为每股收益，则单位为"元"
            if str(data_copy.get("业绩预计对象", "")).strip() in ["CIS00000003G", "CIS00000002U", "CFI0000000K1", "CFI00000002W"] and str(data_copy.get("单位", "")).strip() != "元":
                data_copy["单位"] = "元"

            validated_datas.append(data_copy)
        
        return validated_datas

    def _processed_ai_data(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """预处理AI提取的JSON数据"""
        processed_datas = []
        dw_units = ["亿元", "万元", "元"]

        for data in ai_datas:

            # 创建数据副本，避免修改原始数据
            processed_data = data.copy()

            # 1. 处理业绩预计对象编码
            # 获取业绩对象原始名称
            original_name = self._normalize_string(processed_data.get("预计对象原始名称", ""))
            # 匹配业绩对象编码
            matched_code = self._match_performance_object_code(original_name)
            # 赋值匹配结果
            processed_data["业绩预计对象"] = matched_code

            ## 将字段转换为数值类型
            fdqs = self._preprocess_shareholder_value(processed_data.get("预计幅度起始", ""))
            fdjz = self._preprocess_shareholder_value(processed_data.get("预计幅度截止", ""))
            zjeqs = self._preprocess_shareholder_value(processed_data.get("起始增减额", ""))
            zjejz = self._preprocess_shareholder_value(processed_data.get("截止增减额", ""))
            yjqs = self._preprocess_shareholder_value(processed_data.get("预计起始额", ""))
            yjjz = self._preprocess_shareholder_value(processed_data.get("预计截止额", ""))
            sntq = self._preprocess_shareholder_value(processed_data.get("上年同期额", ""))

            # 2. 校验起始、截止额符号是否正确
            # 幅度起始应为正值
            if fdqs and fdqs < 0 and ((yjqs and sntq and yjqs > sntq) or (zjeqs and zjeqs > 0)):
                fdqs = -fdqs
                processed_data["预计幅度起始"] = str(-fdqs)
            # 幅度截止应为正值
            if fdjz and fdjz < 0 and ((yjjz and sntq and yjjz > sntq) or (zjejz and zjejz > 0)):
                fdjz = -fdjz
                processed_data["预计幅度截止"] = str(-fdjz)
            # 幅度起始应为负值
            if fdqs and fdqs > 0 and ((yjqs and sntq and yjqs < sntq) or (zjeqs and zjeqs < 0)):
                fdqs = -fdqs
                processed_data["预计幅度起始"] = str(-fdqs)
            # 幅度截止应为负值
            if fdjz and fdjz > 0 and ((yjjz and sntq and yjjz < sntq) or (zjejz and zjejz < 0)):
                fdjz = -fdjz
                processed_data["预计幅度截止"] = str(-fdjz)
            # 增减额起始应为正值
            if zjeqs and zjeqs < 0 and ((yjqs and sntq and yjqs > sntq) or (fdqs and fdqs > 0)):
                zjeqs = -zjeqs
                processed_data["起始增减额"] = str(-zjeqs)
            # 增减额截止应为正值
            if zjejz and zjejz < 0 and ((yjjz and sntq and yjjz > sntq) or (fdjz and fdjz > 0)):
                zjejz = -zjejz
                processed_data["截止增减额"] = str(-zjejz)
            # 增减额起始应为负值
            if zjeqs and zjeqs > 0 and ((yjqs and sntq and yjqs < sntq) or (fdqs and fdqs < 0)):
                zjeqs = -zjeqs
                processed_data["起始增减额"] = str(-zjeqs)
            # 增减额截止应为负值
            if zjejz and zjejz > 0 and ((yjjz and sntq and yjjz < sntq) or (fdjz and fdjz < 0)):
                zjejz = -zjejz
                processed_data["截止增减额"] = str(-zjejz)
            
            # 3. 起始额大于截止额，替换起始额和截止额
            if yjqs and yjjz and yjqs > yjjz:
                tmp_yj = yjjz
                yjjz = yjqs
                yjqs = tmp_yj
                processed_data["预计起始额"] = str(yjqs)
                processed_data["预计截止额"] = str(yjjz)
            if zjeqs and zjejz and zjeqs > zjejz:
                tmp_zje = zjejz
                zjejz = zjeqs
                zjeqs = tmp_zje
                processed_data["起始增减额"] = str(zjeqs)
                processed_data["截止增减额"] = str(zjejz)
            if fdqs and fdjz and fdqs > fdjz:
                tmp_fd = fdjz
                fdjz = fdqs
                fdqs = tmp_fd
                processed_data["预计幅度起始"] = str(fdqs)
                processed_data["预计幅度截止"] = str(fdjz)

            # 4. 检查sntq的小数位数，如果超过3位则将所有数值乘以10000
            if sntq and processed_data.get("业绩预计对象", "") not in ['CIS00000003G', 'CIS00000002U', 'CFI0000000K1', 'CFI00000002W']:
                sntq_str = str(sntq)
                if '.' in sntq_str and len(sntq_str.split('.')[1]) > 3:
                    if zjeqs:
                        processed_data["起始增减额"] = str(zjeqs * 10000)
                    if zjejz:
                        processed_data["截止增减额"] = str(zjejz * 10000)
                    if yjqs:
                        processed_data["预计起始额"] = str(yjqs * 10000)
                    if yjjz:
                        processed_data["预计截止额"] = str(yjjz * 10000)
                    if sntq:
                        processed_data["上年同期额"] = str(sntq * 10000)
                    
                    # 单位转换
                    try:
                        if dw_units and processed_data.get("单位", "") in dw_units:
                            unit_index = dw_units.index(processed_data.get("单位", ""))
                            if unit_index + 1 < len(dw_units):
                                processed_data["单位"] = dw_units[unit_index + 1]
                    except:
                        pass

            processed_datas.append(processed_data)
        
        # 根据优先级和必做标志过滤数据
        filtered_datas = self._filter_ai_data_by_priority(processed_datas)
        
        # 根据数据和校验过滤数据
        validated_datas = self._validate_and_filter_data(filtered_datas)
        
        return validated_datas

    def _processed_sql_data(self, sql_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """预处理SQL数据"""
        processed_datas = []

        for data in sql_datas:
            # 创建数据副本，避免修改原始数据
            processed_data = data.copy()

            # 1.数值字段都为空则过滤掉该条数据(提示性公告的经营数据)
            fields_to_check = ["预计幅度起始", "预计幅度截止", "起始增减额", "截止增减额", "预计起始额", "预计截止额"]
            if not any(str(processed_data.get(field, "")).strip() for field in fields_to_check):
                continue

            # 2.原始名称字段包含符号则过滤掉该条数据(影子库错误数据，无需比对)
            if any(char in processed_data.get("预计对象原始名称", "") for char in ["，", ",", "、", ":", "：", "[0-9]", "?", "？", "-"]):
                continue

            processed_datas.append(processed_data)

        return processed_datas

    def _normalize_string(self, text: str) -> str:
        """
        标准化字符串，用于内部比对和主键构建
        功能：去除所有空格
        
        Args:
            text: 需要标准化的字符串
            
        Returns:
            标准化后的字符串
        """
        if not text:
            return ""
        
        # 转换为字符串
        normalized = str(text).strip()
        # 去除所有空格
        normalized = normalized.replace(' ', '')
        
        return normalized

    def _query_database(self, stock_code: str, publish_date: str) -> Optional[List[Dict[str, Any]]]:
        """查询数据库获取业绩预告数据"""
        try:
            # 使用新的SQL查询语句
            sql_data = get_db_manager().execute_query(SQL_QUERY, (stock_code, publish_date))

            if not sql_data:
                file_only_logger.info(f"未找到数据: {stock_code} {publish_date}")
                return None

            # 将所有记录转换为字典列表，并处理字段名
            results = []
            for record in sql_data:
                result = {}
                for key, value in record.items():
                    result[key] = value if value is not None else ""
                results.append(result)

            # 记录查询成功
            file_only_logger.info(f"找到数据: {stock_code} {publish_date} ({len(results)}条)")
            return results

        except Exception as e:
            logger.error(f"数据库查询失败: {stock_code} {publish_date} - 错误: {str(e)}")
            return None

    def _parse_filename(self, filename: str) -> tuple:
        """解析文件名，提取股票代码、信息发布日期、原始文件名"""
        try:
            base_name = os.path.splitext(filename)[0]
            parts = base_name.split('-')

            if len(parts) >= 4:
                stock_code = parts[0]
                publish_date = '-'.join(parts[1: 4])
                original_filename = '-'.join(parts[5:])

                # 验证日期格式
                try:
                    datetime.strptime(publish_date, '%Y-%m-%d')
                except ValueError:
                    print(f"日期格式错误: {publish_date}")
                    return None, None, None

                return stock_code, publish_date, original_filename
            else:
                print(f"文件名格式错误: {filename}")
                return None, None, None

        except Exception as e:
            print(f"解析文件名异常: {e}")
            return None, None, None

    def _compare_values(self, value1: Any, value2: Any) -> bool:
        """比较两个值是否相等"""
        try:
            # 处理空值
            if not value1 and not value2:
                return True
            if not value1 or not value2:
                return False

            # 转换为字符串并使用标准化处理（去除所有空格，转换为小写）
            str_value1 = self._normalize_string(value1)
            str_value2 = self._normalize_string(value2)

            return str_value1 == str_value2
        except Exception:
            return False

    def _compare_data_with_keys(self, ai_datas: Union[Dict[str, Any], List[Dict[str, Any]]],
                                sql_data: List[Dict[str, Any]],
                                stock_code: str, publish_date: str, original_filename: str) -> List[Dict[str, Any]]:
        """
        使用多维度主键进行业绩预告数据比对，提升匹配准确性
        主键策略：
        
        Args:
            ai_datas: AI提取的数据（可能是单个字典或字典列表）
            sql_data: SQL查询的数据列表
            stock_code: 股票代码
            publish_date: 信息发布日期
            
        Returns:
            比对结果列表
        """
        results = []
        ai_data_list = ai_datas if isinstance(ai_datas, list) else [ai_datas]

        # 提取公共字段获取逻辑
        def extract_core_fields(data: Dict[str, Any]) -> tuple:
            """提取核心字段：日期范围和预计对象代码"""
            qsrq = str(data.get("预计起始日期", "")).strip()
            jzrq = str(data.get("预计截止日期", "")).strip()
            yjdx = str(data.get("业绩预计对象", "")).strip()
            return qsrq, jzrq, yjdx

        def extract_business_fields(data: Dict[str, Any]) -> dict:
            """提取业务字段"""
            return {
                "预计对象原始名称": str(data.get("预计对象原始名称", "")).strip(),
                "预计幅度起始": str(data.get("预计幅度起始", "")).strip(),
                "预计幅度截止": str(data.get("预计幅度截止", "")).strip(),
                "起始增减额": str(data.get("起始增减额", "")).strip(),
                "截止增减额": str(data.get("截止增减额", "")).strip(),
                "预计起始额": str(data.get("预计起始额", "")).strip(),
                "预计截止额": str(data.get("预计截止额", "")).strip(),
                "上年同期额": str(data.get("上年同期额", "")).strip(),
                "单位": str(data.get("单位", "")).strip()
            }

        def build_result(stock_code: str, publish_date: str, core_fields: tuple, 
                        data_fields: dict, comparison_result: str) -> dict:
            """构建比对结果"""
            qsrq, jzrq, yjdx = core_fields
            return {
                # 核心字段
                "公告标题": original_filename,
                "股票代码": stock_code,
                "信息发布日期": publish_date,
                "预计起始日期": qsrq,
                "预计截止日期": jzrq,
                "预计对象原始名称": data_fields.get("预计对象原始名称", ""),
                "预计幅度起始": data_fields.get("预计幅度起始", ""),
                "预计幅度截止": data_fields.get("预计幅度截止", ""),
                "起始增减额": data_fields.get("起始增减额", ""),
                "截止增减额": data_fields.get("截止增减额", ""),
                "预计起始额": data_fields.get("预计起始额", ""),
                "预计截止额": data_fields.get("预计截止额", ""),
                "上年同期额": data_fields.get("上年同期额", ""),
                "单位": data_fields.get("单位", ""),
                # 比对结果列
                "比对结果": comparison_result
            }

        # 1. SQL数据为空处理 - AI有SQL无的情况
        if not sql_data:
            for ai_data in ai_data_list:
                core_fields = extract_core_fields(ai_data)
                ai_business_fields = extract_business_fields(ai_data)
                results.append(build_result(stock_code, publish_date, core_fields, 
                                          ai_business_fields, "SQL查询为空，请检查代码和信息发布日期！"))
            return results

        # 2. 构建SQL数据索引
        sql_data_by_composite_key = {}
        for record in sql_data:

            # 使用SQL记录本身的核心字段
            core_fields = extract_core_fields(record)
            if core_fields not in sql_data_by_composite_key:
                sql_data_by_composite_key[core_fields] = []
            sql_data_by_composite_key[core_fields].append(record)


        matched_sql_keys = set()

        # 3. 处理AI数据匹配
        for ai_data in ai_data_list:
            ai_core_fields = extract_core_fields(ai_data)

            # 提取AI数据的核心字段
            ai_qsrq, ai_jzrq, ai_yjdx = ai_core_fields

            if ai_core_fields not in sql_data_by_composite_key:
                ai_business_fields = extract_business_fields(ai_data)
                results.append(build_result(stock_code, publish_date, ai_core_fields,
                                          ai_business_fields, f"漏处理【{ai_qsrq}~{ai_jzrq}: {ai_data.get('预计对象原始名称', '')}】"))
            else:
                # 匹配成功
                matched_sql_keys.add(ai_core_fields)
                for sql_record in sql_data_by_composite_key[ai_core_fields]:
                    sql_business_fields = extract_business_fields(sql_record)
                    comparison_result = self._compare_shareholder_transfer_fields(ai_data, sql_record)
                    results.append(build_result(stock_code, publish_date, ai_core_fields, 
                                              sql_business_fields, comparison_result))

        # 4. SQL有AI无的情况（疑似多处理）
        for sql_record in sql_data:
            sql_core_fields = extract_core_fields(sql_record)
            # 提取SQL数据的核心字段
            sql_qsrq, sql_jzrq, sql_yjdx = sql_core_fields

            if sql_core_fields not in matched_sql_keys:
                sql_business_fields = extract_business_fields(sql_record)
                results.append(build_result(stock_code, publish_date, sql_core_fields,
                                          sql_business_fields, f"多处理【{sql_qsrq}~{sql_jzrq}: {sql_record.get('预计对象原始名称', '')}】"))


        return results

    def _compare_shareholder_transfer_fields(self, ai_data: Dict[str, Any], sql_data: Dict[str, Any]) -> str:
        """
        比较业绩预告业务AI数据和SQL数据的字段，并返回格式化的比对结果
        
        Args:
            ai_data: AI提取的业绩预告数据
            sql_data: SQL查询的业绩预告数据
            
        Returns:
            格式化的比对结果字符串
        """
        error_messages = []

        # 定义需要比对的业绩预告字段列表 (AI字段名, SQL字段名)
        primary_fields = [
            ("单位", "单位")
        ]
        # 补充信息字段
        supplementary_fields = [
            ("预计幅度起始", "预计幅度起始"),
            ("预计幅度截止", "预计幅度截止"),
            ("起始增减额", "起始增减额"),
            ("截止增减额", "截止增减额"),
            ("预计起始额", "预计起始额"),
            ("预计截止额", "预计截止额"),
            ("上年同期额", "上年同期额")
        ]

        # 合并所有比对字段
        fields_to_compare = primary_fields + supplementary_fields

        # 对每个字段进行比对
        for ai_field_name, sql_field_name in fields_to_compare:
            ai_value = ai_data.get(ai_field_name, "")
            sql_value = sql_data.get(sql_field_name, "")

            # 预处理AI值和SQL值
            processed_ai_value = self._preprocess_shareholder_value(ai_value)
            processed_sql_value = self._preprocess_shareholder_value(sql_value)

            # 比较值
            if not self._compare_values(processed_ai_value, processed_sql_value) and processed_ai_value:
                error_messages.append(f"{ai_field_name}错误【AI：{ai_value}】")

        # 返回拼接的错误信息
        return "；".join(error_messages)

    def _preprocess_shareholder_value(self, value: Any) -> Any:
        """
        预处理业绩预告数据的值，处理AI返回的数据类型不稳定的问题
        
        Args:
            value: 需要预处理的值
            
        Returns:
            预处理后的值
        """
        if value is None:
            return ""

        # 转换为字符串，处理decimal.Decimal类型
        str_value = str(value).strip()

        # 如果是空字符串，返回空字符串
        if not str_value:
            return ""

        # 去除千分位符号
        str_value = str_value.replace(',', '')

        # 处理数值类型
        if self._is_numeric_value(str_value):
            if '%' in str_value:
                str_value = str_value.replace("%", "")
            
            # 尝试转换为浮点数
            try:
                return float(str_value)
            except ValueError:
                # 如果转换失败，返回原字符串
                return str_value

        # 返回处理后的字符串
        return str_value

    def _is_numeric_value(self, value: Any) -> bool:
        """
        判断值是否为数值类型
        
        Args:
            value: 需要判断的值
            
        Returns:
            是否为数值类型
        """
        if not value:
            return False

        # 确保value是字符串类型
        str_value = str(value)
        
        # 检查是否包含数字
        has_digit = any(c.isdigit() for c in str_value)

        # 检查是否包含数值相关的字符
        numeric_chars = {".", "-", "+", "e", "E", "%"}
        has_numeric_char = any(c in numeric_chars for c in str_value)

        return has_digit and (has_numeric_char or str_value.isdigit())

    def load_prompt_from_md(self, md_file_path: str = "业绩预告优化.md") -> str:
        """从MD文件加载提示词"""
        try:
            # 使用路径工具获取提示词文件的绝对路径
            abs_md_path = get_resource_path(md_file_path)

            if os.path.exists(abs_md_path):
                with open(abs_md_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                logger.warning(f"提示词文件不存在: {abs_md_path}")
                return ""
        except Exception as e:
            logger.error(f"加载提示词文件失败: {e}")
            return ""

    def highlight_keywords_in_pdf(self, pdf_path: str, keywords: List[str] = None) -> bool:
        """在PDF中高亮关键字
        
        Args:
            pdf_path: PDF文件路径
            keywords: 需要高亮的关键字列表，默认为['收入', '利润', '非经常性', '非经营性', '毛利', '追溯', '调整', '重述', '重组', '每股', '净资产', '所有者权益']
        
        Returns:
            bool: 是否成功高亮
        """
        if keywords is None:
            keywords = ['收入', '利润', '非经常性', '非经营性', '毛利', '追溯', '调整', '重述', '重组', '每股', '净资产', '所有者权益']
        
        try:
            if not os.path.exists(pdf_path):
                logger.warning(f"PDF文件不存在: {pdf_path}")
                return False
            
            doc = fitz.open(pdf_path)
            highlight_count = 0
            
            for page in doc:
                for keyword in keywords:
                    text_instances = page.search_for(keyword)
                    for inst in text_instances:
                        highlight = page.add_highlight_annot(inst)
                        highlight.set_colors(stroke=(1, 1, 0))
                        highlight.update()
                        highlight_count += 1
            
            if highlight_count > 0:
                # 创建临时文件
                temp_path = pdf_path + '.tmp'
                doc.save(temp_path)
                doc.close()
                
                # 替换原文件
                os.replace(temp_path, pdf_path)
                logger.info(f"PDF高亮成功: {pdf_path}, 高亮数量: {highlight_count}")
            else:
                doc.close()
                logger.info(f"PDF中未找到关键字: {pdf_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"PDF高亮失败: {pdf_path}, 错误: {e}")
            return False

    def generate_report(self, session_id, results: List[Dict[str, Any]], report_file: str = None) -> str:
        """生成比对报告"""
        if not results:
            print("没有可生成报告的数据")
            return ""

        # 使用路径工具获取报告目录
        report_dir = get_report_dir()

        if not report_file:
            report_file = os.path.join(report_dir, f"业绩预告比对报告_{session_id}.xlsx")

        try:
            # 为所有PDF文件添加关键字高亮
            print("正在为PDF文件添加关键字高亮...")
            highlight_count = 0
            for result in results:
                file_path = result.get("file_path", "")
                if file_path and file_path.lower().endswith('.pdf'):
                    if self.highlight_keywords_in_pdf(file_path):
                        highlight_count += 1
            
            print(f"PDF高亮完成，共处理 {highlight_count} 个文件")

            # 创建Excel工作簿
            with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
                # 创建比对结果表
                self._create_comparison_sheet(results, writer)

            print(f"报告已生成: {report_file}")
            return report_file

        except Exception as e:
            print(f"生成报告失败: {e}")
            return ""

    def _create_comparison_sheet(self, results: List[Dict[str, Any]], writer: pd.ExcelWriter):
        """创建比对结果表"""
        comparison_data = []

        for result in results:
            # 获取比对结果列表
            comparison_results = result.get("comparison_result", [])
            file_path = result.get("file_path", "")

            # 如果没有比对结果数据，则跳过
            if not comparison_results:
                continue

            # 比对结果是一个列表，包含一个或多个比对结果
            for comparison in comparison_results:
                comparison_data.append({
                    "公告标题": comparison.get("公告标题", ""),
                    "股票代码": comparison.get("股票代码", ""),
                    "信息发布日期": comparison.get("信息发布日期", ""),
                    "预计起始日期": comparison.get("预计起始日期", ""),
                    "预计截止日期": comparison.get("预计截止日期", ""),
                    "预计对象原始名称": comparison.get("预计对象原始名称", ""),
                    "预计幅度起始": comparison.get("预计幅度起始", ""),
                    "预计幅度截止": comparison.get("预计幅度截止", ""),
                    "起始增减额": comparison.get("起始增减额", ""),
                    "截止增减额": comparison.get("截止增减额", ""),
                    "预计起始额": comparison.get("预计起始额", ""),
                    "预计截止额": comparison.get("预计截止额", ""),
                    "上年同期额": comparison.get("上年同期额", ""),
                    "单位": comparison.get("单位", ""),
                    "比对结果": comparison.get("比对结果", ""),
                    "文件路径": file_path
                })

        # 创建DataFrame并写入Excel
        df = pd.DataFrame(comparison_data)
        df.to_excel(writer, sheet_name="比对结果", index=False)

        # 获取工作表并设置超链接
        worksheet = writer.sheets["比对结果"]

        # 为股票代码列添加超链接
        for idx, row in df.iterrows():
            file_path = row.get("文件路径", "")
            file_name = row.get("公告标题", "")
            
            if file_path and file_name:
                # 将文件路径转换为绝对路径并添加file://前缀
                abs_path = os.path.abspath(file_path)
                hyperlink = f"file:///{abs_path.replace('\\', '/')}"
                
                # 设置超链接（从第2行开始，第1列）
                cell = worksheet.cell(row=idx + 2, column=1)
                cell.hyperlink = hyperlink
                cell.value = file_name

        # 根据列名删除文件路径列（不需要在Excel中显示）
        if "文件路径" in df.columns:
            col_idx = df.columns.get_loc("文件路径") + 1  # +1因为Excel列索引从1开始
            worksheet.delete_cols(col_idx)

        # 设置列宽
        column_widths = {
            "公告标题": 30,
            "信息发布日期": 12,
            "预计起始日期": 12,
            "预计截止日期": 12,
            "预计对象原始名称": 25,
        }

        # 应用列宽设置
        for col_idx, col_name in enumerate(df.columns, 1):
            if col_name in column_widths:
                # 获取列字母（A, B, C...）
                col_letter = openpyxl.utils.get_column_letter(col_idx)
                # 设置列宽
                worksheet.column_dimensions[col_letter].width = column_widths[col_name]

def main():
    """主函数 - 优化版，增强错误处理和进度显示"""
    print("=" * 60)
    print("业绩预告AI比对系统")
    print("=" * 60)

    try:
        processor = EnhancedDataProcessor()

        while True:
            print("\n请选择操作:")
            print("1. 下载公告并处理")
            print("2. 处理指定目录文件")
            print("3. 退出")

            choice = input("\n请输入选项 (1-3): ").strip()

            if choice == "1":
                # 下载公告并处理
                reset_session_id()
                # 重新配置日志，使用新的session_id
                setup_logging()
                # 重新初始化日志器
                init_logging()
                download_manager = DownloadManager()
                
                # 从Excel文件获取MD5
                hashcodes = download_manager.get_hashcodes()
                if not hashcodes:
                    print("未从Excel文件获取到有效MD5，返回主菜单")
                    continue
                
                # 创建下载目录
                download_dir = download_manager.create_download_dir()
                
                # 下载文件
                print(f"\n开始下载 {len(hashcodes)} 个文件...")
                download_success = download_manager.download_files(hashcodes, download_dir)
                
                if download_success:
                    # 处理下载的文件
                    print("\n开始处理下载的文件...")
                    pdf_files = list(Path(download_dir).glob("*.pdf"))
                    if pdf_files:
                        start_time = datetime.now()
                        results = processor.process_all_files(pdf_files)
                        end_time = datetime.now()
                        
                        # 显示处理结果
                        logger.info(f"\n\n处理完成! 共处理 {len(results)}/{len(pdf_files)} 个文件，耗时: {end_time - start_time}")
                        
                        # 生成报告
                        session_id = get_session_id()
                        if results:
                            processor.generate_report(session_id, results)
                    else:
                        print("下载目录中未找到PDF文件")
                else:
                    print("下载失败，返回主菜单")
                    
            elif choice == "2":
                
                # 显示子菜单
                print("\n请选择处理方式:")
                print("1. 处理最新下载的文件")
                print("2. 指定目录路径")
                
                sub_choice = input("请输入选项 (1-2): ").strip()
                
                if sub_choice == "1":
                    # 重置会话ID，确保每次运行生成新的会话ID
                    reset_session_id()
                    # 重新配置日志，使用新的session_id
                    setup_logging()
                    # 重新初始化日志器
                    init_logging()
                    # 处理最新下载的文件
                    try:
                        # 使用路径工具获取files文件夹路径
                        files_dir = get_files_dir()
                        
                        # 检查files文件夹是否存在
                        if not os.path.exists(files_dir):
                            print("未找到下载文件目录")
                            continue
                        
                        # 获取所有session_id目录
                        session_dirs = [d for d in os.listdir(files_dir) if os.path.isdir(os.path.join(files_dir, d))]
                        if not session_dirs:
                            print("未找到下载的文件")
                            continue
                        
                        # 找到最新的session_id目录
                        # 按修改时间排序，取最后一个
                        session_dirs.sort(key=lambda d: os.path.getmtime(os.path.join(files_dir, d)), reverse=True)
                        custom_dir = os.path.join(files_dir, session_dirs[0])
                        
                        print(f"\n将处理最新下载的文件，目录: {custom_dir}")
                    except Exception as e:
                        print(f"获取最新下载目录失败: {e}")
                        continue
                elif sub_choice == "2":
                    # 重置会话ID，确保每次运行生成新的会话ID
                    reset_session_id()
                    # 重新配置日志，使用新的session_id
                    setup_logging()
                    # 重新初始化日志器
                    init_logging()
                    # 指定目录路径，保持原有逻辑
                    custom_dir = input("请输入要处理的目录路径: ").strip()
                    if not custom_dir:
                        print("目录路径不能为空")
                        continue
                else:
                    print("无效选项，请重新选择")
                    continue

                # 验证目录是否存在
                if not os.path.exists(custom_dir):
                    print(f"目录不存在: {custom_dir}")
                    continue

                # 查找所有PDF文件
                pdf_files = list(Path(custom_dir).glob("*.pdf"))
                if not pdf_files:
                    print(f"在目录 {custom_dir} 中未找到PDF文件")
                    continue

                # 记录程序开始执行时间
                program_start_time = datetime.now()
                logger.info(f"程序开始执行 - 目录: {custom_dir}, 文件数量: {len(pdf_files)}")
                print(f"\n开始处理 {len(pdf_files)} 个文件...")

                start_time = datetime.now()
                results = processor.process_all_files(pdf_files)
                end_time = datetime.now()

                # 显示处理结果
                logger.info(f"\n\n处理完成! 共处理 {len(results)}/{len(pdf_files)} 个文件，耗时: {end_time - start_time}")

                # 显示详细处理结果统计
                success_count = 0
                failed_files = []
                with processor.lock:
                    for file_path, status in processor.file_status.items():
                        if status == "completed":
                            success_count += 1
                        else:
                            failed_files.append((file_path.name, status))
                
                print(f"成功处理: {success_count} 个文件")
                if failed_files:
                    print(f"处理失败: {len(failed_files)} 个文件")
                    for file_name, status in failed_files[:5]:  # 只显示前5个失败文件
                        print(f"  - {file_name}: {status}")
                    if len(failed_files) > 5:
                        print(f"  ... 还有 {len(failed_files) - 5} 个文件处理失败")

                # 生成报告
                if results:
                    print("\n生成处理报告...")
                    # 使用路径工具获取报告目录
                    report_dir = get_report_dir()

                    session_id = get_session_id()

                    report_file = os.path.join(report_dir,
                                               f"业绩预告比对报告_{session_id}.xlsx")
                    processor.generate_report(session_id, results, report_file)

                    # 记录程序执行结束时间和总耗时
                    program_end_time = datetime.now()
                    total_duration = program_end_time - program_start_time
                    logger.info(f"程序执行完成 - 总耗时: {total_duration}")
                    print(f"\n程序执行完成! 总耗时: {total_duration}")
                else:
                    print("\n没有成功处理的文件，请检查日志获取详细信息")

            elif choice == "3":
                print("退出程序")
                break

            else:
                print("无效选项，请重新选择")

    except KeyboardInterrupt:
        print("\n用户中断操作")
    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}", exc_info=True)
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
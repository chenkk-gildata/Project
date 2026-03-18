"""
股东股权变动AI比对系统 - 优化版本
"""
import concurrent.futures
import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import pyodbc
import requests
import traceback

from config_ShareTransfer import validate_config
from database_manager import db_manager
from ai_service_enhanced import enhanced_ai_service
from logger_config import setup_logging, get_logger, get_session_id, get_file_only_logger, reset_session_id

# 获取程序运行目录（兼容开发和打包环境）
def get_base_path():
    """获取程序基础路径，兼容开发和打包环境"""
    if getattr(sys, 'frozen', False):
        # 打包后的环境，返回exe所在目录
        return os.path.dirname(sys.executable)
    else:
        # 开发环境，返回当前文件所在目录
        return os.path.dirname(os.path.abspath(__file__))

main_dir = get_base_path()

def safe_int(value, default=0):
    """
    安全的整数转换函数，处理空值和非数字字符
    
    Args:
        value: 要转换的值
        default: 转换失败时的默认值
    
    Returns:
        int: 转换后的整数值，失败时返回默认值
    """
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

# 延迟初始化日志器
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
BASE_SQL_QUERY = '''
SELECT A.ID,B.GPDM 股票代码,CONVERT(DATE,A.GQBDQSR) 变动起始日期,CONVERT(DATE,A.GQZSBDRQ) 变动截止日期,
    A.GQCRF 出让方,A.CRQCGSL 出让前持股数量,A.CRHCGSL 出让后持股数量,A.CRHYXSGS 出让后有限售股数,A.CRHWXSGS 出让后无限售股数,
    A.GQZRF 受让方,A.SRQCGSL 受让前持股数量,A.SRHCGSL 受让后持股数量,A.SRHYXSGS 受让后有限售股数,A.SRHWXSGS 受让后无限售股数,
    A.GQBDSJGS 涉及股数,A.JYJE 交易金额,A.GQZRFS 交易方式
FROM [10.101.0.212].JYPRIME.dbo.usrGDGQBD A
 JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
  ON A.INBBM = B.INBBM AND B.ZQSC IN (83,90,18) AND B.ZQLB IN (1,2,41)
WHERE A.LBXZ = 1 AND B.GPDM = ?
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
    
    def _get_hashcodes_from_input(self) -> List[str]:
        """从命令行输入获取MD5"""
        print("\n请输入MD5，每行一个，输入完成后按Ctrl+D(或在空行按Enter)结束:")
        hashcodes = []
        
        while True:
            try:
                line = input().strip()
                if not line:
                    break
                hashcodes.append(line)
            except EOFError:
                break
        
        if not hashcodes:
            print("未输入任何MD5，返回空列表")
        return hashcodes
    
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
        main_dir = get_base_path()
        session_id = get_session_id()
        download_dir = os.path.join(main_dir, "股东股权变动小程序比对", "files", session_id)
        
        os.makedirs(download_dir, exist_ok=True)
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
    """股东股权变动数据处理类"""

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

    def _pipeline_upload_and_process(self, pdf_files: List[Path]) -> List[
        Dict[str, Any]]:
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

        result = None

        try:
            # 处理文件，传递PDF文件路径
            result = self.process_file_with_uploaded_id(file_id, filename, pdf_file)

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
            enhanced_ai_service.delete_file(file_id)
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
                file_id = enhanced_ai_service.upload_file(pdf_file)

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

    def process_file_with_uploaded_id(self, file_id: str, filename: str, pdf_file: Path) -> Optional[Dict[str, Any]]:
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
                    ai_data_results = enhanced_ai_service.extract_data_from_file(file_id, self.load_prompt_from_md())
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
                script_dir = get_base_path()
                log_dir = os.path.join(script_dir, "股东股权变动小程序比对", "logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== {filename} ===\n")
                    f.write(f"{json.dumps(ai_datas, ensure_ascii=False, indent=2)}\n")
            except Exception:
                pass  # 忽略日志保存错误，不影响主流程

            # 预处理AI提取的JSON数据
            ai_datas = self._processed_ai_data(ai_datas)

            # 保存所有AI数据的出让方和受让方信息，以组为单位
            transfer_groups = []
            for ai_data in ai_datas:
                gq_out_name = ai_data.get("出让方", "")
                gq_in_name = ai_data.get("受让方", "")
                transfer_groups.append({
                    "ai_data": ai_data,
                    "gq_out_name": gq_out_name,
                    "gq_in_name": gq_in_name
                })

            # 查询数据库 - 为每组数据分别查询，使用缓存避免重复查询
            all_sql_data = []
            # 创建缓存字典，键为(stock_code, 出让方, 受让方)，值为查询结果
            query_cache = {}
            
            for group in transfer_groups:
                # 构建缓存键
                cache_key = (stock_code, group["gq_out_name"], group["gq_in_name"])
                
                # 检查缓存中是否已存在该组合的查询结果
                if cache_key in query_cache:
                    # 存在直接跳过
                    continue
                
                try:
                    # 执行数据库查询
                    sql_datas = self._query_database(stock_code, group["gq_out_name"], group["gq_in_name"])
                    # 将查询结果的ID存入缓存
                    query_cache[cache_key] = sql_datas
                    if sql_datas:
                        all_sql_data.extend(sql_datas)
                        logger.info(
                            f"数据库查询成功: {filename} - 出让方：{group['gq_out_name']} 受让方：{group['gq_in_name']} - 共{len(sql_datas)}条记录")
                except Exception as e:
                    logger.warning(f"数据库查询失败: {filename} - 出让方：{group['gq_out_name']} 受让方：{group['gq_in_name']} - {str(e)}")
                    # 即使查询失败，也要将结果存入缓存（None），避免重复查询失败
                    query_cache[cache_key] = None
                    # 继续处理下一组数据

            # 如果没有任何SQL数据，设置为None以保持原有逻辑
            sql_datas = all_sql_data if all_sql_data else None
            
            # 第一次比对：精确主键匹配，只比对主键完全一致的数据
            first_comparison_results = []
            unmatched_ai_datas = []
            first_comparison_results, unmatched_ai_datas = self._first_compare_with_exact_keys(
                ai_datas, sql_datas, stock_code, publish_date, original_filename, pdf_file
            )

            # 第二步：对匹配失败的AI数据进行SQL合并处理
            merged_sql_data = sql_datas  # 初始化merged_sql_data
            if unmatched_ai_datas and sql_datas:
                merged_sql_data = self._merge_sql_data_by_ai_date(unmatched_ai_datas, sql_datas)

            # 第三步：将第一次比对返回的AI数据和合并后的SQL数据进行二次比对
            second_comparison_results = []
            if unmatched_ai_datas and sql_datas:
                second_comparison_results = self._compare_data_with_keys(
                    unmatched_ai_datas, merged_sql_data, stock_code, publish_date, original_filename, pdf_file
                )

            # 第四步：合并两次比对结果
            comparison_result = first_comparison_results + second_comparison_results

            # 生成处理结果，添加PDF文件路径
            result = {
                "stock_code": stock_code,
                "publish_date": publish_date,
                "ai_datas": ai_datas,
                "sql_data": merged_sql_data,  # 使用合并后的SQL数据
                "comparison_result": comparison_result,
                "pdf_path": str(pdf_file.absolute())  # 添加PDF文件绝对路径
            }

            # 记录处理成功
            logger.info(f"处理成功: {filename}")
            return result

        except Exception as e:
            # 记录错误信息
            logger.error(f"处理文件异常: {filename} - 错误: {str(e)}")
            return None

    def _processed_ai_data(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """预处理AI提取的JSON数据"""
        processed_datas = []
        is_calculate = False
        
        for data in ai_datas:
            # 创建数据副本，避免修改原始数据
            processed_data = data.copy()

            if data.get('涉及股数') == '0':
                continue
            
            # 1. 预处理出让方和受让方的名称
            # 处理出让方名称
            if "出让方" in processed_data and processed_data["出让方"]:
                # 去除多余空格（保留正常英文间的空格）
                processed_data["出让方"] = self._clean_name(processed_data["出让方"])

                if processed_data["涉及股数"] and (not processed_data["出让前持股数量"] or not processed_data["出让后持股数量"]):
                    is_calculate = True
            
            # 处理受让方名称
            if "受让方" in processed_data and processed_data["受让方"]:
                processed_data["受让方"] = self._clean_name(processed_data["受让方"])

                if processed_data["涉及股数"] and (not processed_data["受让前持股数量"] or not processed_data["受让后持股数量"]):
                    is_calculate = True

            # 2. 出让方/受让方为空，相关字段设为空
            if "出让方" in processed_data and not processed_data["出让方"]:
                processed_data["出让前持股数量"] = ""
                processed_data["出让后持股数量"] = ""
                processed_data["出让后有限售股数"] = ""
                processed_data["出让后无限售股数"] = ""
            elif "受让方" in processed_data and not processed_data["受让方"]:
                processed_data["受让前持股数量"] = ""
                processed_data["受让后持股数量"] = ""
                processed_data["受让后有限售股数"] = ""
                processed_data["受让后无限售股数"] = ""

            # 3. 变动后持股数量为空，相关字段设为空
            if "出让后持股数量" in processed_data and not processed_data["出让后持股数量"]:
                processed_data["出让后有限售股数"] = ""
                processed_data["出让后无限售股数"] = ""
            elif "受让后持股数量" in processed_data and not processed_data["受让后持股数量"]:
                processed_data["受让后有限售股数"] = ""
                processed_data["受让后无限售股数"] = ""

            # 4. 处理交易方式
            if "交易方式" in processed_data and processed_data["交易方式"]:
                trading_method = str(processed_data["交易方式"]).strip()
                
                # 检查交易方式包含的关键词
                has大宗交易 = "大宗交易" in trading_method
                has集中竞价 = "集中竞价" in trading_method or "集中交易" in trading_method
                
                # 根据关键词进行替换
                if has大宗交易 and has集中竞价:
                    # 同时包含"大宗交易"和"集中竞价"，替换为'99'
                    processed_data["交易方式"] = "99"
                elif has大宗交易:
                    # 仅包含"大宗交易"，替换为'12'
                    processed_data["交易方式"] = "12"
                elif has集中竞价:
                    # 仅包含"集中竞价"，替换为'56'
                    processed_data["交易方式"] = "56"
                elif "司法拍卖" in trading_method or "法院裁定" in trading_method:
                    processed_data["交易方式"] = "3"
                elif "协议转让" in trading_method:
                    processed_data["交易方式"] = "1"
                elif "询价" in trading_method:
                    processed_data["交易方式"] = "20"
                else:
                    # 为空默认"集中竞价"
                    processed_data["交易方式"] = "56"

            # 5. 截止日期大于当前日期+1天，则去除
            if "变动截止日期" in processed_data and processed_data["变动截止日期"]:
                if datetime.strptime(processed_data["变动截止日期"], '%Y-%m-%d').date() > datetime.now().date() + timedelta(days=1):
                    continue
                # 6. 起始日期大于截止日期，日期互换
                if "变动起始日期" in processed_data and processed_data["变动起始日期"]:
                    if (datetime.strptime(processed_data["变动起始日期"], '%Y-%m-%d').date() >
                            datetime.strptime(processed_data["变动截止日期"], '%Y-%m-%d').date()):
                        processed_data["变动起始日期"], processed_data["变动截止日期"] = processed_data[
                            "变动截止日期"], processed_data["变动起始日期"]

            # 添加处理后的数据到结果列表
            processed_datas.append(processed_data)

        # 7. 同一股东截止日期不同，变动前后股数相同，去除多余股数字段
        processed_datas, is_calculate = self._remove_duplicate_share_fields(processed_datas, is_calculate)

        # 8. 根据涉及股数计算出让前后持股数量 or 受让前后持股数量
        if is_calculate:
            processed_datas = self._processed_ai_data_shares(processed_datas)

        # 9. 合并除了交易方式外其他字段都相同的记录
        processed_datas = self._merge_ai_data_by_fields(processed_datas)

        return processed_datas

    def _remove_duplicate_share_fields(self, processed_datas: List[Dict[str, Any]], is_calculate):
        """
        同一股东截止日期不同，变动前后股数相同，去除多余股数字段
        处理出让方和受让方两种情况
        
        Args:
            processed_datas: 预处理后的数据列表
            is_calculate: 是否需要计算标识
            
        Returns:
            (processed_datas, is_calculate) 元组
        """
        if len(processed_datas) <= 1:
            return processed_datas, is_calculate
        
        # 定义两种处理模式：出让方和受让方
        modes = [
            {
                "group_field": "出让方",
                "before_field": "出让前持股数量",
                "after_field": "出让后持股数量"
            },
            {
                "group_field": "受让方",
                "before_field": "受让前持股数量",
                "after_field": "受让后持股数量"
            }
        ]
        
        for mode in modes:
            group_field = mode["group_field"]
            before_field = mode["before_field"]
            after_field = mode["after_field"]
            
            # 按分组字段分组
            groups = {}
            for data in processed_datas:
                group_key = data.get(group_field, "")
                if group_key not in groups:
                    groups[group_key] = []
                groups[group_key].append(data)
            
            # 对每组数据进行处理
            for group_key, group_data in groups.items():
                if len(group_data) < 2:
                    continue
                
                # 检查是否有出让前持股数量相等、出让后持股数量相等，但截止日期不等的记录
                # 按出让前持股数量和出让后持股数量分组
                shares_groups = {}
                for data in group_data:
                    before_shares = data.get(before_field, "")
                    after_shares = data.get(after_field, "")
                    end_date = data.get("变动截止日期", "")
                    if before_shares and after_shares and end_date:
                        shares_key = (before_shares, after_shares)
                        if shares_key not in shares_groups:
                            shares_groups[shares_key] = []
                        shares_groups[shares_key].append(data)
                
                # 对每个持股数量分组进行处理
                for shares_key, shares_records in shares_groups.items():
                    if len(shares_records) < 2:
                        continue
                    
                    # 检查截止日期是否不等
                    end_dates = set(data.get("变动截止日期", "") for data in shares_records)
                    if len(end_dates) < 2:
                        continue
                    
                    # 根据截止日期排序（从早到晚）
                    sorted_records = sorted(shares_records, key=lambda x: datetime.strptime(x.get("变动截止日期", ""), '%Y-%m-%d'))
                    
                    # 最早记录：变动后持股数置空
                    sorted_records[0][after_field] = ""
                    
                    # 最晚记录：变动前持股数置空
                    sorted_records[-1][before_field] = ""
                    
                    # 中间记录：变动前后持股数都置空
                    for i in range(1, len(sorted_records) - 1):
                        sorted_records[i][before_field] = ""
                        sorted_records[i][after_field] = ""

                    is_calculate = True
        
        return processed_datas, is_calculate

    def _processed_ai_data_shares(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """计算AI提取的股数：出让前后持股数量 or 受让前后持股数量"""
        if not ai_datas:
            return []

        processed_datas = ai_datas.copy()

        # 1. 按出让方分组，计算出让方相关字段
        out_groups = {}
        for idx, data in enumerate(processed_datas):
            out_party = data.get("出让方", "")
            if out_party:
                if out_party not in out_groups:
                    out_groups[out_party] = []
                out_groups[out_party].append(idx)

        # 按照日期倒序排列，保证计算顺序
        for out_party, indices in out_groups.items():
            group_data = [processed_datas[idx] for idx in indices]
            sorted_data = sorted(group_data, key=lambda x: (x.get("变动截止日期", ""), x.get("变动起始日期", "")), reverse=True)

            for i in range(len(sorted_data)):
                current_data = sorted_data[i]

                out_before = current_data.get("出让前持股数量")
                out_after = current_data.get("出让后持股数量")
                out_involved_shares = current_data.get("涉及股数")
            
                # 处理第一条记录
                if i == 0:
                    # 出让后持股数和涉及股数有值且满足下面任意条件
                    if (out_after and out_involved_shares and 
                        # 1. 出让前持股数为空
                        (not out_before 
                        # 2. 出让前持股数与下一条记录相同且变动截止日期不同
                        or (i + 1 < len(sorted_data) and out_before == sorted_data[i + 1].get("出让前持股数量") and current_data.get("变动截止日期") != sorted_data[i + 1].get("变动截止日期")))
                        # 3. 出让前持股数不为空，但与计算不等
                        or (out_before and out_after and out_before != str(safe_int(out_after) + safe_int(out_involved_shares)))
                        ):
                        out_before = str(safe_int(out_after) + safe_int(out_involved_shares))
                        current_data["出让前持股数量"] = out_before
                    # 计算出让后持股数
                    elif out_before and out_involved_shares and not out_after:
                        current_data["出让后持股数量"] = str(safe_int(out_before) - safe_int(out_involved_shares))
                # 处理其他记录
                else:
                    # 记录上一条记录的出让前持股数和出让后持股数
                    out_prev_before = sorted_data[i - 1].get("出让前持股数量")
                    out_prev_after = sorted_data[i - 1].get("出让后持股数量")
                    # 如果上一条的出让前持股数有值且满足下面任意条件
                    if (out_prev_before and 
                        # 1.并且出让后持股数为空
                        (not out_after
                        # 2. 出让后持股数与上一条记录相同且变动截止日期不同
                        or (out_after == out_prev_after and current_data.get("变动截止日期") != sorted_data[i - 1].get("变动截止日期")))
                        ):
                        # 本记录的出让后持股数等于上一条记录的出让前持股数
                        current_data["出让后持股数量"] = out_prev_before
                        out_after = out_prev_before
                    # 计算出让后持股数
                    elif out_before and out_involved_shares:
                        current_data["出让后持股数量"] = str(safe_int(out_before) - safe_int(out_involved_shares))
                    
                    # 出让后持股数和涉及股数有值且满足下面任意条件
                    if (out_after and out_involved_shares and 
                        # 1. 出让前持股数为空
                        (not out_before 
                        # 2. 出让前持股数与上一条记录相同且变动截止日期不同
                        or (out_before == out_prev_before and current_data.get("变动截止日期") != sorted_data[i - 1].get("变动截止日期")))
                        ):
                        # 计算出让前持股数
                        out_before = str(safe_int(out_after) + safe_int(out_involved_shares))
                        current_data["出让前持股数量"] = out_before
                    
                    # 如果本记录的变动截止日期与上一条记录相同：
                    if current_data.get("变动截止日期") == sorted_data[i - 1].get("变动截止日期"):
                        # 则本记录的出让后持股数等于上一条记录的出让后持股数
                        current_data["出让后持股数量"] = sorted_data[i - 1].get("出让后持股数量")
                        # 上一条记录的出让前持股数等于本记录的出让前持股数
                        sorted_data[i - 1]["出让前持股数量"] = current_data["出让前持股数量"]

        # 2. 按受让方分组，计算受让方相关字段
        receive_groups = {}
        for idx, data in enumerate(processed_datas):
            receive_party = data.get("受让方", "")
            if receive_party:
                if receive_party not in receive_groups:
                    receive_groups[receive_party] = []
                receive_groups[receive_party].append(idx)

        # 根据变动截止日期、变动起始日期倒序排序
        for receive_party, indices in receive_groups.items():
            group_data = [processed_datas[idx] for idx in indices]
            sorted_data = sorted(group_data, key=lambda x: (x.get("变动截止日期", ""), x.get("变动起始日期", "")), reverse=True)
            
            # 记录日期相等的记录范围
            is_equal_date = 1

            for i in range(len(sorted_data)):
                current_data = sorted_data[i]

                receive_before = current_data.get("受让前持股数量")
                receive_after = current_data.get("受让后持股数量")
                receive_involved_shares = current_data.get("涉及股数")
            
                if i == 0:
                    # 第一条数据，若受让后持股数存在且涉及股数存在且受让前持股数不存在，则计算受让前持股数
                    if receive_after and receive_involved_shares and not receive_before:
                        receive_before = str(safe_int(receive_after) - safe_int(receive_involved_shares))
                        current_data["受让前持股数量"] = receive_before
                    # 第一条数据，若受让前持股数存在且涉及股数存在且受让后持股数不存在，则计算受让后持股数
                    elif receive_before and receive_involved_shares and not receive_after:
                        current_data["受让后持股数量"] = str(safe_int(receive_before) + safe_int(receive_involved_shares))

                else:
                    # is_equal_date != 1 说明截止日期相等；is_equal_date == 1 说明截止日期不相等
                    if current_data.get("变动截止日期") == sorted_data[i - 1].get("变动截止日期"):
                        is_equal_date += 1
                    else:
                        is_equal_date = 1

                    # 非第一条数据，记录上一条数据的值，方便计算
                    receive_prev_before = sorted_data[i - 1].get("受让前持股数量")
                    receive_prev_after = sorted_data[i - 1].get("受让后持股数量")
                    
                    # 计算受让后持股数
                    if receive_prev_before and is_equal_date == 1 and receive_after != receive_prev_before:
                        current_data["受让后持股数量"] = receive_prev_before
                        receive_after = receive_prev_before
                    elif is_equal_date != 1 and receive_after != receive_prev_after:
                        current_data["受让后持股数量"] = receive_prev_after
                    elif receive_before and receive_involved_shares and not receive_after:
                        current_data["受让后持股数量"] = str(safe_int(receive_before) + safe_int(receive_involved_shares))
                    
                    # 计算受让前持股数
                    if receive_after and receive_involved_shares and is_equal_date == 1 and (not receive_before or receive_before == receive_prev_before):
                        receive_before = str(safe_int(receive_after) - safe_int(receive_involved_shares))
                        current_data["受让前持股数量"] = receive_before
                    elif is_equal_date != 1 and receive_after == receive_prev_after:
                        receive_before = str(safe_int(receive_prev_before) - safe_int(receive_involved_shares))
                        # 将变动截止日期相同的记录的受让前持股数更新为当前值
                        for j in range(i+1-is_equal_date, i+1):
                            if sorted_data[j]["受让前持股数量"] != receive_before:
                                sorted_data[j]["受让前持股数量"] = receive_before

        
        # 3. 根据计算后的持股数量判断是否需要清空有限售股数和无限售股数
        for data in processed_datas:
            if safe_int(data.get("出让后有限售股数")) + safe_int(data.get("出让后无限售股数")) != safe_int(data.get("出让后持股数量")):
                data["出让后有限售股数"] = ""
                data["出让后无限售股数"] = ""
            if safe_int(data.get("受让后有限售股数")) + safe_int(data.get("受让后无限售股数")) != safe_int(data.get("受让后持股数量")):
                data["受让后有限售股数"] = ""
                data["受让后无限售股数"] = ""

        return processed_datas

    def _merge_ai_data_by_fields(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """合并除了交易方式外其他字段都相同的记录"""
        if not ai_datas:
            return []

        merged_datas = []
        merge_groups = {}

        # 定义除了交易方式外的所有字段
        exclude_fields = {"交易方式"}

        # 按除了交易方式外的所有字段分组
        for data in ai_datas:
            # 构建分组键（排除交易方式）
            key_items = []
            for field, value in data.items():
                if field not in exclude_fields:
                    key_items.append((field, str(value) if value is not None else ""))
            
            key = tuple(key_items)
            
            if key not in merge_groups:
                merge_groups[key] = []
            merge_groups[key].append(data)

        # 合并每组数据
        for key, group_data in merge_groups.items():
            if len(group_data) == 1:
                # 只有一条记录，直接添加
                merged_datas.append(group_data[0])
            else:
                # 多条记录，合并为一条
                merged_data = group_data[0].copy()
                merged_data["交易方式"] = "56"
                merged_datas.append(merged_data)

        return merged_datas

    def _clean_name(self, name: str) -> str:
        """
        清理名称，去除多余空格并将中文符号的括号替换为英文符号的括号
        
        Args:
            name: 需要清理的名称
            
        Returns:
            清理后的名称
        """
        if not name:
            return ""
        
        # 转换为字符串并去除前后空格
        cleaned_name = str(name).strip()
        
        # 将中文符号的括号替换为英文符号的括号
        cleaned_name = cleaned_name.replace("（", "(").replace("）", ")")

        # 统一"-"为英文短横线
        cleaned_name = cleaned_name.replace("—", "-").replace("－", "-")
        cleaned_name = cleaned_name.replace("--", "-")

        # 去除多余空格（保留正常英文间的空格）
        import re
        # 使用正则表达式将连续多个空格替换为单个空格
        cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
        # 去除中文前后的空格
        cleaned_name = re.sub(r'\s+(?=[\u4e00-\u9fa5])|(?<=[\u4e00-\u9fa5])\s+', '', cleaned_name)
        
        return cleaned_name
    
    def _normalize_string(self, text: str) -> str:
        """
        标准化字符串，用于内部比对和主键构建
        功能：去除所有空格并转换为小写
        
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
        # 转换为小写
        normalized = normalized.lower()
        
        return normalized

    def _contains_english_and_space(self, text: str) -> bool:
        """
        检查字符串是否包含英文和空格
        
        Args:
            text: 要检查的字符串
            
        Returns:
            bool: 是否包含英文和空格
        """
        if not text:
            return False
        
        # 检查是否包含英文（a-z或A-Z）
        has_english = any(ord('a') <= ord(c) <= ord('z') or ord('A') <= ord(c) <= ord('Z') for c in text)
        # 检查是否包含空格
        has_space = ' ' in text
        
        return has_english and has_space
    
    def _build_dynamic_sql(self, stock_code: str, gq_out_name: str, gq_in_name: str) -> tuple:
        """
        动态构建SQL查询语句和参数
        
        Args:
            stock_code: 股票代码
            gq_out_name: 出让方名称
            gq_in_name: 受让方名称
            
        Returns:
            (sql_query, params) 元组
        """
        sql_conditions = []
        params = [stock_code]
        
        # 动态添加出让方条件
        if gq_out_name:
            if self._contains_english_and_space(gq_out_name):
                # 包含英文和空格，使用REPLACE和COLLATE
                sql_conditions.append("REPLACE(A.GQCRF,' ','') COLLATE Chinese_PRC_CI_AS = ?")
                # 同样处理参数，去除空格
                params.append(gq_out_name.replace(' ', ''))
            else:
                # 不包含英文和空格，保持原逻辑
                sql_conditions.append("A.GQCRF = ?")
                params.append(gq_out_name)
        else:
            sql_conditions.append("A.GQCRF IS NULL")
        
        # 动态添加受让方条件
        if gq_in_name:
            if self._contains_english_and_space(gq_in_name):
                # 包含英文和空格，使用REPLACE和COLLATE
                sql_conditions.append("REPLACE(A.GQZRF,' ','') COLLATE Chinese_PRC_CI_AS = ?")
                # 同样处理参数，去除空格
                params.append(gq_in_name.replace(' ', ''))
            else:
                # 不包含英文和空格，保持原逻辑
                sql_conditions.append("A.GQZRF = ?")
                params.append(gq_in_name)
        else:
            sql_conditions.append("A.GQZRF IS NULL")
        
        # 组合完整的SQL查询
        if sql_conditions:
            dynamic_sql = BASE_SQL_QUERY + " AND " + " AND ".join(sql_conditions)
        else:
            dynamic_sql = BASE_SQL_QUERY
        
        return dynamic_sql, tuple(params)

    def _query_database(self, stock_code: str, gq_out_name: str, gq_in_name: str) -> Optional[List[Dict[str, Any]]]:
        """查询数据库获取股东股权变动数据"""
        try:
            # 处理参数，确保空字符串和None都转换为空字符串
            gq_out_name = gq_out_name if gq_out_name is not None else ""
            gq_in_name = gq_in_name if gq_in_name is not None else ""
            
            # 动态构建SQL查询和参数
            dynamic_sql, params = self._build_dynamic_sql(stock_code, gq_out_name, gq_in_name)
            
            sql_data = db_manager.execute_query(dynamic_sql, params)
            if not sql_data:
                file_only_logger.info(f"未找到数据: {stock_code} 出让方：{gq_out_name} 受让方：{gq_in_name}")
                return None

            # 记录查询成功
            file_only_logger.info(f"找到数据: {stock_code} 出让方：{gq_out_name} 受让方：{gq_in_name} ({len(sql_data)}条)")
            return sql_data

        except Exception as e:
            logger.error(f"数据库查询失败: {stock_code} 出让方：{gq_out_name} 受让方：{gq_in_name} - 错误: {str(e)}")
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
            
    def _is_date_range_included(self, ai_start_date: str, ai_end_date: str, sql_start_date: str, sql_end_date: str) -> bool:
        """
        检查SQL数据的日期范围是否完全包含在AI数据的日期范围内
        
        Args:
            ai_start_date: AI数据的变动起始日期
            ai_end_date: AI数据的变动截止日期
            sql_start_date: SQL数据的变动起始日期
            sql_end_date: SQL数据的变动截止日期
            
        Returns:
            bool: SQL数据日期范围是否包含在AI数据日期范围内
        """
        try:
            # 将字符串转换为日期对象
            ai_start = datetime.strptime(ai_start_date, '%Y-%m-%d').date()
            ai_end = datetime.strptime(ai_end_date, '%Y-%m-%d').date()
            sql_start = datetime.strptime(sql_start_date, '%Y-%m-%d').date() if isinstance(sql_start_date,str) else sql_start_date
            sql_end = datetime.strptime(sql_end_date, '%Y-%m-%d').date() if isinstance(sql_end_date,str) else sql_end_date
            
            # 检查SQL日期范围是否完全包含在AI日期范围内
            return ai_start <= sql_start and sql_end <= ai_end
        except Exception:
            return False
            
    def _merge_sql_records(self, group_key: tuple, sql_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        将多条SQL数据合并为一条，按照指定规则处理各字段
        
        Args:
            group_key: 分组键，用于区分不同交易方式的记录
            sql_records: 需要合并的SQL数据列表
            
        Returns:
            合并后的SQL数据字典
        """
        if not sql_records:
            return {}
        
        if len(sql_records) == 1:
            return sql_records[0]
        
        # 1. 处理日期相关字段
        # 转换为日期对象以便比较
        date_records = []
        for record in sql_records:
            start_date = record.get("变动起始日期", "")
            end_date = record.get("变动截止日期", "")
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').date() if isinstance(start_date,str) else start_date
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').date() if isinstance(end_date,str) else end_date
                date_records.append((start_dt, end_dt, record))
            except ValueError:
                continue
        
        if not date_records:
            return sql_records[0]
        
        # 2. 找出最小起始日期和最大截止日期的记录
        # 按起始日期排序，取第一条
        date_records.sort(key=lambda x: x[0])
        min_start_record = date_records[0][2]
        min_start_date = min_start_record.get("变动起始日期", "")
        
        # 按截止日期排序，取最后一条
        date_records.sort(key=lambda x: x[1])
        max_end_record = date_records[-1][2]
        max_end_date = max_end_record.get("变动截止日期", "")

        # 3. 处理数值累加字段
        # 涉及股数相加
        total_involved_shares = 0

        for record in sql_records:
            # 交易方式相等或99
            if group_key[2] == str(record.get("交易方式", "")).strip() or group_key[2] == '99':
                shares = record.get("涉及股数", 0)
                try:
                    total_involved_shares += float(shares) if shares else ""
                except (ValueError, TypeError):
                    pass
        sjgs = total_involved_shares

        # 交易金额相加
        total_transaction_amount = 0

        for record in sql_records:
            amount = record.get("交易金额", 0)
            if amount:
                try:
                    total_transaction_amount += float(amount)
                except (ValueError, TypeError):
                    pass
            else:
                total_transaction_amount = ""
                break

        jyje = total_transaction_amount

        # 4. 处理交易方式
        jyfs_set = set(record.get("交易方式", "") for record in sql_records)
        if group_key[2] == '99':
            jyfs = '56'
        elif len(jyfs_set) == 1:
            # 所有交易方式相同，使用SQL记录的交易方式
            jyfs = sql_records[0].get("交易方式", "")
        else:
            # 交易方式不同，使用AI数据的交易方式（group_key[2]）
            jyfs = group_key[2]

        merged_record = {
             # 5. 初始化合并结果
            "变动起始日期": min_start_date,
            "变动截止日期": max_end_date,
            "交易方式": jyfs,
            "出让方": sql_records[0].get("出让方", ""),
            "受让方": sql_records[0].get("受让方", ""),
             # 6. 处理持股数量相关字段
            "出让前持股数量": min_start_record.get("出让前持股数量", ""),
            "出让后持股数量": max_end_record.get("出让后持股数量", ""),
            "出让后有限售股数": max_end_record.get("出让后有限售股数", ""),
            "出让后无限售股数": max_end_record.get("出让后无限售股数", ""),
            "受让前持股数量": min_start_record.get("受让前持股数量", ""),
            "受让后持股数量": max_end_record.get("受让后持股数量", ""),
            "受让后有限售股数": max_end_record.get("受让后有限售股数", ""),
            "受让后无限售股数": max_end_record.get("受让后无限售股数", ""),
            "涉及股数": sjgs,
            "交易金额": jyje,
            # 7. 处理交易价格均价
            "交易价格均价": "合并处理"
        }

        return merged_record
        
    def _merge_sql_data_by_ai_date(self, ai_datas: List[Dict[str, Any]], sql_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        根据AI提取数据的变动日期将SQL数据符合条件的变动日期进行合并处理
        
        Args:
            ai_datas: AI提取的数据列表
            sql_data: SQL查询的数据列表
            
        Returns:
            合并后的SQL数据列表
        """
        if not sql_data:
            return sql_data
        
        if not ai_datas:
            return sql_data
        
        merged_sql_data = []
        
        # 1. 遍历每个AI数据，处理对应的SQL数据
        for ai_data in ai_datas:

            ai_start_date = ai_data.get("变动起始日期", "")
            ai_end_date = ai_data.get("变动截止日期", "")
            ai_ggcrf = ai_data.get("出让方", "")
            ai_gqzrf = ai_data.get("受让方", "")
            ai_jyfs = ai_data.get("交易方式", "")

            if not ai_start_date or not ai_end_date:
                continue

            # 构建标准化的AI分组键
            normalized_ggcrf = self._normalize_string(ai_ggcrf)
            normalized_gqzrf = self._normalize_string(ai_gqzrf)

            group_key = (normalized_ggcrf, normalized_gqzrf, ai_jyfs)
            ai_hold_key = (normalized_ggcrf, normalized_gqzrf)  # 不根据交易方式分组，合并处理出让前后持股数量
            
            # 2. 按出让方、受让方
            sql_groups = {}
            for record in sql_data:

                # 检查日期范围是否包含
                sql_start_date = record.get("变动起始日期", "")
                sql_end_date = record.get("变动截止日期", "")

                if not sql_start_date or not sql_end_date:
                    continue

                if not self._is_date_range_included(ai_start_date, ai_end_date, sql_start_date, sql_end_date):
                    continue

                # 获取分组键
                sql_gqcrf = record.get("出让方", "")
                sql_gqzrf = record.get("受让方", "")
                
                # 构建标准化的SQL分组键
                normalized_sql_ggcrf = self._normalize_string(sql_gqcrf)
                normalized_sql_gqzrf = self._normalize_string(sql_gqzrf)
                
                # SQL分组键
                sql_hold_key = (normalized_sql_ggcrf, normalized_sql_gqzrf)

                if not ai_hold_key == sql_hold_key:
                    continue

                # 添加到对应的分组
                if group_key not in sql_groups:
                    sql_groups[group_key] = []
                sql_groups[group_key].append(record)
            
            # 3. 对每个分组进行合并处理
            for group_key, group_records in sql_groups.items():
                if len(group_records) <= 1:
                    # 单条记录不需要合并，直接添加
                    for record in group_records:
                        merged_sql_data.append(record)
                else:
                    # 多条记录需要合并
                    merged_record = self._merge_sql_records(group_key, group_records)
                    merged_sql_data.append(merged_record)
        
        return merged_sql_data

    def _first_compare_with_exact_keys(self, ai_datas: List[Dict[str, Any]],
                                        sql_data: List[Dict[str, Any]],
                                        stock_code: str, publish_date: str, 
                                        original_filename: str, pdf_file: Path) -> tuple:
        """
        第一次比对：只进行精确主键匹配（变动起始日期+变动截止日期+出让方+受让方+交易方式）
        不进行日期范围匹配
        
        Args:
            ai_datas: AI提取的数据列表
            sql_data: SQL查询的数据列表
            stock_code: 股票代码
            publish_date: 信息发布日期
            original_filename: 原始文件名
            pdf_file: PDF文件路径
            
        Returns:
            tuple: (first_comparison_results, unmatched_ai_datas)
                - first_comparison_results: 精确匹配成功的比对结果列表
                - unmatched_ai_datas: 主键匹配失败的AI数据列表
        """
        results = []
        unmatched_ai_datas = []
        ai_data_list = ai_datas if isinstance(ai_datas, list) else [ai_datas]

        # 提取公共字段获取逻辑
        def extract_core_fields(data: Dict[str, Any]) -> tuple:
            """提取核心字段：日期范围和参与方"""
            qsrq = str(data.get("变动起始日期", "")).strip()
            jzrq = str(data.get("变动截止日期", "")).strip()
            gqcrf = self._normalize_string(data.get("出让方", ""))
            gqzrf = self._normalize_string(data.get("受让方", ""))
            jyfs = str(data.get("交易方式", "")).strip()
            return qsrq, jzrq, gqcrf, gqzrf, jyfs

        def extract_business_fields(data: Dict[str, Any]) -> dict:
            """提取业务字段：交易相关信息"""
            return {
                "变动起始日期": str(data.get("变动起始日期", "")).strip(),
                "变动截止日期": str(data.get("变动截止日期", "")).strip(),
                "出让方": str(data.get("出让方", "")).strip(),
                "受让方": str(data.get("受让方", "")).strip(),
                "交易方式": str(data.get("交易方式", "")).strip(),
                "出让前持股数量": str(data.get("出让前持股数量", "")).strip(),
                "出让后持股数量": str(data.get("出让后持股数量", "")).strip(),
                "出让后有限售股数": str(data.get("出让后有限售股数", "")).strip(),
                "出让后无限售股数": str(data.get("出让后无限售股数", "")).strip(),
                "受让前持股数量": str(data.get("受让前持股数量", "")).strip(),
                "受让后持股数量": str(data.get("受让后持股数量", "")).strip(),
                "受让后有限售股数": str(data.get("受让后有限售股数", "")).strip(),
                "受让后无限售股数": str(data.get("受让后无限售股数", "")).strip(),
                "涉及股数": str(data.get("涉及股数", "")).strip(),
                "交易金额": str(data.get("交易金额", "")).strip(),
                "交易价格均价": str(data.get("交易价格均价", "")).strip()
            }

        def build_result(stock_code: str, publish_date: str, core_fields: tuple, 
                        sql_fields: dict, ai_fields: dict, comparison_result: str, pdf_file: Path) -> dict:
            """构建比对结果"""
            qsrq, jzrq, gqcrf, gqzrf, jyfs = core_fields
            return {
                # 核心字段（公用字段）
                "公告标题": original_filename,
                "股票代码": stock_code,
                "信息发布日期": publish_date,
                "出让方": ai_fields.get("出让方", ""),
                "受让方": ai_fields.get("受让方", ""),
                "交易方式": jyfs,
                
                # AI字段
                "AI变动起始日期": ai_fields.get("变动起始日期", ""),
                "AI变动截止日期": ai_fields.get("变动截止日期", ""),
                "AI交易方式": ai_fields.get("交易方式", ""),
                "AI出让前持股数量": ai_fields.get("出让前持股数量", ""),
                "AI出让后持股数量": ai_fields.get("出让后持股数量", ""),
                "AI出让后有限售股数": ai_fields.get("出让后有限售股数", ""),
                "AI出让后无限售股数": ai_fields.get("出让后无限售股数", ""),
                "AI受让前持股数量": ai_fields.get("受让前持股数量", ""),
                "AI受让后持股数量": ai_fields.get("受让后持股数量", ""),
                "AI受让后有限售股数": ai_fields.get("受让后有限售股数", ""),
                "AI受让后无限售股数": ai_fields.get("受让后无限售股数", ""),
                "AI涉及股数": ai_fields.get("涉及股数", ""),
                "AI交易金额": ai_fields.get("交易金额", ""),
                "交易价格均价": sql_fields.get("交易价格均价", ""),
                
                # 比对结果列
                "比对结果": comparison_result,
                "PDF路径": str(pdf_file.absolute()),
            }

        # 1. SQL数据为空处理 - 所有AI数据都未匹配
        if not sql_data:
            unmatched_ai_datas = ai_data_list.copy()
            for ai_data in ai_data_list:
                core_fields = extract_core_fields(ai_data)
                ai_business_fields = extract_business_fields(ai_data)
                sql_business_fields = {}  # SQL字段为空
                results.append(build_result(stock_code, publish_date, core_fields, 
                            sql_business_fields, ai_business_fields, 
                            "SQL查询为空", pdf_file))
            return results, unmatched_ai_datas

        # 2. 构建SQL数据索引
        sql_data_by_composite_key = {}
        for record in sql_data:
            core_fields = extract_core_fields(record)
            if core_fields not in sql_data_by_composite_key:
                sql_data_by_composite_key[core_fields] = []
            sql_data_by_composite_key[core_fields].append(record)

        matched_sql_keys = set()

        # 3. 处理AI数据精确匹配
        for ai_data in ai_data_list:
            process_ai_data = ai_data.copy()
            if process_ai_data.get("交易方式", "") == "99":
                process_ai_data["交易方式"] = "56"
            ai_core_fields = extract_core_fields(process_ai_data)
            ai_business_fields = extract_business_fields(process_ai_data)

            if ai_core_fields not in sql_data_by_composite_key:
                # 精确匹配失败，添加到未匹配列表
                unmatched_ai_datas.append(ai_data)
            else:
                # 精确匹配成功
                matched_sql_keys.add(ai_core_fields)
                for sql_record in sql_data_by_composite_key[ai_core_fields]:
                    sql_business_fields = extract_business_fields(sql_record)
                    comparison_result = self._compare_shareholder_transfer_fields(process_ai_data, sql_record)
                    results.append(build_result(stock_code, publish_date, ai_core_fields, 
                                              sql_business_fields, ai_business_fields, comparison_result, pdf_file))

        return results, unmatched_ai_datas

    def _compare_data_with_keys(self, ai_datas: Union[Dict[str, Any], List[Dict[str, Any]]],
                                sql_data: List[Dict[str, Any]],
                                stock_code: str, publish_date: str, original_filename: str, pdf_file: Path) -> List[Dict[str, Any]]:
        """
        使用多维度主键进行股权变动数据比对，提升匹配准确性
        主键策略：日期范围(变动起始日期+变动截止日期) + 参与方(出让方+受让方)
        
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
            """提取核心字段：日期范围和参与方"""
            qsrq = str(data.get("变动起始日期", "")).strip()
            jzrq = str(data.get("变动截止日期", "")).strip()
            gqcrf = self._normalize_string(data.get("出让方", ""))
            gqzrf = self._normalize_string(data.get("受让方", ""))
            jyfs = str(data.get("交易方式", "")).strip()
            return qsrq, jzrq, gqcrf, gqzrf, jyfs

        def extract_business_fields(data: Dict[str, Any]) -> dict:
            """提取业务字段：交易相关信息"""
            return {
                "变动起始日期": str(data.get("变动起始日期", "")).strip(),
                "变动截止日期": str(data.get("变动截止日期", "")).strip(),
                "出让方": str(data.get("出让方", "")).strip(),
                "受让方": str(data.get("受让方", "")).strip(),
                "交易方式": str(data.get("交易方式", "")).strip(),
                "出让前持股数量": str(data.get("出让前持股数量", "")).strip(),
                "出让后持股数量": str(data.get("出让后持股数量", "")).strip(),
                "出让后有限售股数": str(data.get("出让后有限售股数", "")).strip(),
                "出让后无限售股数": str(data.get("出让后无限售股数", "")).strip(),
                "受让前持股数量": str(data.get("受让前持股数量", "")).strip(),
                "受让后持股数量": str(data.get("受让后持股数量", "")).strip(),
                "受让后有限售股数": str(data.get("受让后有限售股数", "")).strip(),
                "受让后无限售股数": str(data.get("受让后无限售股数", "")).strip(),
                "涉及股数": str(data.get("涉及股数", "")).strip(),
                "交易金额": str(data.get("交易金额", "")).strip(),
                "交易价格均价": str(data.get("交易价格均价", "")).strip()
            }

        def build_result(stock_code: str, publish_date: str, core_fields: tuple, 
                        sql_fields: dict, ai_fields: dict, comparison_result: str, pdf_file: Path) -> dict:
            """构建比对结果"""
            qsrq, jzrq, gqcrf, gqzrf, jyfs = core_fields
            return {
                # 核心字段（公用字段）
                "公告标题": original_filename,
                "股票代码": stock_code,
                "信息发布日期": publish_date,
                "出让方": ai_fields.get("出让方", ""),
                "受让方": ai_fields.get("受让方", ""),
                "交易方式": jyfs,
                
                # AI字段
                "AI变动起始日期": ai_fields.get("变动起始日期", ""),
                "AI变动截止日期": ai_fields.get("变动截止日期", ""),
                "AI交易方式": ai_fields.get("交易方式", ""),
                "AI出让前持股数量": ai_fields.get("出让前持股数量", ""),
                "AI出让后持股数量": ai_fields.get("出让后持股数量", ""),
                "AI出让后有限售股数": ai_fields.get("出让后有限售股数", ""),
                "AI出让后无限售股数": ai_fields.get("出让后无限售股数", ""),
                "AI受让前持股数量": ai_fields.get("受让前持股数量", ""),
                "AI受让后持股数量": ai_fields.get("受让后持股数量", ""),
                "AI受让后有限售股数": ai_fields.get("受让后有限售股数", ""),
                "AI受让后无限售股数": ai_fields.get("受让后无限售股数", ""),
                "AI涉及股数": ai_fields.get("涉及股数", ""),
                "AI交易金额": ai_fields.get("交易金额", ""),
                "交易价格均价": sql_fields.get("交易价格均价", ""),
                
                # 比对结果列
                "比对结果": comparison_result,
                "PDF路径": str(pdf_file.absolute()),
            }

        # 1. SQL数据为空处理 - AI有SQL无的情况
        if not sql_data:
            for ai_data in ai_data_list:
                core_fields = extract_core_fields(ai_data)
                ai_business_fields = extract_business_fields(ai_data)
                sql_business_fields = {}  # SQL字段为空
                results.append(build_result(stock_code, publish_date, core_fields, 
                                          sql_business_fields, ai_business_fields, 
                                          "SQL查询为空", pdf_file))
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
            if ai_data.get("交易方式", "") == "99":
                ai_data["交易方式"] = "56"
            ai_core_fields = extract_core_fields(ai_data)
            ai_business_fields = extract_business_fields(ai_data)

            if ai_core_fields not in sql_data_by_composite_key:
                # 精确匹配失败，尝试日期范围匹配，同时考虑出让方、受让方和交易方式
                fallback_matched = False
                
                # 获取AI数据的核心字段
                ai_qsrq, ai_jzrq, ai_gqcrf, ai_gqzrf, ai_jyfs = ai_core_fields
                
                for sql_record in sql_data:
                    # 获取SQL记录的核心字段
                    sql_core_fields = extract_core_fields(sql_record)
                    sql_qsrq, sql_jzrq, sql_gqcrf, sql_gqzrf, sql_jyfs = sql_core_fields
                    
                    # 检查SQL记录是否已匹配
                    if sql_core_fields in matched_sql_keys:
                        continue
                    
                    # 条件1：交易方式必须一致
                    # 条件2：出让方和受让方必须一致
                    # 条件3：日期范围有重叠（AI数据日期范围包含SQL数据日期范围）
                    if (sql_jyfs == ai_jyfs and
                        sql_gqcrf == ai_gqcrf and 
                        sql_gqzrf == ai_gqzrf and 
                        self._is_date_range_included(ai_qsrq, ai_jzrq, sql_qsrq, sql_jzrq)):
                        
                        # 日期范围匹配成功，添加到结果
                        sql_business_fields = extract_business_fields(sql_record)
                        comparison_result = self._compare_shareholder_transfer_fields(ai_data, sql_record)
                        results.append(build_result(stock_code, publish_date, ai_core_fields, 
                                                  sql_business_fields, ai_business_fields, 
                                                  f"{comparison_result}", pdf_file))
                        matched_sql_keys.add(sql_core_fields)
                        fallback_matched = True
                
                if not fallback_matched:
                    # SQL完全无匹配
                    results.append(build_result(stock_code, publish_date, ai_core_fields, 
                                              {}, ai_business_fields, "SQL无对应出让方、受让方和交易方式相等的记录", pdf_file))
            else:
                # 精确匹配成功
                matched_sql_keys.add(ai_core_fields)
                for sql_record in sql_data_by_composite_key[ai_core_fields]:
                    sql_business_fields = extract_business_fields(sql_record)
                    comparison_result = self._compare_shareholder_transfer_fields(ai_data, sql_record)
                    results.append(build_result(stock_code, publish_date, ai_core_fields, 
                                              sql_business_fields, ai_business_fields, comparison_result, pdf_file))

        # 4. SQL有AI无的情况不输出结果（根据用户要求）
        # 只需记录已匹配的SQL主键，避免重复处理即可
        return results

    def _compare_shareholder_transfer_fields(self, ai_data: Dict[str, Any], sql_data: Dict[str, Any]) -> str:
        """
        比较股权变动业务AI数据和SQL数据的字段，并返回格式化的比对结果
        
        Args:
            ai_data: AI提取的股权变动数据
            sql_data: SQL查询的股权变动数据
            
        Returns:
            格式化的比对结果字符串
        """
        error_messages = []

        # 定义需要比对的股权变动字段列表 (AI字段名, SQL字段名)
        # 核心业务字段：涉及股数、交易价格均价、交易方式
        primary_fields = [
            ("交易价格均价", "交易价格均价"),
            ("涉及股数", "涉及股数"),
            ("交易方式", "交易方式")
        ]
        # 补充信息字段
        supplementary_fields = [
            ("出让前持股数量", "出让前持股数量"),
            ("出让后持股数量", "出让后持股数量"),
            ("出让后有限售股数", "出让后有限售股数"),
            ("出让后无限售股数", "出让后无限售股数"),
            ("受让前持股数量", "受让前持股数量"),
            ("受让后持股数量", "受让后持股数量"),
            ("受让后有限售股数", "受让后有限售股数"),
            ("受让后无限售股数", "受让后无限售股数"),
            ("交易金额", "交易金额")
        ]

        # 合并所有比对字段
        fields_to_compare = primary_fields + supplementary_fields

        # 对每个字段进行比对
        for ai_field_name, sql_field_name in fields_to_compare:
            if sql_field_name == "交易价格均价" and sql_data.get(sql_field_name, "") == "合并处理":
                continue
            if ai_field_name == "交易金额" and not sql_data.get(sql_field_name, "") and sql_data.get("交易价格均价", "") == "合并处理":
                continue
            ai_value = ai_data.get(ai_field_name, "")
            sql_value = sql_data.get(sql_field_name, "")

            # 预处理AI值和SQL值
            processed_ai_value = self._preprocess_shareholder_value(ai_value)
            processed_sql_value = self._preprocess_shareholder_value(sql_value)

            # 比较值
            if not self._compare_values(processed_ai_value, processed_sql_value) and processed_ai_value:
                error_messages.append(f"{ai_field_name}错误【正式库：{sql_value}，AI：{ai_value}】")

        # 返回拼接的错误信息
        return "；".join(error_messages)

    def _preprocess_shareholder_value(self, value: Any) -> Any:
        """
        预处理股权变动数据的值，处理AI返回的数据类型不稳定的问题
        
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

    def load_prompt_from_md(self, md_file_path: str = "股权变动.md") -> str:
        """从MD文件加载提示词"""
        try:
            # 获取程序基础路径（兼容开发和打包环境）
            script_dir = get_base_path()
            # 构建提示词文件的绝对路径
            abs_md_path = os.path.join(script_dir, md_file_path)

            if os.path.exists(abs_md_path):
                with open(abs_md_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                logger.warning(f"提示词文件不存在: {abs_md_path}")
                return ""
        except Exception as e:
            logger.error(f"加载提示词文件失败: {e}")
            return ""

    def generate_report(self, session_id, results: List[Dict[str, Any]], report_file: str = None) -> str:
        """生成比对报告"""
        if not results:
            print("没有可生成报告的数据")
            return ""

        # 获取程序基础路径（兼容开发和打包环境）
        script_dir = get_base_path()
        # 确保report文件夹存在
        report_dir = os.path.join(script_dir, "股东股权变动小程序比对", "report")
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)
            print(f"创建报告目录: {report_dir}")

        if not report_file:
            report_file = os.path.join(report_dir, f"股东股权变动比对报告_{session_id}.xlsx")

        try:
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
        """创建比对结果表，包含公告标题超链接"""
        comparison_data = []

        for result in results:
            # 获取比对结果列表
            comparison_results = result.get("comparison_result", [])

            # 如果没有比对结果，则跳过
            if not comparison_results:
                continue

            # 比对结果是一个列表，包含一个或多个比对结果
            for comparison in comparison_results:
                comparison_data.append({
                    "公告标题": comparison.get("公告标题", ""),
                    "股票代码": comparison.get("股票代码", ""),
                    "信息发布日期": comparison.get("信息发布日期", ""),
                    "出让方": comparison.get("出让方", ""),
                    "受让方": comparison.get("受让方", ""),
                    "变动起始日期": comparison.get("AI变动起始日期", ""),
                    "变动截止日期": comparison.get("AI变动截止日期", ""),
                    "交易方式": comparison.get("AI交易方式", ""),
                    "出让前持股数量": comparison.get("AI出让前持股数量", ""),
                    "出让后持股数量": comparison.get("AI出让后持股数量", ""),
                    "出让后有限售股数": comparison.get("AI出让后有限售股数", ""),
                    "出让后无限售股数": comparison.get("AI出让后无限售股数", ""),
                    "受让前持股数量": comparison.get("AI受让前持股数量", ""),
                    "受让后持股数量": comparison.get("AI受让后持股数量", ""),
                    "受让后有限售股数": comparison.get("AI受让后有限售股数", ""),
                    "受让后无限售股数": comparison.get("AI受让后无限售股数", ""),
                    "涉及股数": comparison.get("AI涉及股数", ""),
                    "交易金额": comparison.get("AI交易金额", ""),
                    "交易价格均价": comparison.get("交易价格均价", ""),
                    "比对结果": comparison.get("比对结果", ""),
                    "PDF路径": comparison.get("PDF路径", "")
                })

        # 创建DataFrame并写入Excel
        df = pd.DataFrame(comparison_data)
        df.to_excel(writer, sheet_name="比对结果", index=False)
        
        # 获取工作表
        workbook = writer.book
        worksheet = workbook["比对结果"]
        
        # 设置列宽
        worksheet.column_dimensions["A"].width = 30  # 公告标题列宽
        worksheet.column_dimensions["C"].width = 12  # 信息发布日期列宽
        worksheet.column_dimensions["F"].width = 12  # 变动起始日期列宽
        worksheet.column_dimensions["G"].width = 12  # 变动截止日期列宽
        
        # 查找"PDF路径"列的位置
        pdf_path_col = None
        for col in range(1, worksheet.max_column + 1):
            header_value = worksheet.cell(row=1, column=col).value
            if header_value == "PDF路径":
                pdf_path_col = col
                break
        
        # 遍历每一行，为公告标题添加超链接
        for row in range(2, worksheet.max_row + 1):  # 从第2行开始，第1行是表头
            pdf_path = worksheet.cell(row=row, column=pdf_path_col).value if pdf_path_col else None  # 获取PDF路径
            title_cell = worksheet.cell(row=row, column=1)  # 公告标题单元格（第1列）
            
            if pdf_path and os.path.exists(pdf_path):
                # 创建超链接，格式为: HYPERLINK("file:///C:/path/to/file.pdf", "标题文本")
                # Windows系统需要使用file:///前缀，且路径中的\替换为/
                pdf_path_url = pdf_path.replace("\\", "/")
                title_cell.value = f'=HYPERLINK("file:///{pdf_path_url}", "{title_cell.value}")'
            else:
                # 如果PDF路径不存在，保持原样
                pass
        
        # 删除PDF路径列（不需要在Excel中显示）
        if pdf_path_col:
            worksheet.delete_cols(pdf_path_col)  # 删除"PDF路径"列


def main():
    """主函数 - 优化版，增强错误处理和进度显示"""
    print("=" * 60)
    print("股东股权变动AI比对系统")
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
                        print(f"\n\n处理完成! 共处理 {len(results)}/{len(pdf_files)} 个文件，耗时: {end_time - start_time}")
                        
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
                        # 构建files文件夹路径
                        main_dir = get_base_path()
                        files_dir = os.path.join(main_dir, "股东股权变动小程序比对", "files")
                        
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
                        latest_session_dir = os.path.join(files_dir, session_dirs[0])
                        
                        print(f"\n将处理最新下载的文件，目录: {latest_session_dir}")
                        custom_dir = latest_session_dir
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
                print(f"\n\n处理完成! 共处理 {len(results)}/{len(pdf_files)} 个文件，耗时: {end_time - start_time}")

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

                # 生成报告
                if results:
                    print("\n生成处理报告...")
                    # 获取程序基础路径（兼容开发和打包环境）
                    script_dir = get_base_path()
                    report_dir = os.path.join(script_dir, "股东股权变动小程序比对", "report")
                    if not os.path.exists(report_dir):
                        os.makedirs(report_dir)
                        print(f"创建报告目录: {report_dir}")

                    session_id = get_session_id()

                    report_file = os.path.join(report_dir,
                                               f"股东股权变动比对报告_{session_id}.xlsx")
                    processor.generate_report(session_id, results, report_file)

                    # 记录程序执行结束时间和总耗时
                    program_end_time = datetime.now()
                    total_duration = program_end_time - program_start_time
                    logger.info(f"程序执行完成 - 总耗时: {total_duration}")
                    print(f"\n程序执行完成!")
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
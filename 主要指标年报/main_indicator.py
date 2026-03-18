"""
主要指标年报报AI比对系统 - 优化版本
"""
import concurrent.futures
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from config import validate_config
from database_manager import db_manager
from ai_service_enhanced import enhanced_ai_service
from logger_config import setup_logging, get_logger, get_session_id, get_file_only_logger

# 配置日志
setup_logging()
logger = get_logger(__name__)
file_only_logger = get_file_only_logger(__name__)
main_dir = os.path.dirname(os.path.abspath(__file__))

# SQL查询语句
SQL_QUERY = '''
SELECT A.ID,B.GPDM,CONVERT(DATE,A.XXFBRQ) XXFBRQ,CONVERT(DATE,A.JZRQ) JZRQ,
       A.JBMGSY,A.XSMGSY,A.JBMGSYKC,A.XSMGSYKC,
       CAST(A.JLRJZCSYLJQ AS DECIMAL(18,8)) * 100 AS JLRJZCSYLJQ,
       CAST(A.KCHJLRJZCSYLJQ AS DECIMAL(18,8)) * 100 AS KCHJLRJZCSYLJQ,
       CAST(A.PTGJZCSYLJQ AS DECIMAL(18,8)) * 100 AS PTGJZCSYLJQ,
       CAST(A.KCPTGJZCSYLJQ AS DECIMAL(18,8)) * 100 AS KCPTGJZCSYLJQ,
       A.YYZSR,A.YYZSRTBZZ*100 YYZSRTBZZ,A.YYSR,A.YYSRTBZZ*100 YYSRTBZZ,A.JLRHJ,A.JLRHJTBZZ*100 JLRHJTBZZ,
       A.JLR,A.JLRTBZZ*100 JLRTBZZ,A.FJCXSY,A.KCFJYXSYHDJLR,A.KCFJYXSYHDJLRTBZZ*100 KCFJYXSYHDJLRTBZZ,
       A.JYXJLLJE,A.MGJYXJLLJE,A.ZCZE,A.GDQYHJ,A.GDQY,A.MGJZCPL,A.GJKJZEJLR,A.GJKJZZJZC
FROM [10.101.0.212].JYPRIME.dbo.usrGSCWZYZB A JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
    ON A.INBBM=B.INBBM AND B.ZQSC IN (18,83,90) AND B.ZQLB IN (1,2,41)
WHERE A.XXLYBM = 110101 AND A.GGLB = 20 AND
      B.GPDM = ? AND A.XXFBRQ = ?
'''


class EnhancedDataProcessor:
    """主要指标年报数据处理类"""

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
                                print(f"✓ 上传成功({upload_count}/{len(pdf_files)}): {pdf_file.name}")

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
                                print(f"✗ 上传失败 ({upload_count + 1}/{len(pdf_files)}): {pdf_file.name}")

                        except Exception as e:
                            # 上传异常
                            failed_uploads.append(pdf_file.name)
                            print(f"✗ 上传异常 ({upload_count + 1}/{len(pdf_files)}) {pdf_file.name}: {e}")

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
            # 处理文件
            result = self.process_file_with_uploaded_id(file_id, filename)

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

    def process_file_with_uploaded_id(self, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个PDF文件 - 简化错误处理和恢复机制"""
        stock_code = None
        publish_date = None
        ai_datas = None
        sql_data = None
        
        try:
            # 从文件名提取股票代码和发布日期
            stock_code, publish_date = self._parse_filename(filename)
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
                script_dir = os.path.dirname(os.path.abspath(__file__))
                log_dir = os.path.join(script_dir, "主要指标年报小程序比对", "logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== {filename} ===\n")
                    f.write(f"{json.dumps(ai_datas, ensure_ascii=False, indent=2)}\n")
            except Exception:
                pass  # 忽略日志保存错误，不影响主流程

            # 查询数据库 - 简化重试机制
            try:
                sql_data = self._query_database(stock_code, publish_date)
            except Exception as e:
                logger.warning(f"数据库查询失败: {filename} - {str(e)}")
                sql_data = None  # 设置为None，继续处理

            # 比对数据 - 使用新的比对逻辑
            comparison_result = self._compare_data_with_keys(ai_datas, sql_data, stock_code, publish_date)

            # 生成处理结果
            result = {
                "stock_code": stock_code,
                "publish_date": publish_date,
                "ai_datas": ai_datas,
                "sql_data": sql_data,
                "comparison_result": comparison_result
            }

            # 记录处理成功
            logger.info(f"处理成功: {filename}")
            return result

        except Exception as e:
            # 记录错误信息
            logger.error(f"处理文件异常: {filename} - 错误: {str(e)}")
            return None

    def _processed_ai_data(self, ai_datas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """预处理AI提取的JSON数据，去除空数据"""
        processed_datas = ai_datas.get("extracted_data", [])
        for ai_data in processed_datas:
            if not ai_data.get("extracted_data"):  # 如果这条数据AI提取为空
                  # 删除这一条数据
                  processed_datas.remove(ai_data)
        return processed_datas

    def _query_database(self, stock_code: str, publish_date: str) -> Optional[List[Dict[str, Any]]]:
        """查询数据库获取主要指标数据 - 简化错误处理和恢复机制"""
        try:
            # 使用新的SQL查询语句
            sql_data = db_manager.execute_query(SQL_QUERY, (stock_code, publish_date))

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
        """解析文件名，提取股票代码、信息发布日期"""
        try:
            base_name = os.path.splitext(filename)[0]
            parts = base_name.split('-')

            if len(parts) >= 4:
                stock_code = parts[0]
                publish_date = '-'.join(parts[1: 4])

                # 验证日期格式
                try:
                    datetime.strptime(publish_date, '%Y-%m-%d')
                except ValueError:
                    print(f"日期格式错误: {publish_date}")
                    return None, None

                return stock_code, publish_date
            else:
                print(f"文件名格式错误: {filename}")
                return None, None

        except Exception as e:
            print(f"解析文件名异常: {e}")
            return None, None

    def _compare_values(self, value1: Any, value2: Any) -> bool:
        """比较两个值是否相等"""
        try:
            # 处理空值
            if not value1 and not value2:
                return True
            if not value1 or not value2:
                return False

            # 转换为字符串并去除前后空格
            str_value1 = str(value1).strip()
            str_value2 = str(value2).strip()

            return str_value1 == str_value2
        except Exception:
            return False

    def _compare_data_with_keys(self, ai_datas: Union[Dict[str, Any], List[Dict[str, Any]]],
                                sql_data: List[Dict[str, Any]],
                                stock_code: str, publish_date: str) -> List[Dict[str, Any]]:
        """
        使用股票代码、信息发布日期和截止日期作为主键进行数据比对
        
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

        if not sql_data:
            for ai_data in ai_data_list:
                ai_date_jzrq = str(ai_data.get("JZRQ", "")).strip()
                results.append({
                    "股票代码": stock_code,
                    "信息发布日期": publish_date,
                    "截止日期": ai_date_jzrq,
                    "比对结果": "正式库无对应记录"
                })
            return results

        sql_data_by_jzrq = {}
        for record in sql_data:
            sql_date_jzrq = str(record.get("JZRQ", "")).strip()
            if sql_date_jzrq not in sql_data_by_jzrq:
                sql_data_by_jzrq[sql_date_jzrq] = []
            sql_data_by_jzrq[sql_date_jzrq].append(record)

        matched_sql_keys = set()

        for ai_data in ai_data_list:
            ai_date_jzrq = str(ai_data.get("JZRQ", "")).strip()

            if ai_date_jzrq not in sql_data_by_jzrq.keys():
                results.append({
                    "股票代码": stock_code,
                    "信息发布日期": publish_date,
                    "截止日期": ai_date_jzrq,
                    "比对结果": "正式库无对应主键的记录"
                })
            else:
                matched_sql_keys.add(ai_date_jzrq)

                for sql_record in sql_data_by_jzrq[ai_date_jzrq]:
                    comparison_result = self._compare_fields_with_format(ai_data, sql_record)

                    results.append({
                        "股票代码": stock_code,
                        "信息发布日期": publish_date,
                        "截止日期": ai_date_jzrq,
                        "比对结果": comparison_result
                    })

        for sql_date_jzrq, sql_records in sql_data_by_jzrq.items():
            if sql_date_jzrq not in matched_sql_keys:
                for sql_record in sql_records:
                    results.append({
                        "股票代码": stock_code,
                        "信息发布日期": publish_date,
                        "截止日期": sql_date_jzrq,
                        "比对结果": "AI无对应记录"
                    })

        return results

    def _compare_fields_with_format(self, ai_data: Dict[str, Any], sql_data: Dict[str, Any]) -> str:
        """
        比较AI数据和SQL数据的字段，并返回格式化的比对结果
        
        Args:
            ai_data: AI提取的数据
            sql_data: SQL查询的数据
            
        Returns:
            格式化的比对结果字符串
        """
        error_messages = []

        # 定义需要比对的字段列表（由于AI字段名和SQL字段名相同，直接使用字段列表）
        fields_to_compare = [
            "YYZSR", "YYSR", "JLRHJ", "JLR", "KCFJYXSYHDJLR",
            "YYZSRTBZZ", "YYSRTBZZ", "JLRHJTBZZ", "JLRTBZZ", "KCFJYXSYHDJLRTBZZ",
            "PTGJLR", "PTGJLRTBZZ", "KCFJCXSYHPTGJLR", "KCFJCXSYHPTGJLRTBZZ",
            "JYXJLLJE", "JBMGSY", "XSMGSY", "JBMGSYKC", "XSMGSYKC",
            "JLRJZCSYLJQ", "KCHJLRJZCSYLJQ", "PTGJZCSYLJQ", "KCPTGJZCSYLJQ",
            "ZCZE", "GDQY", "FJCXSY", "MGJZCPL", "PTGMGJZC", "MGJYXJLLJE", "GJKJZEJLR", "GJKJZZJZC"
        ]

        # 对每个字段进行比对
        for field_name in fields_to_compare:
            ai_value = ai_data.get(field_name, "")
            sql_value = sql_data.get(field_name, "")

            # 预处理AI值和SQL值
            processed_ai_value = self._preprocess_value(ai_value)
            processed_sql_value = self._preprocess_value(sql_value)

            # 比较值
            if not self._compare_values(processed_ai_value, processed_sql_value):
                error_messages.append(f"{field_name}错误【正式库：{sql_value}，AI：{ai_value}】")

        # 如果没有错误，返回"数据一致"
        if not error_messages:
            return "数据一致"

        # 返回拼接的错误信息
        return "；".join(error_messages)

    def _preprocess_value(self, value: Any) -> Any:
        if value is None:
            return ""

        str_value = str(value).strip()

        if not str_value:
            return ""

        if str_value in ("不适用", "-"):
            return ""

        str_value = str_value.replace(',','')

        is_negative = False
        if str_value.startswith('(') and str_value.endswith(')'):
            is_negative = True
            str_value = str_value[1:-1]

        if self._is_numeric_value(str_value):
            if '%' in str_value:
                str_value = str_value.replace("%", "")
            
            try:
                num_value = float(str_value)
                return -num_value if is_negative else num_value
            except ValueError:
                return str_value

        return str_value

    def _is_numeric_value(self, value: str) -> bool:
        """
        判断值是否为数值类型
        
        Args:
            value: 需要判断的值
            
        Returns:
            是否为数值类型
        """
        if not value:
            return False

        # 检查是否包含数字
        has_digit = any(c.isdigit() for c in value)

        # 检查是否包含数值相关的字符
        numeric_chars = {'.', '-', '+', 'e', 'E', '%'}
        has_numeric_char = any(c in numeric_chars for c in value)

        return has_digit and (has_numeric_char or value.isdigit())

    def load_prompt_from_md(self, md_file_path: str = "主要指标年度报告test.md") -> str:
        """从MD文件加载提示词"""
        try:
            # 获取脚本所在目录的绝对路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
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

    def generate_report(self, results: List[Dict[str, Any]], report_file: str = None) -> str:
        """生成比对报告"""
        if not results:
            print("没有可生成报告的数据")
            return ""

        # 获取脚本所在目录的绝对路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 确保report文件夹存在
        report_dir = os.path.join(script_dir, "主要指标年报小程序比对", "report")
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)
            print(f"创建报告目录: {report_dir}")

        if not report_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = os.path.join(report_dir, f"主要指标年报比对报告_{timestamp}.xlsx")

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
        comparison_data = []

        for result in results:
            comparison_results = result.get("comparison_result", [])

            if not comparison_results:
                continue

            for comparison in comparison_results:
                comparison_data.append({
                    "股票代码": comparison.get("股票代码", ""),
                    "信息发布日期": comparison.get("信息发布日期", ""),
                    "截止日期": comparison.get("截止日期", ""),
                    "比对结果": comparison.get("比对结果", "")
                })

        df = pd.DataFrame(comparison_data)
        df.to_excel(writer, sheet_name="比对结果", index=False)


def main():
    """主函数 - 优化版，增强错误处理和进度显示"""
    print("=" * 60)
    print("主要指标年报AI比对系统")
    print("=" * 60)

    try:
        processor = EnhancedDataProcessor()

        while True:
            print("\n请选择操作:")
            print("1. 处理指定目录文件")
            print("2. 退出")

            choice = input("\n请输入选项 (1-2): ").strip()

            if choice == "1":
                custom_dir = input("请输入要处理的目录路径: ").strip()
                if not custom_dir:
                    print("目录路径不能为空")
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
                    if len(failed_files) > 5:
                        print(f"  ... 还有 {len(failed_files) - 5} 个文件处理失败")

                # 生成报告
                if results:
                    print("\n生成处理报告...")
                    # 获取脚本所在目录的绝对路径
                    script_dir = os.path.dirname(os.path.abspath(__file__))
                    report_dir = os.path.join(script_dir, "主要指标年报小程序比对", "report")
                    if not os.path.exists(report_dir):
                        os.makedirs(report_dir)
                        print(f"创建报告目录: {report_dir}")

                    report_file = os.path.join(report_dir,
                                               f"主要指标年报比对报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                    processor.generate_report(results, report_file)

                    # 记录程序执行结束时间和总耗时
                    program_end_time = datetime.now()
                    total_duration = program_end_time - program_start_time
                    logger.info(f"程序执行完成 - 总耗时: {total_duration}")
                    print(f"\n程序执行完成! 总耗时: {total_duration}")
                else:
                    print("\n没有成功处理的文件，请检查日志获取详细信息")

            elif choice == "2":
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

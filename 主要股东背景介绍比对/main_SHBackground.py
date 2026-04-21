"""
主要股东背景介绍小程序AI比对系统 - 优化版本
"""
import concurrent.futures
import json
import os
import sys
import threading
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from zhconv import convert
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
SELECT a.GPDM 股票代码, cast(b.XXFBRQ as date)信息发布日期 ,
    cast(b.JZRQ as date)截止日期  ,b.GDMC  股东名称,b.GDXH 股东序号,
    ROUND(b.CGBL * 100, 4) AS 持股比例
FROM [10.101.0.212].JYPRIME.dbo.usrZYGDBJJS b, [10.101.0.212].JYPRIME.dbo.usrZQZB a
WHERE a.IGSDM=b.IGSDM
  AND a.ZQSC in (83,90,18)
  AND a.ZQLB in (1,2,41)
  AND a.SSZT =1
  AND b.CZYF =1
  AND b.GDXH in (1,2,3,4)
  AND a.GPDM = ?
order BY a.GPDM,b.GDXH
'''


class EnhancedDataProcessor:
    """主要股东背景介绍数据处理类"""

    def __init__(self):
        self.processed_count = 0
        self.total_files = 0
        self.lock = threading.Lock()
        self.file_status = {}
        self.uploaded_file_ids = {}

        # 验证配置
        try:
            validate_config()
        except ValueError as e:
            print(f"配置验证失败: {e}")
            raise

    def process_all_files(self, pdf_files: List[Path], batch_size: int = 6) -> List[Dict[str, Any]]:
        """处理所有PDF文件 - 优化版，增强流水线模式"""
        if not pdf_files:
            return []

        # 使用固定的12个处理线程
        process_workers = 12

        # 使用优化后的流水线模式
        return self._pipeline_upload_and_process(pdf_files, process_workers, batch_size)

    def _pipeline_upload_and_process(self, pdf_files: List[Path], process_workers: int, batch_size: int) -> List[
        Dict[str, Any]]:
        """
        优化的流水线处理：上传和处理并行进行，避免资源竞争
        
        主要优化点：
        1. 分离上传和处理逻辑，减少资源竞争
        2. 添加队列大小控制，防止内存溢出
        3. 增加详细注释，提高代码可读性
        4. 优化异常处理，确保资源正确释放
        """
        # 初始化变量
        all_results = []
        upload_queue = pdf_files.copy()  # 待上传文件队列
        processing_queue = {}  # 待处理文件字典 {文件路径: file_id}
        failed_uploads = []  # 失败的上传文件列表

        # 线程池配置 - 根据实际需求调整
        upload_workers = 2  # 增加上传线程数，避免成为瓶颈
        upload_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=upload_workers,
            thread_name_prefix="Upload"
        )
        process_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=process_workers,
            thread_name_prefix="Process"
        )

        # 任务跟踪字典
        upload_futures = {}  # 上传任务字典 {future: pdf_file}
        process_futures = {}  # 处理任务字典 {future: pdf_file}

        # 计数器
        upload_count = 0
        processing_count = 0
        completed_count = 0

        # 队列大小控制 - 防止内存溢出
        max_processing_queue = batch_size * 2  # 最大处理队列大小
        max_concurrent_uploads = upload_workers * 2  # 最大并发上传任务数

        print(f"流水线配置: {upload_workers}个上传线程, {process_workers}个处理线程")
        print(f"队列限制: 最大处理队列{max_processing_queue}, 最大并发上传{max_concurrent_uploads}")

        try:
            print("开始流水线处理...")

            # 主循环：处理上传和任务直到所有文件完成
            while upload_queue or processing_queue or upload_futures or process_futures:
                # === 第一阶段：提交上传任务 ===
                # 控制条件：有待上传文件 + 上传任务未达上限
                while (len(upload_futures) < max_concurrent_uploads and
                       upload_queue):
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
                    # 收集已完成的上传任务
                    completed_uploads = [f for f in upload_futures if f.done()]

                    # 处理每个完成的上传任务
                    for future in completed_uploads:
                        pdf_file = upload_futures.pop(future)

                        try:
                            # 获取上传结果
                            file_id = future.result()

                            if file_id:
                                # 上传成功：添加到处理队列
                                processing_queue[pdf_file] = file_id
                                upload_count += 1
                                print(f"✓ 上传成功({upload_count}/{len(pdf_files)}): {pdf_file.name}")

                                # 立即提交处理任务
                                process_future = process_executor.submit(
                                    self._process_and_cleanup_single_file,
                                    pdf_file, file_id, pdf_file.name
                                )
                                process_futures[process_future] = pdf_file
                                processing_count += 1
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
                    # 收集已完成的处理任务
                    completed_processes = [f for f in process_futures if f.done()]

                    # 处理每个完成的处理任务
                    for future in completed_processes:
                        pdf_file = process_futures.pop(future)
                        # 从处理队列中移除
                        processing_queue.pop(pdf_file, None)

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
                # 短暂休眠，避免CPU占用过高
                time.sleep(0.1)

        finally:
            # === 资源清理阶段 ===
            logger.info("开始关闭线程池...")

            # 等待所有上传任务完成或取消
            for future in upload_futures:
                future.cancel()

            # 等待所有处理任务完成或取消
            for future in process_futures:
                future.cancel()

            # 关闭线程池
            upload_executor.shutdown(wait=True)
            process_executor.shutdown(wait=True)

            logger.info("所有线程池已关闭")

            # === 结果统计阶段 ===
            # 输出失败的上传文件统计
            if failed_uploads:
                print(f"\n上传失败的文件: {len(failed_uploads)} 个")
                for file in failed_uploads:  # 显示所有失败文件
                    print(f"  - {file}")

            # 输出处理结果统计
            success_rate = (len(all_results) / len(pdf_files) * 100) if pdf_files else 0
            print(f"\n处理完成! 成功: {len(all_results)}/{len(pdf_files)} ({success_rate:.1f}%)")

        return all_results

    def _process_and_cleanup_single_file(self, pdf_file: Path, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个文件并清理资源 - 增强日志版"""
        process_start_time = time.time()
        logger.info(f"开始处理文件: {filename} (文件ID: {file_id})")

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
                    self.processed_count += 1
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
        """带超时控制的单个文件上传 - 优化版，移除信号量限制"""
        # 记录上传开始时间
        upload_start_time = time.time()

        try:
            stock_code, _ = self._parse_filename(pdf_file.name)

            # 判断是否为科创板代码（688开头 or 689开头），如果是则不上传
            if stock_code and (stock_code.startswith("688") or stock_code.startswith("689")):
                file_only_logger.info(f"跳过科创板文件上传: {pdf_file.name} (股票代码: {stock_code})")
                return None

            file_only_logger.info(f"开始上传文件: {pdf_file.name} (大小: {pdf_file.stat().st_size / 1024 / 1024:.2f} MB)")

            # 直接上传文件，不再使用信号量限制
            logger.debug(f"开始上传: {pdf_file.name}")

            # 上传文件
            file_id = enhanced_ai_service.upload_file(pdf_file)

            # 记录上传成功
            upload_duration = time.time() - upload_start_time
            file_only_logger.info(f"文件上传成功: {pdf_file.name} (耗时: {upload_duration:.2f}秒, 文件ID: {file_id})")

            # 存储上传的文件ID，用于后续清理
            with self.lock:
                self.uploaded_file_ids[pdf_file] = file_id

            return file_id

        except Exception as e:
            # 记录详细错误信息
            upload_duration = time.time() - upload_start_time if 'upload_start_time' in locals() else 0
            logger.error(f"上传文件失败: {pdf_file.name} (耗时: {upload_duration:.2f}秒) - 错误: {str(e)}",
                         exc_info=True)
            return None

    def process_file_with_uploaded_id(self, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个PDF文件"""
        try:
            # 从文件名提取股票代码和发布日期
            stock_code, publish_date = self._parse_filename(filename)
            if not stock_code or not publish_date:
                print(f"文件名格式错误，无法提取股票代码和日期: {filename}")
                return None

            # 使用AI服务提取数据
            ai_data_results = enhanced_ai_service.extract_data_from_file(file_id, self.load_prompt_from_md())
            ai_datas = ai_data_results.get('extracted_data')
            if not ai_datas:
                print(f"AI数据提取失败: {filename}")
                return None

            # 将AI提取的JSON数据保存到日志文件
            try:
                log_dir = os.path.join("主要股东背景介绍小程序比对", "logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== {filename} ===\n")
                    f.write(f"{json.dumps(ai_datas, ensure_ascii=False, indent=2)}")
                    f.write(f"\n===========================================\n")
            except Exception as e:
                file_only_logger.warning(f"保存AI提取数据到日志文件失败: {filename} - {e}")

            # 查询数据库
            sql_data = self._query_database(stock_code)
            
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

            return result

        except Exception as e:
            print(f"处理文件异常 {filename}: {e}")
            return None

    def _query_database(self, stock_code: str) -> Optional[List[Dict[str, Any]]]:
        """查询数据库获取主要股东背景介绍数据"""
        try:
            # 使用新的SQL查询语句
            sql_data = db_manager.execute_query(SQL_QUERY, (stock_code))

            if not sql_data:
                file_only_logger.info(f"未找到股票代码 {stock_code} 的主要股东背景介绍数据")
                return None

            # 将所有记录转换为字典列表，并处理字段名
            results = []
            for record in sql_data:
                result = {}
                for key, value in record.items():
                    result[key] = value if value is not None else ""
                results.append(result)

            file_only_logger.info(f"找到股票代码 {stock_code} 的 {len(results)} 条主要股东背景介绍数据")
            return results

        except Exception as e:
            logger.error(f"数据库查询异常: {e}")
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

    def _compare_data_with_keys(self, ai_datas: Union[Dict[str, Any], List[Dict[str, Any]]], sql_data: List[Dict[str, Any]],
                                stock_code: str, publish_date: str) -> List[Dict[str, Any]]:
        """
        使用股票代码、信息发布日期、截止日期和日期标志作为主键进行数据比对
        
        Args:
            ai_datas: AI提取的数据（可能是单个字典或字典列表）
            sql_data: SQL查询的数据列表
            stock_code: 股票代码
            publish_date: 信息发布日期
            
        Returns:
            比对结果列表
        """
        results = []
        
        # 确保ai_datas是列表格式
        ai_data_list = ai_datas if isinstance(ai_datas, list) else [ai_datas]

        # 如果SQL数据为空，则返回"正式库无对应记录"
        if not sql_data:
            for ai_data in ai_data_list:
                ai_data_SHName = str(ai_data.get("股东名称", "")).strip()
                ai_data_SHType = str(ai_data.get("股东类别", "")).strip()
                ai_data_SHIndex = str(ai_data.get("股东序号", "")).strip()
                ai_data_SHPercent = str(ai_data.get("持股比例", "")).strip()
                ai_data_SHNum = str(ai_data.get("持股数量", "")).strip()
                results.append({
                    "股票代码": stock_code,
                    "公告发布日期": publish_date,
                    "信息发布日期": "",
                    "截止日期": "",
                    "股东名称": ai_data_SHName,
                    "股东类别": ai_data_SHType,
                    "股东序号": ai_data_SHIndex,
                    "持股比例": ai_data_SHPercent,
                    "持股数量": ai_data_SHNum,
                    "比对结果": "正式库无对应记录"
                })
            return results

        # 将SQL数据按股票代码和股东名称分组 - 移到循环外部，只执行一次
        sql_data_by_code_name = {}
        for record in sql_data:
            sql_data_code = str(record.get("股票代码", "")).strip()
            sql_data_SHName = str(record.get("股东名称", "")).strip()
            sql_data_SHName_lower = self._preprocess_bd_SHName(sql_data_SHName)  # 主键比对用
            if (sql_data_code, sql_data_SHName_lower) not in sql_data_by_code_name:
                sql_data_by_code_name[(sql_data_code, sql_data_SHName_lower)] = []
            sql_data_by_code_name[(sql_data_code, sql_data_SHName_lower)].append(record)
        
        # 记录所有已匹配的SQL主键组合，避免重复处理
        matched_sql_keys = set()
        
        # 处理每个AI数据项
        for ai_data in ai_data_list:
            # 获取AI数据中的股票代码和股东名称
            ai_data_code = stock_code
            ai_data_SHName = self._preprocess_ai_SHName(str(ai_data.get("股东名称", "")).strip())
            ai_data_SHName_lower = self._preprocess_bd_SHName(ai_data_SHName)  # 主键比对用
            ai_data_SHType = str(ai_data.get("股东类别", "")).strip()
            ai_data_SHIndex = str(ai_data.get("股东序号", "")).strip()
            ai_data_SHPercent = str(ai_data.get("持股比例", "")).strip()
            ai_data_SHNum = str(ai_data.get("持股数量", "")).strip()

            # 检查AI数据中的股票代码和股东名称是否在SQL数据中存在
            if (ai_data_code, ai_data_SHName_lower) not in sql_data_by_code_name.keys():
                results.append({
                    "股票代码": stock_code,
                    "公告发布日期": publish_date,
                    "信息发布日期": "",
                    "截止日期": "",
                    "股东名称": ai_data_SHName,
                    "股东类别": ai_data_SHType,
                    "股东序号": ai_data_SHIndex,
                    "持股比例": ai_data_SHPercent,
                    "持股数量": ai_data_SHNum,
                    "比对结果": "正式库无对应主键的记录"
                })
            else:
                # 记录已匹配的SQL主键组合
                matched_sql_keys.add((ai_data_code, ai_data_SHName_lower))
                
                # 对每个匹配的SQL记录进行比对
                for sql_record in sql_data_by_code_name[(ai_data_code, ai_data_SHName_lower)]:
                    comparison_result = self._compare_fields_with_format(ai_data, sql_record)
                    sql_data_xxfbrq = str(sql_record.get("信息发布日期", "")).strip()
                    sql_data_jzrq = str(sql_record.get("截止日期", "")).strip()
                    sql_data_SHName = str(sql_record.get("股东名称", "")).strip()
                    sql_data_SHIndex = str(sql_record.get("股东序号", "")).strip()
                    sql_data_SHPercent = str(sql_record.get("持股比例", "")).strip()

                    results.append({
                        "股票代码": stock_code,
                        "公告发布日期": publish_date,
                        "信息发布日期": sql_data_xxfbrq,
                        "截止日期": sql_data_jzrq,
                        "股东名称": sql_data_SHName,
                        "股东类别": ai_data_SHType,
                        "股东序号": sql_data_SHIndex,
                        "持股比例": sql_data_SHPercent,
                        "持股数量": ai_data_SHNum,
                        "比对结果": comparison_result
                    })

        # 检查SQL数据中是否有AI数据中没有的股票代码和股东名称
        # 这部分逻辑移到所有AI数据处理完成后，避免重复处理
        for (sql_data_code, sql_data_SHName_lower), sql_records in sql_data_by_code_name.items():
            if (sql_data_code, sql_data_SHName_lower) not in matched_sql_keys:
                for sql_record in sql_records:
                    sql_data_xxfbrq = str(sql_record.get("信息发布日期", "")).strip()
                    sql_data_jzrq = str(sql_record.get("截止日期", "")).strip()
                    sql_data_SHName = str(sql_record.get("股东名称", "")).strip()
                    sql_data_SHIndex = str(sql_record.get("股东序号", "")).strip()
                    sql_data_SHPercent = str(sql_record.get("持股比例", "")).strip()
                    results.append({
                        "股票代码": stock_code,
                        "公告发布日期": publish_date,
                        "信息发布日期": sql_data_xxfbrq,
                        "截止日期": sql_data_jzrq,
                        "股东名称": sql_data_SHName,
                        "股东类别": "",
                        "股东序号": sql_data_SHIndex,
                        "持股比例": sql_data_SHPercent,
                        "持股数量": "",
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
            "股东序号", "持股比例"
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

    def _preprocess_ai_SHName(self, sh_name: str) -> str:
        """
        预处理AI股东名称数据，转换为标准格式

        主要处理：
        1. 将中文括号"（）"替换为英文括号"()"
        2. 去除字段值前后多余的空格
        3. 去除中文字符之间可能存在的多余空格
        4. 替换"－"等其他类似符号为"-"
        5. 去除多余的*号#号等特殊符号
        6. 将繁体字转换为简体字
        7. 去除股东名称中可能存在的"注"、"注1"、"注2"等后缀

        Args:
            sh_name: 原始股东名称

        Returns:
            处理后的股东名称
        """
        if not sh_name:
            return ""

        # 转换为字符串并去除前后空格
        processed_name = str(sh_name).strip()

        # 将繁体字转换为简体字
        try:
            processed_name = convert(processed_name, 'zh-hans')
        except ImportError:
            # 如果没有安装zhconv库，则跳过繁简转换
            pass

        # 替换各种连接符为标准的"-"
        replacement_map = {
            "－": "-",
            "—": "-",
            "–": "-",
            "―": "-",
            "（": "(",
            "）": ")",
            "*": "",
            "#": ""
        }
        for old_char, new_char in replacement_map.items():
            processed_name = processed_name.replace(old_char, new_char)

        # 去除中文字符之间可能存在的多余空格
        # 匹配中文字符之间的空格并移除
        processed_name = re.sub(r'([\u4e00-\u9fa5])\s+([\u4e00-\u9fa5])', r'\1\2', processed_name)
        # 匹配中文字符和英文括号之间的空格并移除
        processed_name = re.sub(r'([\u4e00-\u9fa5])\s+([()])', r'\1\2', processed_name)
        processed_name = re.sub(r'([()])\s+([\u4e00-\u9fa5])', r'\1\2', processed_name)
        # 匹配中文字符和数字之间的空格并移除
        processed_name = re.sub()
        # 去除股东名称中可能存在的"注"、"注1"、"注2"等后缀
        # 处理多种格式：注、注1、注2、注 1、注 2等
        processed_name = re.sub(r'注\s*\d*$', '', processed_name)
        # 去除股东名称中可能存在的"数字"后缀
        processed_name = re.sub(r'\d+$', '', processed_name)

        # 再次去除前后空格，确保处理后的结果干净
        processed_name = processed_name.strip()

        return processed_name

    def _preprocess_bd_SHName(self, sh_name: str) -> str:
        # 预处理股东名称数据-比对用
        replacement_map = {
            " ": "",
            ",": "",
            ".": ""
        }
        for old_char, new_char in replacement_map.items():
            processed_name = sh_name.replace(old_char, new_char).lower()

        return processed_name


    def _preprocess_value(self, value: Any) -> Any:
        """
        预处理值，处理AI返回的数据类型不稳定的问题
        
        Args:
            value: 需要预处理的值
            
        Returns:
            预处理后的值
        """
        if value is None:
            return ""
        
        # 转换为字符串
        str_value = str(value).strip()
        
        # 如果是空字符串，返回空字符串
        if not str_value:
            return ""
        
        # 处理数值类型
        if self._is_numeric_value(str_value):
            # 移除千分位分隔符
            str_value = str_value.replace(",", "")
            
            # 尝试转换为浮点数
            try:
                return float(str_value)
            except ValueError:
                # 如果转换失败，返回原字符串
                return str_value
        
        # 返回处理后的字符串
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
        
        # 移除千分位分隔符
        value = value.replace(",", "")
        
        # 检查是否包含数字
        has_digit = any(c.isdigit() for c in value)
        
        # 检查是否包含数值相关的字符
        numeric_chars = {'.', '-', '+', 'e', 'E', '%'}
        has_numeric_char = any(c in numeric_chars for c in value)
        
        return has_digit and (has_numeric_char or value.isdigit())

    def _initialize_processing_state(self, file_count: int):
        """初始化处理状态"""
        self.total_files = file_count
        self.processed_count = 0
        self.file_status = {}

    def load_prompt_from_md(self, md_file_path: str = "主要股东背景介绍.md") -> str:
        """从MD文件加载提示词 - 优化版，支持打包后的资源读取"""
        try:
            # 如果是打包后的程序，优先从打包资源中读取
            if hasattr(sys, '_MEIPASS'):
                # PyInstaller打包后的临时目录
                resource_path = os.path.join(sys._MEIPASS, md_file_path)
                if os.path.exists(resource_path):
                    with open(resource_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        logger.debug(f"从打包资源中成功读取提示词文件: {resource_path}")
                        return content
                else:
                    logger.warning(f"打包资源中未找到提示词文件: {resource_path}")
            
            # 开发环境或打包资源读取失败，尝试从脚本目录读取
            script_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(script_dir, md_file_path)
            if os.path.exists(script_path):
                with open(script_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    logger.debug(f"从脚本目录成功读取提示词文件: {script_path}")
                    return content
            
            # 最后尝试从当前工作目录读取
            cwd_path = os.path.join(os.getcwd(), md_file_path)
            if os.path.exists(cwd_path):
                with open(cwd_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    logger.debug(f"从当前工作目录成功读取提示词文件: {cwd_path}")
                    return content
            
            # 所有路径都尝试失败，记录错误并返回空字符串
            logger.error(f"无法找到提示词文件: {md_file_path}")
            logger.error(f"已尝试的路径: 打包资源={resource_path if hasattr(sys, '_MEIPASS') else 'N/A'}, 脚本目录={script_path}, 当前目录={cwd_path}")
            return ""
        except Exception as e:
            logger.error(f"读取提示词文件时发生异常: {e}")
            return ""

    def generate_report(self, results: List[Dict[str, Any]], report_file: str = None) -> str:
        """生成比对报告"""
        if not results:
            print("没有可生成报告的数据")
            return ""

        # 确保report文件夹存在
        report_dir = os.path.join("主要股东背景介绍小程序比对", "report")
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)
            print(f"创建报告目录: {report_dir}")

        if not report_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = os.path.join(report_dir, f"主要股东背景介绍比对报告_{timestamp}.xlsx")

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
        """创建比对结果表"""
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
                    "股票代码": comparison.get("股票代码", ""),
                    "公告发布日期": comparison.get("公告发布日期", ""),
                    "信息发布日期": comparison.get("信息发布日期", ""),
                    "截止日期": comparison.get("截止日期", ""),
                    "股东名称": comparison.get("股东名称", ""),
                    "股东类别(AI)": comparison.get("股东类别", ""),
                    "股东序号": comparison.get("股东序号", ""),
                    "持股比例": comparison.get("持股比例", ""),
                    "持股数量(AI)": comparison.get("持股数量", ""),
                    "比对结果": comparison.get("比对结果", "")
                })

        # 创建DataFrame并写入Excel
        df = pd.DataFrame(comparison_data)
        df.to_excel(writer, sheet_name="比对结果", index=False)


def main():
    """主函数 - 优化版，增强错误处理和进度显示"""
    print("=" * 60)
    print("主要股东背景介绍小程序AI比对系统")
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

                # 查找所有PDF文件
                pdf_files = list(Path(custom_dir).glob("*.pdf"))
                if not pdf_files:
                    print(f"在目录 {custom_dir} 中未找到PDF文件")
                    continue

                # 初始化处理状态
                processor._initialize_processing_state(len(pdf_files))

                start_time = datetime.now()
                results = processor.process_all_files(pdf_files)
                end_time = datetime.now()

                # 显示处理结果
                print(f"共处理 {len(results)}/{len(pdf_files)} 个文件，耗时: {end_time - start_time}")

                # 生成报告
                if results:
                    print("\n生成处理报告...")
                    report_dir = os.path.join("主要股东背景介绍小程序比对", "report")
                    if not os.path.exists(report_dir):
                        os.makedirs(report_dir)
                        print(f"创建报告目录: {report_dir}")

                    report_file = os.path.join(report_dir,
                                               f"主要股东背景介绍比对报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                    processor.generate_report(results, report_file)
                else:
                    print("处理失败或没有成功处理的文件")

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

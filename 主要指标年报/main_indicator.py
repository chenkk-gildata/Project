"""
主要指标年报报AI比对系统 - 优化版本
"""
import concurrent.futures
import json
import os
import sys
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


def get_base_path():
    """获取程序基础路径，兼容开发和打包环境"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

# 模块二子目录名称
MODULE2_SUBDIR = "净资产收益率和每股收益"

# 模块二文件名后缀
MODULE2_FILE_SUFFIX = "_mgsy.pdf"

# 提示词文件名
PROMPT_MODULE1 = "主要指标年度报告.md"
PROMPT_MODULE2 = "主要指标年度报告_每股收益.md"

# 每股收益类字段列表
MGSY_FIELDS = ["JBMGSY", "XSMGSY", "JBMGSYKC", "XSMGSYKC"]

# 净资产收益率类字段列表
JZCSYL_FIELDS = ["PTGJZCSYLJQ", "KCPTGJZCSYLJQ"]

# SQL查询语句
SQL_QUERY = '''
SELECT A.ID,B.GPDM,CONVERT(DATE,A.XXFBRQ) XXFBRQ,CONVERT(DATE,A.JZRQ) JZRQ,
       A.JBMGSY,A.XSMGSY,A.JBMGSYKC,A.XSMGSYKC,
       CAST(A.JLRJZCSYLJQ AS DECIMAL(18,8)) * 100 AS JLRJZCSYLJQ,
       CAST(A.KCHJLRJZCSYLJQ AS DECIMAL(18,8)) * 100 AS KCHJLRJZCSYLJQ,
       CAST(A.PTGJZCSYLJQ AS DECIMAL(18,8)) * 100 AS PTGJZCSYLJQ,
       CAST(A.KCPTGJZCSYLJQ AS DECIMAL(18,8)) * 100 AS KCPTGJZCSYLJQ,
       A.YYZSR,A.YYZSRTBZZ*100 YYZSRTBZZ,A.YYSR,A.YYSRTBZZ*100 YYSRTBZZ,
       A.YYSRKCJE,A.KCHYYSR,A.KCHYYSRTBZZ*100 KCHYYSRTBZZ,A.JLRHJ,A.JLRHJTBZZ*100 JLRHJTBZZ,
       A.JLR,A.JLRTBZZ*100 JLRTBZZ,A.FJCXSY,A.KCFJYXSYHDJLR,A.KCFJYXSYHDJLRTBZZ*100 KCFJYXSYHDJLRTBZZ,
       A.PTGJLR,A.PTGJLRTBZZ*100 PTGJLRTBZZ,A.KCFJCXSYHPTGJLR,A.KCFJCXSYHPTGJLRTBZZ*100 KCFJCXSYHPTGJLRTBZZ,
       A.JYXJLLJE,A.MGJYXJLLJE,A.ZCZE,A.GDQY,A.MGJZCPL,A.PTGMGJZC,A.GJKJZEJLR,A.GJKJZZJZC
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

    def _get_company_key(self, filename: str) -> Optional[str]:
        """
        从文件名提取公司唯一标识（股票代码-发布日期）
        用于匹配模块一和模块二的文件
        
        Args:
            filename: 文件名（不含路径）
            
        Returns:
            公司标识字符串，格式为"股票代码-发布日期"，失败返回None
        """
        try:
            base_name = os.path.splitext(filename)[0]
            if base_name.endswith("_mgsy"):
                base_name = base_name[:-5]
            
            parts = base_name.split('-')
            
            if len(parts) >= 4:
                stock_code = parts[0]
                publish_date = '-'.join(parts[1:4])
                
                try:
                    datetime.strptime(publish_date, '%Y-%m-%d')
                except ValueError:
                    logger.warning(f"日期格式错误: {publish_date}")
                    return None
                
                return f"{stock_code}-{publish_date}"
            else:
                logger.warning(f"文件名格式错误: {filename}")
                return None
                
        except Exception as e:
            logger.error(f"提取公司标识异常: {filename} - {e}")
            return None

    def _find_paired_files(self, base_dir: Path) -> Dict[str, Dict[str, Optional[Path]]]:
        """
        查找并配对模块一和模块二的文件
        
        Args:
            base_dir: 基础目录路径
            
        Returns:
            配对文件字典，结构为：
            {
                "company_key": {
                    "module1_file": Path,  # 模块一文件路径
                    "module2_file": Path   # 模块二文件路径（可能为None）
                }
            }
        """
        paired_files = {}
        
        module1_files = list(base_dir.glob("*.pdf"))
        
        module2_dir = base_dir / MODULE2_SUBDIR
        module2_files = []
        if module2_dir.exists():
            module2_files = list(module2_dir.glob(f"*{MODULE2_FILE_SUFFIX}"))
        
        module2_map = {}
        for m2_file in module2_files:
            key = self._get_company_key(m2_file.name)
            if key:
                module2_map[key] = m2_file
        
        for m1_file in module1_files:
            key = self._get_company_key(m1_file.name)
            if key:
                paired_files[key] = {
                    "module1_file": m1_file,
                    "module2_file": module2_map.get(key)
                }
        
        logger.info(f"文件配对完成: 模块一文件 {len(module1_files)} 个, 模块二文件 {len(module2_files)} 个, 配对结果 {len(paired_files)} 组")
        
        return paired_files

    def process_all_files(self, base_dir: Path) -> List[Dict[str, Any]]:
        """处理所有PDF文件 - 优化版，支持模块一和模块二并发处理"""
        if not base_dir or not base_dir.exists():
            logger.error(f"目录不存在: {base_dir}")
            return []

        paired_files = self._find_paired_files(base_dir)
        if not paired_files:
            logger.warning(f"未找到可处理的文件: {base_dir}")
            return []

        return self._pipeline_upload_and_process(paired_files)

    def _pipeline_upload_and_process(self, paired_files: Dict[str, Dict[str, Optional[Path]]]) -> List[Dict[str, Any]]:
        """
        优化的流水线处理：上传和处理并行进行，支持模块一和模块二并发处理
        
        处理单元从"单个文件"变为"公司文件对"
        """
        all_results = []
        company_keys = list(paired_files.keys())
        total_companies = len(company_keys)
        
        upload_workers = 2
        process_workers = 16
        
        upload_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=upload_workers,
            thread_name_prefix="Upload"
        )
        process_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=process_workers,
            thread_name_prefix="Process"
        )

        upload_futures = {}
        process_futures = {}
        
        upload_queue = []
        for company_key in company_keys:
            file_pair = paired_files[company_key]
            upload_queue.append(("module1", company_key, file_pair["module1_file"]))
            if file_pair["module2_file"]:
                upload_queue.append(("module2", company_key, file_pair["module2_file"]))

        upload_count = 0
        completed_count = 0
        company_file_ids = {}
        failed_uploads = {}
        upload_success_printed = set()

        print(f"任务流配置: {upload_workers}个上传线程, {process_workers}个处理线程")
        print(f"共 {total_companies} 家公司待处理")

        try:
            while upload_queue or upload_futures or process_futures:
                while len(upload_futures) < upload_workers and upload_queue:
                    module_type, company_key, pdf_file = upload_queue.pop(0)
                    
                    future = upload_executor.submit(
                        self._upload_single_file_with_timeout,
                        pdf_file
                    )
                    upload_futures[future] = (module_type, company_key, pdf_file)
                    logger.debug(f"提交上传任务: {module_type} - {pdf_file.name}")

                if upload_futures:
                    completed_uploads = []
                    try:
                        for future in concurrent.futures.as_completed(upload_futures, timeout=0.1):
                            completed_uploads.append(future)
                    except concurrent.futures.TimeoutError:
                        pass
                    
                    for future in completed_uploads:
                        module_type, company_key, pdf_file = upload_futures.pop(future)

                        try:
                            file_id = future.result()

                            if file_id:
                                if company_key not in company_file_ids:
                                    company_file_ids[company_key] = {}
                                company_file_ids[company_key][module_type] = {
                                    "file_id": file_id,
                                    "filename": pdf_file.name,
                                    "file_path": pdf_file
                                }

                                file_pair = paired_files[company_key]
                                has_module1 = "module1" in company_file_ids[company_key]
                                has_module2 = "module2" in company_file_ids[company_key] or file_pair["module2_file"] is None
                                
                                if has_module1 and has_module2 and company_key not in upload_success_printed:
                                    upload_success_printed.add(company_key)
                                    upload_count += 1
                                    module1_filename = company_file_ids[company_key]["module1"]["filename"]
                                    print(f"↑ 上传成功({upload_count}): {module1_filename}")
                                
                                if has_module1 and has_module2:
                                    process_future = process_executor.submit(
                                        self._process_and_cleanup_company_files,
                                        company_key,
                                        company_file_ids[company_key],
                                        file_pair
                                    )
                                    process_futures[process_future] = company_key
                                    logger.debug(f"提交处理任务: {company_key}")
                            else:
                                if company_key not in failed_uploads:
                                    failed_uploads[company_key] = []
                                failed_uploads[company_key].append(module_type)
                                print(f"✗ 上传失败: {module_type} - {pdf_file.name}")
                                
                                if module_type == "module1":
                                    print(f"✗ 公司 {company_key} 模块一上传失败，跳过该公司")
                                    completed_count += 1

                        except Exception as e:
                            if company_key not in failed_uploads:
                                failed_uploads[company_key] = []
                            failed_uploads[company_key].append(module_type)
                            print(f"✗ 上传异常: {module_type} - {pdf_file.name} - {e}")

                if process_futures:
                    completed_processes = []
                    try:
                        for future in concurrent.futures.as_completed(process_futures, timeout=0.1):
                            completed_processes.append(future)
                    except concurrent.futures.TimeoutError:
                        pass
                    
                    for future in completed_processes:
                        company_key = process_futures.pop(future)

                        try:
                            result = future.result()

                            if result:
                                all_results.append(result)
                                status = "成功"
                            else:
                                status = "失败"

                            completed_count += 1
                            print(f"{'✓' if result else '✗'} 处理{status}({completed_count}/{total_companies}): {company_key}")

                        except Exception as e:
                            completed_count += 1
                            print(f"✗ 处理异常({completed_count}/{total_companies}): {company_key} - {e}")

                time.sleep(0.05)

        except Exception as e:
            logger.error(f"流水线处理过程中发生异常: {e}", exc_info=True)
        
        finally:
            logger.info("开始关闭线程池...")

            for future in list(upload_futures.keys()):
                try:
                    future.result(timeout=30)
                except Exception as e:
                    logger.error(f"等待上传任务完成时出错: {e}")
                    future.cancel()

            for future in list(process_futures.keys()):
                try:
                    future.result(timeout=120)
                except Exception as e:
                    logger.error(f"等待处理任务完成时出错: {e}")
                    future.cancel()

            upload_executor.shutdown(wait=True)
            process_executor.shutdown(wait=True)

            logger.info("所有线程池已关闭")

        return all_results

    def _process_and_cleanup_company_files(self, company_key: str, 
                                             file_ids: Dict[str, Dict],
                                             file_pair: Dict[str, Optional[Path]]) -> Optional[Dict[str, Any]]:
        """
        处理单家公司的模块一和模块二文件并清理资源
        
        Args:
            company_key: 公司标识
            file_ids: 文件ID字典 {"module1": {...}, "module2": {...}}
            file_pair: 文件路径对
            
        Returns:
            处理结果
        """
        process_start_time = time.time()
        logger.info(f"开始处理公司: {company_key}")
        result = None

        try:
            result = self._process_company_files(company_key, file_ids)

            process_duration = time.time() - process_start_time
            if result:
                file_only_logger.info(f"公司处理成功: {company_key} (耗时: {process_duration:.2f}秒)")
                with self.lock:
                    if file_pair.get("module1_file"):
                        self.file_status[file_pair["module1_file"]] = "completed"
                    if file_pair.get("module2_file"):
                        self.file_status[file_pair["module2_file"]] = "completed"
            else:
                logger.warning(f"公司处理失败: {company_key} (耗时: {process_duration:.2f}秒)")
                with self.lock:
                    if file_pair.get("module1_file"):
                        self.file_status[file_pair["module1_file"]] = "failed"
                    if file_pair.get("module2_file"):
                        self.file_status[file_pair["module2_file"]] = "failed"

            return result

        except Exception as e:
            process_duration = time.time() - process_start_time
            logger.error(f"处理公司异常: {company_key} (耗时: {process_duration:.2f}秒) - 错误: {str(e)}", exc_info=True)
            with self.lock:
                if file_pair.get("module1_file"):
                    self.file_status[file_pair["module1_file"]] = "error"
                if file_pair.get("module2_file"):
                    self.file_status[file_pair["module2_file"]] = "error"
            return None

        finally:
            for module_type, file_info in file_ids.items():
                try:
                    self._cleanup_single_file(file_info["file_id"])
                    file_only_logger.debug(f"已清理上传文件: {module_type} - {file_info['filename']}")
                except Exception as e:
                    logger.error(f"清理上传文件失败: {module_type} - {file_info['filename']} - 错误: {str(e)}")

    def _process_company_files(self, company_key: str, file_ids: Dict[str, Dict]) -> Optional[Dict[str, Any]]:
        """
        并发处理同一公司的模块一和模块二文件，合并结果后进行比对
        
        Args:
            company_key: 公司标识（股票代码-发布日期）
            file_ids: 文件ID字典 {"module1": {...}, "module2": {...}}
            
        Returns:
            处理结果字典
        """
        parts = company_key.split('-')
        stock_code = parts[0]
        publish_date = '-'.join(parts[1:4])
        
        module1_data = None
        module2_data = None
        
        try:
            module1_info = file_ids.get("module1")
            if not module1_info:
                logger.error(f"模块一文件信息缺失: {company_key}")
                return None
            
            module2_info = file_ids.get("module2")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {}
                
                futures["module1"] = executor.submit(
                    self._extract_module1_data,
                    module1_info["file_id"],
                    module1_info["filename"]
                )
                
                if module2_info:
                    futures["module2"] = executor.submit(
                        self._extract_module2_data,
                        module2_info["file_id"],
                        module2_info["filename"]
                    )
                
                module1_data = futures["module1"].result()
                
                if "module2" in futures:
                    module2_data = futures["module2"].result()
            
            if not module1_data:
                logger.error(f"模块一数据提取失败: {company_key}")
                return None
            
            try:
                base_dir = get_base_path()
                log_dir = os.path.join(base_dir, "logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== {module1_info['filename']} ===\n")
                    f.write(f"【模块一】\n")
                    f.write(f"{json.dumps(module1_data, ensure_ascii=False, indent=2)}\n")
                    if module2_data:
                        f.write(f"\n【模块二】\n")
                        f.write(f"{json.dumps(module2_data, ensure_ascii=False, indent=2)}\n")
            except Exception:
                pass

            merged_data = self._merge_extracted_data(module1_data, module2_data)

            sql_data = None
            try:
                sql_data = self._query_database(stock_code, publish_date)
            except Exception as e:
                logger.warning(f"数据库查询失败: {company_key} - {str(e)}")

            comparison_result = self._compare_data_with_keys(merged_data, sql_data, stock_code, publish_date)

            result = {
                "stock_code": stock_code,
                "publish_date": publish_date,
                "ai_datas": merged_data,
                "sql_data": sql_data,
                "comparison_result": comparison_result
            }

            logger.info(f"处理成功: {company_key}")
            return result

        except Exception as e:
            logger.error(f"处理公司文件异常: {company_key} - 错误: {str(e)}")
            return None

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
                time.sleep(1)

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

    VALUE_TBZZ_MAPPING = {
        "YYZSR": "YYZSRTBZZ",
        "YYSR": "YYSRTBZZ",
        "KCHYYSR": "KCHYYSRTBZZ",
        "JLRHJ": "JLRHJTBZZ",
        "JLR": "JLRTBZZ",
        "KCFJYXSYHDJLR": "KCFJYXSYHDJLRTBZZ",
        "PTGJLR": "PTGJLRTBZZ",
        "KCFJCXSYHPTGJLR": "KCFJCXSYHPTGJLRTBZZ",
    }

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

        ai_data_processed = ai_data.copy()
        sql_data_processed = sql_data.copy()

        for value_field, tbzz_field in self.VALUE_TBZZ_MAPPING.items():
            ai_value = self._preprocess_value(ai_data.get(value_field, ""))
            if ai_value == "" or ai_value is None:
                ai_data_processed[tbzz_field] = ""

            sql_value = self._preprocess_value(sql_data.get(value_field, ""))
            if sql_value == "" or sql_value is None:
                sql_data_processed[tbzz_field] = ""

        fields_to_compare = [
            "YYZSR", "YYSR", "YYSRKCJE", "KCHYYSR", "JLRHJ", "JLR", "KCFJYXSYHDJLR",
            "YYZSRTBZZ", "YYSRTBZZ", "KCHYYSRTBZZ", "JLRHJTBZZ", "JLRTBZZ", "KCFJYXSYHDJLRTBZZ",
            "PTGJLR", "PTGJLRTBZZ", "KCFJCXSYHPTGJLR", "KCFJCXSYHPTGJLRTBZZ",
            "JYXJLLJE", "JBMGSY", "XSMGSY", "JBMGSYKC", "XSMGSYKC",
            "JLRJZCSYLJQ", "KCHJLRJZCSYLJQ", "PTGJZCSYLJQ", "KCPTGJZCSYLJQ",
            "ZCZE", "GDQY", "FJCXSY", "MGJZCPL", "PTGMGJZC", "MGJYXJLLJE", "GJKJZEJLR", "GJKJZZJZC"
        ]

        for field_name in fields_to_compare:
            ai_value = ai_data_processed.get(field_name, "")
            sql_value = sql_data_processed.get(field_name, "")

            processed_ai_value = self._preprocess_value(ai_value)
            processed_sql_value = self._preprocess_value(sql_value)

            if not self._compare_values(processed_ai_value, processed_sql_value):
                error_messages.append(f"{field_name}错误【正式库：{sql_value}，AI：{ai_value}】")

        if not error_messages:
            return "数据一致"

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

    def load_prompt_from_md(self, md_file_path: str) -> str:
        """从MD文件加载提示词"""
        try:
            base_dir = get_base_path()
            abs_md_path = os.path.join(base_dir, md_file_path)

            if os.path.exists(abs_md_path):
                with open(abs_md_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                logger.warning(f"提示词文件不存在: {abs_md_path}")
                return ""
        except Exception as e:
            logger.error(f"加载提示词文件失败: {e}")
            return ""

    def _extract_module1_data(self, file_id: str, filename: str) -> Optional[List[Dict[str, Any]]]:
        """
        使用test1提示词提取模块一数据
        
        Args:
            file_id: 上传后的文件ID
            filename: 文件名
            
        Returns:
            提取的数据列表，失败返回None
        """
        try:
            prompt = self.load_prompt_from_md(PROMPT_MODULE1)
            if not prompt:
                logger.error(f"加载模块一提示词失败: {filename}")
                return None
            
            ai_data_results = enhanced_ai_service.extract_data_from_file(file_id, prompt)
            if ai_data_results and ai_data_results.get('extracted_data'):
                return ai_data_results.get('extracted_data')
            else:
                logger.warning(f"模块一AI数据提取返回空结果: {filename}")
                return None
                
        except Exception as e:
            logger.error(f"模块一AI数据提取异常: {filename} - {e}")
            return None

    def _extract_module2_data(self, file_id: str, filename: str) -> Optional[List[Dict[str, Any]]]:
        """
        使用test2提示词提取模块二数据
        
        Args:
            file_id: 上传后的文件ID
            filename: 文件名
            
        Returns:
            提取的数据列表，失败返回None
        """
        try:
            prompt = self.load_prompt_from_md(PROMPT_MODULE2)
            if not prompt:
                logger.error(f"加载模块二提示词失败: {filename}")
                return None
            
            ai_data_results = enhanced_ai_service.extract_data_from_file(file_id, prompt)
            if ai_data_results and ai_data_results.get('extracted_data'):
                return ai_data_results.get('extracted_data')
            else:
                logger.warning(f"模块二AI数据提取返回空结果: {filename}")
                return None
                
        except Exception as e:
            logger.error(f"模块二AI数据提取异常: {filename} - {e}")
            return None

    def _get_precision(self, value: Any) -> int:
        """
        获取数值的小数精度（小数点后位数）
        
        Args:
            value: 需要判断精度的值
            
        Returns:
            小数点后的位数，非数值返回0
        """
        if value is None:
            return 0
        
        str_value = str(value).strip()
        if not str_value:
            return 0
        
        if str_value in ("不适用", "-", "--"):
            return 0
        
        str_value = str_value.replace(',', '').replace('%', '')
        
        if '(' in str_value and ')' in str_value:
            str_value = str_value.replace('(', '').replace(')', '')
        
        try:
            float(str_value)
        except ValueError:
            return 0
        
        if '.' in str_value:
            decimal_part = str_value.split('.')[-1]
            return len(decimal_part)
        else:
            return 0

    def _merge_mgsy_field(self, value1: Any, value2: Any) -> Any:
        """
        合并每股收益类字段
        精度一致取模块二，否则取精度高的
        
        Args:
            value1: 模块一的值
            value2: 模块二的值
            
        Returns:
            合并后的值
        """
        if not value1 and not value2:
            return ""
        if not value1:
            return value2
        if not value2:
            return value1
        
        precision1 = self._get_precision(value1)
        precision2 = self._get_precision(value2)
        
        if precision1 == precision2:
            return value2
        elif precision1 > precision2:
            return value1
        else:
            return value2

    def _merge_jzcsyl_field(self, value1: Any, value2: Any) -> Any:
        """
        合并净资产收益率类字段
        以模块二为准
        
        Args:
            value1: 模块一的值
            value2: 模块二的值
            
        Returns:
            合并后的值
        """
        if value2:
            return value2
        return value1 if value1 else ""

    def _merge_single_record(self, record1: Dict[str, Any], record2: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        合并单条记录
        
        Args:
            record1: 模块一的记录
            record2: 模块二的记录（可能为None）
            
        Returns:
            合并后的记录
        """
        merged = record1.copy()
        
        if not record2:
            return merged
        
        for field in MGSY_FIELDS:
            value1 = record1.get(field, "")
            value2 = record2.get(field, "")
            merged[field] = self._merge_mgsy_field(value1, value2)
        
        for field in JZCSYL_FIELDS:
            value1 = record1.get(field, "")
            value2 = record2.get(field, "")
            merged[field] = self._merge_jzcsyl_field(value1, value2)
        
        kchjlrjzcsyljq = merged.get("KCHJLRJZCSYLJQ", "")
        kcptgjzcsyljq = merged.get("KCPTGJZCSYLJQ", "")
        if kchjlrjzcsyljq == "" and kcptgjzcsyljq:
            merged["KCHJLRJZCSYLJQ"] = kcptgjzcsyljq
        
        return merged

    def _merge_extracted_data(self, module1_data: List[Dict[str, Any]], 
                               module2_data: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        合并模块一和模块二的提取数据
        按JZRQ匹配后合并
        
        Args:
            module1_data: 模块一提取的数据列表
            module2_data: 模块二提取的数据列表（可能为None或空）
            
        Returns:
            合并后的数据列表
        """
        if not module2_data:
            return module1_data
        
        module2_map = {}
        for record in module2_data:
            jzrq = str(record.get("JZRQ", "")).strip()
            if jzrq:
                module2_map[jzrq] = record
        
        merged_data = []
        for record1 in module1_data:
            jzrq = str(record1.get("JZRQ", "")).strip()
            record2 = module2_map.get(jzrq)
            merged_record = self._merge_single_record(record1, record2)
            merged_data.append(merged_record)
        
        return merged_data

    def generate_report(self, results: List[Dict[str, Any]], report_file: str = None) -> str:
        """生成比对报告"""
        if not results:
            print("没有可生成报告的数据")
            return ""

        base_dir = get_base_path()
        report_dir = os.path.join(base_dir, "report")
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
    """主函数 - 优化版，支持模块一和模块二并发处理"""
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

                base_dir = Path(custom_dir)
                if not base_dir.exists():
                    print(f"目录不存在: {custom_dir}")
                    continue

                module1_files = list(base_dir.glob("*.pdf"))
                if not module1_files:
                    print(f"在目录 {custom_dir} 中未找到PDF文件")
                    continue

                program_start_time = datetime.now()
                logger.info(f"程序开始执行 - 目录: {custom_dir}, 模块一文件数量: {len(module1_files)}")
                print(f"\n开始处理，模块一文件数量: {len(module1_files)}...")

                start_time = datetime.now()
                results = processor.process_all_files(base_dir)
                end_time = datetime.now()

                print(f"\n\n处理完成! 共处理 {len(results)} 家公司，耗时: {end_time - start_time}")

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
                    for file_name, status in failed_files[:5]:
                        print(f"  - {file_name}: {status}")
                    if len(failed_files) > 5:
                        print(f"  ... 还有 {len(failed_files) - 5} 个文件处理失败")

                # 生成报告
                if results:
                    print("\n生成处理报告...")
                    base_dir = get_base_path()
                    report_dir = os.path.join(base_dir, "report")
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

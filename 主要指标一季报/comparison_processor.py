"""
比对输出模块 - AI提取、日志记录和报告生成
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
from logger_config import get_logger, get_file_only_logger, get_session_id

logger = get_logger(__name__)
file_only_logger = get_file_only_logger(__name__)

SQL_QUERY = '''
SELECT A.ID,B.GPDM,CONVERT(DATE,A.XXFBRQ) XXFBRQ,CONVERT(DATE,A.JZRQ) JZRQ,
       CASE WHEN A.RQBZ=2 THEN '季度' ELSE '累计' END AS RQBZ,
       A.JBMGSY,A.XSMGSY,A.JBMGSYKC,A.XSMGSYKC,
       CAST(A.JLRJZCSYLJQ AS DECIMAL(18,8)) * 100 AS JLRJZCSYLJQ,
       CAST(A.KCHJLRJZCSYLJQ AS DECIMAL(18,8)) * 100 AS KCHJLRJZCSYLJQ,
       A.YYZSR,A.YYZSRTBZZ*100 YYZSRTBZZ,A.YYSR,A.YYSRTBZZ*100 YYSRTBZZ,A.JLRHJ,A.JLRHJTBZZ*100 JLRHJTBZZ,
       A.JLR,A.JLRTBZZ*100 JLRTBZZ,A.FJCXSY,A.KCFJYXSYHDJLR,A.KCFJYXSYHDJLRTBZZ*100 KCFJYXSYHDJLRTBZZ,
       A.JYXJLLJE,A.MGJYXJLLJE,A.ZCZE,A.GDQYHJ,A.GDQY,A.MGJZCPL
FROM [10.101.0.212].JYPRIME.dbo.usrGSCWZYZB A JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB B
    ON A.INBBM=B.INBBM AND B.ZQSC IN (18,83,90) AND B.ZQLB IN (1,2,41)
WHERE A.XXLYBM IN (110104,120105) AND A.GGLB = 20 AND
      B.GPDM = ? AND A.XXFBRQ = ?
'''


class ComparisonProcessor:
    """比对处理器"""

    def __init__(self):
        self.lock = threading.Lock()
        self.file_status = {}
        self.uploaded_file_ids = {}

        try:
            validate_config()
        except ValueError as e:
            print(f"配置验证失败: {e}")
            raise

    def process_all_files(self, pdf_files: List[Path]) -> List[Dict[str, Any]]:
        """处理所有PDF文件 - 优化版，增强流水线模式"""
        if not pdf_files:
            return []

        return self._pipeline_upload_and_process(pdf_files)

    def _pipeline_upload_and_process(self, pdf_files: List[Path]) -> List[Dict[str, Any]]:
        """优化的流水线处理：上传和处理并行进行，避免资源竞争"""
        all_results = []
        upload_queue = pdf_files.copy()
        failed_uploads = []

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

        upload_count = 0
        completed_count = 0

        print(f"比对任务流配置: {upload_workers}个上传线程, {process_workers}个处理线程")

        try:
            while upload_queue or upload_futures or process_futures:
                while len(upload_futures) < upload_workers and upload_queue:
                    pdf_file = upload_queue.pop(0)

                    future = upload_executor.submit(
                        self._upload_single_file_with_timeout,
                        pdf_file
                    )
                    upload_futures[future] = pdf_file
                    logger.debug(f"提交上传任务: {pdf_file.name}")

                if upload_futures:
                    completed_uploads = []
                    try:
                        for future in concurrent.futures.as_completed(upload_futures, timeout=0.1):
                            completed_uploads.append(future)
                    except concurrent.futures.TimeoutError:
                        pass
                    
                    for future in completed_uploads:
                        pdf_file = upload_futures.pop(future)

                        try:
                            file_id = future.result()

                            if file_id:
                                upload_count += 1
                                print(f"✓ 上传成功({upload_count}/{len(pdf_files)}): {pdf_file.name}")

                                process_future = process_executor.submit(
                                    self._process_and_cleanup_single_file,
                                    pdf_file, file_id, pdf_file.name
                                )
                                process_futures[process_future] = pdf_file
                                logger.debug(f"提交处理任务: {pdf_file.name}")
                            else:
                                failed_uploads.append(pdf_file.name)
                                print(f"✗ 上传失败 ({upload_count + 1}/{len(pdf_files)}): {pdf_file.name}")

                        except Exception as e:
                            failed_uploads.append(pdf_file.name)
                            print(f"✗ 上传异常 ({upload_count + 1}/{len(pdf_files)}) {pdf_file.name}: {e}")

                if process_futures:
                    completed_processes = []
                    try:
                        for future in concurrent.futures.as_completed(process_futures, timeout=0.1):
                            completed_processes.append(future)
                    except concurrent.futures.TimeoutError:
                        pass
                    
                    for future in completed_processes:
                        pdf_file = process_futures.pop(future)

                        try:
                            result = future.result()

                            if result:
                                all_results.append(result)
                                status = "成功"
                            else:
                                status = "失败"

                            completed_count += 1
                            print(
                                f"{'✓' if result else '✗'} 处理{status}({completed_count}/{len(pdf_files)}): {pdf_file.name}")

                        except Exception as e:
                            completed_count += 1
                            print(f"✗ 处理异常({completed_count}/{len(pdf_files)}): {pdf_file.name} - {e}")

                time.sleep(0.05)

        except Exception as e:
            logger.error(f"流水线处理过程中发生异常: {e}", exc_info=True)
        
        finally:
            logger.info("开始关闭线程池...")

            logger.info(f"等待 {len(upload_futures)} 个上传任务完成...")
            for future in list(upload_futures.keys()):
                try:
                    future.result(timeout=30)
                except Exception as e:
                    logger.error(f"等待上传任务完成时出错: {e}")
                    future.cancel()

            logger.info(f"等待 {len(process_futures)} 个处理任务完成...")
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

    def _process_and_cleanup_single_file(self, pdf_file: Path, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个文件并清理资源 - 增强日志版"""
        process_start_time = time.time()
        logger.info(f"开始处理文件: {filename} (文件ID: {file_id})")
        result = None

        try:
            result = self.process_file_with_uploaded_id(file_id, filename)

            process_duration = time.time() - process_start_time
            if result:
                file_only_logger.info(f"文件处理成功: {filename} (耗时: {process_duration:.2f}秒)")
                with self.lock:
                    self.file_status[pdf_file] = "completed"
            else:
                logger.warning(f"文件处理失败: {filename} (耗时: {process_duration:.2f}秒)")
                with self.lock:
                    self.file_status[pdf_file] = "failed"

            return result

        except Exception as e:
            process_duration = time.time() - process_start_time
            logger.error(f"处理文件异常: {filename} (耗时: {process_duration:.2f}秒) - 错误: {str(e)}", exc_info=True)
            with self.lock:
                self.file_status[pdf_file] = "error"
            return None

        finally:
            try:
                self._cleanup_single_file(file_id)
                file_only_logger.debug(f"已清理上传文件: {filename} (文件ID: {file_id})")
            except Exception as e:
                logger.error(f"清理上传文件失败: {filename} (文件ID: {file_id}) - 错误: {str(e)}")

            with self.lock:
                self.uploaded_file_ids.pop(pdf_file, None)

    def _cleanup_single_file(self, file_id: str):
        """清理单个上传的文件"""
        try:
            enhanced_ai_service.delete_file(file_id)
        except Exception as e:
            pass

    def _upload_single_file_with_timeout(self, pdf_file: Path) -> Optional[str]:
        """上传单个PDF文件到AI平台（简化的超时控制和重试机制）"""
        max_retries = 1
        
        for retry_count in range(max_retries + 1):
            try:
                file_id = enhanced_ai_service.upload_file(pdf_file)

                with self.lock:
                    self.uploaded_file_ids[pdf_file] = file_id
                
                if retry_count == 0:
                    file_size = pdf_file.stat().st_size / 1024 / 1024
                    file_only_logger.info(f"上传文件: {pdf_file.name} (大小: {file_size:.2f} MB, ID: {file_id})")

                return file_id

            except Exception as e:
                if retry_count == max_retries:
                    logger.error(f"文件上传失败: {pdf_file.name} - 错误: {str(e)}")
                    return None
                
                time.sleep(1)

    def process_file_with_uploaded_id(self, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个PDF文件 - 简化错误处理和恢复机制"""
        stock_code = None
        publish_date = None
        ai_datas = None
        sql_data = None
        
        try:
            stock_code, publish_date = self._parse_filename(filename)
            if not stock_code or not publish_date:
                logger.error(f"文件名格式错误: {filename}")
                return None

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
                    time.sleep(1)
            
            if not ai_data_results or not ai_data_results.get('extracted_data'):
                logger.error(f"AI数据提取返回空结果: {filename}")
                return None
                
            ai_datas = ai_data_results.get('extracted_data')

            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                log_dir = os.path.join(script_dir, "logs")
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                session_id = get_session_id()
                with open(os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"), "a", encoding="utf-8") as f:
                    f.write(f"\n=== {filename} ===\n")
                    f.write(f"{json.dumps(ai_datas, ensure_ascii=False, indent=2)}\n")
            except Exception:
                pass

            try:
                sql_data = self._query_database(stock_code, publish_date)
            except Exception as e:
                logger.warning(f"数据库查询失败: {filename} - {str(e)}")
                sql_data = None

            comparison_result = self._compare_data_with_keys(ai_datas, sql_data, stock_code, publish_date)

            result = {
                "stock_code": stock_code,
                "publish_date": publish_date,
                "ai_datas": ai_datas,
                "sql_data": sql_data,
                "comparison_result": comparison_result
            }

            logger.info(f"处理成功: {filename}")
            return result

        except Exception as e:
            logger.error(f"处理文件异常: {filename} - 错误: {str(e)}")
            return None

    def _query_database(self, stock_code: str, publish_date: str) -> Optional[List[Dict[str, Any]]]:
        """查询数据库获取主要指标数据 - 简化错误处理和恢复机制"""
        try:
            sql_data = db_manager.execute_query(SQL_QUERY, (stock_code, publish_date))

            if not sql_data:
                file_only_logger.info(f"未找到数据: {stock_code} {publish_date}")
                return None

            results = []
            for record in sql_data:
                result = {}
                for key, value in record.items():
                    result[key] = value if value is not None else ""
                results.append(result)

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
            if not value1 and not value2:
                return True
            if not value1 or not value2:
                return False

            str_value1 = str(value1).strip()
            str_value2 = str(value2).strip()

            return str_value1 == str_value2
        except Exception:
            return False

    def _compare_data_with_keys(self, ai_datas: Union[Dict[str, Any], List[Dict[str, Any]]],
                                sql_data: List[Dict[str, Any]],
                                stock_code: str, publish_date: str) -> List[Dict[str, Any]]:
        """使用股票代码、信息发布日期、截止日期和日期标志作为主键进行数据比对"""
        results = []

        ai_data_list = ai_datas if isinstance(ai_datas, list) else [ai_datas]

        if not sql_data:
            for ai_data in ai_data_list:
                ai_date_flag = str(ai_data.get("RQBZ", "")).strip()
                ai_date_jzrq = str(ai_data.get("JZRQ", "")).strip()
                ai_data_mgjzc = str(ai_data.get("MGJZCPL", "")).strip()
                ai_data_mgjyxjllje = str(ai_data.get("MGJYXJLLJE", "")).strip()
                results.append({
                    "股票代码": stock_code,
                    "信息发布日期": publish_date,
                    "截止日期": ai_date_jzrq,
                    "日期标志": ai_date_flag,
                    "每股净资产": ai_data_mgjzc,
                    "每股经营活动现金流量净额": ai_data_mgjyxjllje,
                    "比对结果": "正式库无对应记录"
                })
            return results

        sql_data_by_flag_jzrq = {}
        for record in sql_data:
            sql_date_flag = str(record.get("RQBZ", "")).strip()
            sql_date_jzrq = str(record.get("JZRQ", "")).strip()
            if (sql_date_flag, sql_date_jzrq) not in sql_data_by_flag_jzrq:
                sql_data_by_flag_jzrq[(sql_date_flag, sql_date_jzrq)] = []
            sql_data_by_flag_jzrq[(sql_date_flag, sql_date_jzrq)].append(record)

        matched_sql_keys = set()

        for ai_data in ai_data_list:
            ai_date_flag = str(ai_data.get("RQBZ", "")).strip()
            ai_date_jzrq = str(ai_data.get("JZRQ", "")).strip()
            ai_data_mgjzc = str(ai_data.get("MGJZCPL", "")).strip()
            ai_data_mgjyxjllje = str(ai_data.get("MGJYXJLLJE", "")).strip()

            if (ai_date_flag, ai_date_jzrq) not in sql_data_by_flag_jzrq.keys():
                results.append({
                    "股票代码": stock_code,
                    "信息发布日期": publish_date,
                    "截止日期": ai_date_jzrq,
                    "日期标志": ai_date_flag,
                    "每股净资产": ai_data_mgjzc,
                    "每股经营活动现金流量净额": ai_data_mgjyxjllje,
                    "比对结果": "正式库无对应主键的记录"
                })
            else:
                matched_sql_keys.add((ai_date_flag, ai_date_jzrq))

                for sql_record in sql_data_by_flag_jzrq[(ai_date_flag, ai_date_jzrq)]:
                    comparison_result = self._compare_fields_with_format(ai_data, sql_record)

                    results.append({
                        "股票代码": stock_code,
                        "信息发布日期": publish_date,
                        "截止日期": ai_date_jzrq,
                        "日期标志": ai_date_flag,
                        "每股净资产": ai_data_mgjzc,
                        "每股经营活动现金流量净额": ai_data_mgjyxjllje,
                        "比对结果": comparison_result
                    })

        for (sql_date_flag, sql_date_jzrq), sql_records in sql_data_by_flag_jzrq.items():
            if (sql_date_flag, sql_date_jzrq) not in matched_sql_keys:
                for sql_record in sql_records:
                    sql_data_mgjzc = str(sql_record.get("MGJZCPL", "")).strip()
                    sql_data_mgjyxjllje = str(sql_record.get("MGJYXJLLJE", "")).strip()
                    results.append({
                        "股票代码": stock_code,
                        "信息发布日期": publish_date,
                        "截止日期": sql_date_jzrq,
                        "日期标志": sql_date_flag,
                        "每股净资产": sql_data_mgjzc,
                        "每股经营活动现金流量净额": sql_data_mgjyxjllje,
                        "比对结果": "AI无对应记录"
                    })

        return results

    def _compare_fields_with_format(self, ai_data: Dict[str, Any], sql_data: Dict[str, Any]) -> str:
        """比较AI数据和SQL数据的字段，并返回格式化的比对结果"""
        error_messages = []

        fields_to_compare = [
            "YYZSR", "YYSR", "JLRHJ", "JLR", "KCFJYXSYHDJLR",
            "YYZSRTBZZ", "YYSRTBZZ", "JLRHJTBZZ", "JLRTBZZ", "KCFJYXSYHDJLRTBZZ",
            "JYXJLLJE", "JBMGSY", "XSMGSY", "JBMGSYKC", "XSMGSYKC",
            "JLRJZCSYLJQ", "KCHJLRJZCSYLJQ", "ZCZE", "GDQY", "FJCXSY"
        ]

        for field_name in fields_to_compare:
            ai_value = ai_data.get(field_name, "")
            sql_value = sql_data.get(field_name, "")

            processed_ai_value = self._preprocess_value(ai_value)
            processed_sql_value = self._preprocess_value(sql_value)

            if not self._compare_values(processed_ai_value, processed_sql_value):
                error_messages.append(f"{field_name}错误【正式库：{sql_value}，AI：{ai_value}】")

        if not error_messages:
            return "数据一致"

        return "；".join(error_messages)

    def _preprocess_value(self, value: Any) -> Any:
        """预处理值，处理AI返回的数据类型不稳定的问题"""
        if value is None:
            return ""

        str_value = str(value).strip()

        if not str_value:
            return ""

        str_value = str_value.replace(',','')

        if self._is_numeric_value(str_value):
            if '%' in str_value:
                str_value = str_value.replace("%", "")
            
            try:
                return float(str_value)
            except ValueError:
                return str_value

        return str_value

    def _is_numeric_value(self, value: str) -> bool:
        """判断值是否为数值类型"""
        if not value:
            return False

        has_digit = any(c.isdigit() for c in value)

        numeric_chars = {'.', '-', '+', 'e', 'E', '%'}
        has_numeric_char = any(c in numeric_chars for c in value)

        return has_digit and (has_numeric_char or value.isdigit())

    def load_prompt_from_md(self, md_file_path: str = "主要指标季度报告.md") -> str:
        """从MD文件加载提示词"""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
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

        script_dir = os.path.dirname(os.path.abspath(__file__))
        report_dir = os.path.join(script_dir, "reports")
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)
            print(f"创建报告目录: {report_dir}")

        if not report_file:
            session_id = get_session_id()
            report_file = os.path.join(report_dir, f"主要指标一季报比对报告_{session_id}.xlsx")

        try:
            with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
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
            comparison_results = result.get("comparison_result", [])

            if not comparison_results:
                continue

            for comparison in comparison_results:
                comparison_data.append({
                    "股票代码": comparison.get("股票代码", ""),
                    "信息发布日期": comparison.get("信息发布日期", ""),
                    "截止日期": comparison.get("截止日期", ""),
                    "日期标志": comparison.get("日期标志", ""),
                    "每股净资产": comparison.get("每股净资产", ""),
                    "每股经营活动现金流量净额": comparison.get("每股经营活动现金流量净额", ""),
                    "比对结果": comparison.get("比对结果", "")
                })

        df = pd.DataFrame(comparison_data)
        df.to_excel(writer, sheet_name="比对结果", index=False)

"""
主要股东背景介绍一季报比对处理模块。
"""
import concurrent.futures
import json
import os
import re
import threading
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

warnings.filterwarnings(
    "ignore",
    message="pkg_resources is deprecated as an API.*",
    category=UserWarning,
)
from zhconv import convert

from ai_service_enhanced import enhanced_ai_service
from config import validate_config
from database_manager import db_manager
from logger_config import get_file_only_logger, get_logger, get_session_id
from path_utils import get_logs_dir, get_prompt_path, get_reports_dir

logger = get_logger(__name__)
file_only_logger = get_file_only_logger(__name__)

SQL_QUERY = """
SELECT a.GPDM 股票代码,
       CAST(b.XXFBRQ AS DATE) 信息发布日期,
       CAST(b.JZRQ AS DATE) 截止日期,
       b.GDMC 股东名称,
       b.GDXH 股东序号,
       ROUND(b.CGBL * 100, 4) AS 持股比例
FROM [10.101.0.212].JYPRIME.dbo.usrZYGDBJJS b,
     [10.101.0.212].JYPRIME.dbo.usrZQZB a
WHERE a.IGSDM = b.IGSDM
  AND a.ZQSC IN (83, 90, 18)
  AND a.ZQLB IN (1, 2, 41)
  AND a.SSZT = 1
  AND b.CZYF = 1
  AND b.GDXH IN (1, 2, 3, 4)
  AND a.GPDM = ?
ORDER BY a.GPDM, b.GDXH
"""


class ComparisonProcessor:
    """股东背景介绍比对处理器。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.file_status = {}
        self.uploaded_file_ids = {}
        validate_config()

    def process_all_files(self, pdf_files: List[Path], batch_size: int = 6) -> List[Dict[str, Any]]:
        """批量处理所有 PDF 文件。"""
        if not pdf_files:
            return []
        return self._pipeline_upload_and_process(pdf_files, process_workers=12, batch_size=batch_size)

    def _pipeline_upload_and_process(
        self,
        pdf_files: List[Path],
        process_workers: int,
        batch_size: int,
    ) -> List[Dict[str, Any]]:
        """流水线方式并行上传和处理文件。"""
        all_results = []
        upload_queue = pdf_files.copy()
        failed_uploads = []

        upload_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="Upload")
        process_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=process_workers,
            thread_name_prefix="Process",
        )

        upload_futures = {}
        process_futures = {}
        upload_count = 0
        completed_count = 0
        max_concurrent_uploads = 4
        _ = batch_size

        print(f"流水线配置: 2 个上传线程, {process_workers} 个处理线程")

        try:
            while upload_queue or upload_futures or process_futures:
                while len(upload_futures) < max_concurrent_uploads and upload_queue:
                    pdf_file = upload_queue.pop(0)
                    future = upload_executor.submit(self._upload_single_file_with_timeout, pdf_file)
                    upload_futures[future] = pdf_file

                for future in [item for item in upload_futures if item.done()]:
                    pdf_file = upload_futures.pop(future)
                    try:
                        file_id = future.result()
                        if file_id:
                            upload_count += 1
                            print(f"上传成功({upload_count}/{len(pdf_files)}): {pdf_file.name}")
                            process_future = process_executor.submit(
                                self._process_and_cleanup_single_file,
                                pdf_file,
                                file_id,
                                pdf_file.name,
                            )
                            process_futures[process_future] = pdf_file
                        else:
                            failed_uploads.append(pdf_file.name)
                            print(f"上传失败 ({upload_count + 1}/{len(pdf_files)}): {pdf_file.name}")
                    except Exception as exc:
                        failed_uploads.append(pdf_file.name)
                        print(f"上传异常 ({upload_count + 1}/{len(pdf_files)}) {pdf_file.name}: {exc}")

                for future in [item for item in process_futures if item.done()]:
                    pdf_file = process_futures.pop(future)
                    try:
                        result = future.result()
                        if result:
                            all_results.append(result)
                            status = "成功"
                        else:
                            status = "失败"
                        completed_count += 1
                        print(f"处理{status}({completed_count}/{len(pdf_files)}): {pdf_file.name}")
                    except Exception as exc:
                        completed_count += 1
                        print(f"处理异常({completed_count}/{len(pdf_files)}): {pdf_file.name} - {exc}")

                time.sleep(0.1)
        finally:
            for future in upload_futures:
                future.cancel()
            for future in process_futures:
                future.cancel()
            upload_executor.shutdown(wait=True)
            process_executor.shutdown(wait=True)

            if failed_uploads:
                print(f"\n上传失败的文件 {len(failed_uploads)} 个:")
                for file_name in failed_uploads:
                    print(f"  - {file_name}")

            success_rate = (len(all_results) / len(pdf_files) * 100) if pdf_files else 0
            print(f"\n处理完成! 成功: {len(all_results)}/{len(pdf_files)} ({success_rate:.1f}%)")

        return all_results

    def _process_and_cleanup_single_file(self, pdf_file: Path, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个文件并清理上传资源。"""
        start_time = time.time()
        logger.info("开始处理文件: %s (文件 ID: %s)", filename, file_id)

        try:
            result = self.process_file_with_uploaded_id(file_id, filename)
            duration = time.time() - start_time
            if result:
                file_only_logger.info("文件处理成功: %s (耗时: %.2f 秒)", filename, duration)
                with self.lock:
                    self.file_status[pdf_file] = "completed"
            else:
                logger.warning("文件处理失败: %s (耗时: %.2f 秒)", filename, duration)
                with self.lock:
                    self.file_status[pdf_file] = "failed"
            return result
        except Exception as exc:
            duration = time.time() - start_time
            logger.error("处理文件异常: %s (耗时: %.2f 秒) - %s", filename, duration, exc, exc_info=True)
            with self.lock:
                self.file_status[pdf_file] = "error"
            return None
        finally:
            try:
                enhanced_ai_service.delete_file(file_id)
            except Exception:
                pass
            with self.lock:
                self.uploaded_file_ids.pop(pdf_file, None)

    def _upload_single_file_with_timeout(self, pdf_file: Path) -> Optional[str]:
        """上传单个 PDF 到 AI 平台。"""
        upload_start_time = time.time()
        try:
            stock_code, _ = self._parse_filename(pdf_file.name)
            if stock_code and (stock_code.startswith("688") or stock_code.startswith("689")):
                file_only_logger.info("跳过科创板文件上传: %s", pdf_file.name)
                return None

            file_only_logger.info(
                "开始上传文件: %s (大小: %.2f MB)",
                pdf_file.name,
                pdf_file.stat().st_size / 1024 / 1024,
            )
            file_id = enhanced_ai_service.upload_file(pdf_file)
            duration = time.time() - upload_start_time
            file_only_logger.info("文件上传成功: %s (耗时: %.2f 秒, 文件 ID: %s)", pdf_file.name, duration, file_id)
            with self.lock:
                self.uploaded_file_ids[pdf_file] = file_id
            return file_id
        except Exception as exc:
            duration = time.time() - upload_start_time
            logger.error("上传文件失败: %s (耗时: %.2f 秒) - %s", pdf_file.name, duration, exc, exc_info=True)
            return None

    def process_file_with_uploaded_id(self, file_id: str, filename: str) -> Optional[Dict[str, Any]]:
        """处理单个已上传文件。"""
        try:
            stock_code, publish_date = self._parse_filename(filename)
            if not stock_code or not publish_date:
                print(f"文件名格式错误，无法提取股票代码和日期: {filename}")
                return None

            ai_data_results = enhanced_ai_service.extract_data_from_file(file_id, self.load_prompt_from_md())
            ai_datas = ai_data_results.get("extracted_data")
            if not ai_datas:
                print(f"AI 数据提取失败: {filename}")
                return None

            try:
                log_dir = get_logs_dir()
                session_id = get_session_id()
                with open(
                    os.path.join(log_dir, f"ai_extraction_data_{session_id}.log"),
                    "a",
                    encoding="utf-8",
                ) as file:
                    file.write(f"\n=== {filename} ===\n")
                    file.write(json.dumps(ai_datas, ensure_ascii=False, indent=2))
                    file.write("\n===========================================\n")
            except Exception as exc:
                file_only_logger.warning("保存 AI 提取数据失败 %s - %s", filename, exc)

            sql_data = self._query_database(stock_code)
            comparison_result = self._compare_data_with_keys(ai_datas, sql_data, stock_code, publish_date)
            return {
                "stock_code": stock_code,
                "publish_date": publish_date,
                "ai_datas": ai_datas,
                "sql_data": sql_data,
                "comparison_result": comparison_result,
            }
        except Exception as exc:
            print(f"处理文件异常 {filename}: {exc}")
            return None

    def _query_database(self, stock_code: str) -> Optional[List[Dict[str, Any]]]:
        """查询数据库获取主要股东背景介绍数据。"""
        try:
            sql_data = db_manager.execute_query(SQL_QUERY, (stock_code,))
            if not sql_data:
                file_only_logger.info("未找到股票代码 %s 的主要股东背景介绍数据", stock_code)
                return None

            results = []
            for record in sql_data:
                result = {}
                for key, value in record.items():
                    result[key] = value if value is not None else ""
                results.append(result)

            file_only_logger.info("找到股票代码 %s 的 %s 条主要股东背景介绍数据", stock_code, len(results))
            return results
        except Exception as exc:
            logger.error("数据库查询异常: %s", exc)
            return None

    def _parse_filename(self, filename: str) -> tuple:
        """解析文件名，提取股票代码和信息发布日期。"""
        try:
            base_name = os.path.splitext(filename)[0]
            parts = base_name.split("-")
            if len(parts) < 4:
                return None, None
            stock_code = parts[0]
            publish_date = "-".join(parts[1:4])
            datetime.strptime(publish_date, "%Y-%m-%d")
            return stock_code, publish_date
        except Exception:
            return None, None

    def _compare_values(self, value1: Any, value2: Any) -> bool:
        """比较两个值是否相等。"""
        try:
            if not value1 and not value2:
                return True
            if not value1 or not value2:
                return False
            return str(value1).strip() == str(value2).strip()
        except Exception:
            return False

    def _compare_data_with_keys(
        self,
        ai_datas: Union[Dict[str, Any], List[Dict[str, Any]]],
        sql_data: List[Dict[str, Any]],
        stock_code: str,
        publish_date: str,
    ) -> List[Dict[str, Any]]:
        """按股票代码和规范化股东名称比对数据。"""
        results = []
        ai_data_list = ai_datas if isinstance(ai_datas, list) else [ai_datas]

        if not sql_data:
            for ai_data in ai_data_list:
                results.append(
                    {
                        "股票代码": stock_code,
                        "公告发布日期": publish_date,
                        "信息发布日期": "",
                        "截止日期": "",
                        "股东名称": str(ai_data.get("股东名称", "")).strip(),
                        "股东类别": str(ai_data.get("股东类别", "")).strip(),
                        "股东序号": str(ai_data.get("股东序号", "")).strip(),
                        "持股比例": str(ai_data.get("持股比例", "")).strip(),
                        "持股数量": str(ai_data.get("持股数量", "")).strip(),
                        "比对结果": "正式库无对应记录",
                    }
                )
            return results

        sql_data_by_code_name = {}
        for record in sql_data:
            sql_data_code = str(record.get("股票代码", "")).strip()
            sql_data_name = str(record.get("股东名称", "")).strip()
            sql_key = (sql_data_code, self._preprocess_db_shareholder_name(sql_data_name))
            sql_data_by_code_name.setdefault(sql_key, []).append(record)

        matched_sql_keys = set()

        for ai_data in ai_data_list:
            ai_name = self._preprocess_ai_shareholder_name(str(ai_data.get("股东名称", "")).strip())
            ai_key = (stock_code, self._preprocess_db_shareholder_name(ai_name))
            ai_type = str(ai_data.get("股东类别", "")).strip()
            ai_index = str(ai_data.get("股东序号", "")).strip()
            ai_percent = str(ai_data.get("持股比例", "")).strip()
            ai_num = str(ai_data.get("持股数量", "")).strip()

            if ai_key not in sql_data_by_code_name:
                results.append(
                    {
                        "股票代码": stock_code,
                        "公告发布日期": publish_date,
                        "信息发布日期": "",
                        "截止日期": "",
                        "股东名称": ai_name,
                        "股东类别": ai_type,
                        "股东序号": ai_index,
                        "持股比例": ai_percent,
                        "持股数量": ai_num,
                        "比对结果": "正式库无对应主键的记录",
                    }
                )
                continue

            matched_sql_keys.add(ai_key)
            for sql_record in sql_data_by_code_name[ai_key]:
                results.append(
                    {
                        "股票代码": stock_code,
                        "公告发布日期": publish_date,
                        "信息发布日期": str(sql_record.get("信息发布日期", "")).strip(),
                        "截止日期": str(sql_record.get("截止日期", "")).strip(),
                        "股东名称": str(sql_record.get("股东名称", "")).strip(),
                        "股东类别": ai_type,
                        "股东序号": str(sql_record.get("股东序号", "")).strip(),
                        "持股比例": str(sql_record.get("持股比例", "")).strip(),
                        "持股数量": ai_num,
                        "比对结果": self._compare_fields_with_format(ai_data, sql_record),
                    }
                )

        for sql_key, sql_records in sql_data_by_code_name.items():
            if sql_key in matched_sql_keys:
                continue
            for sql_record in sql_records:
                results.append(
                    {
                        "股票代码": stock_code,
                        "公告发布日期": publish_date,
                        "信息发布日期": str(sql_record.get("信息发布日期", "")).strip(),
                        "截止日期": str(sql_record.get("截止日期", "")).strip(),
                        "股东名称": str(sql_record.get("股东名称", "")).strip(),
                        "股东类别": "",
                        "股东序号": str(sql_record.get("股东序号", "")).strip(),
                        "持股比例": str(sql_record.get("持股比例", "")).strip(),
                        "持股数量": "",
                        "比对结果": "AI 无对应记录",
                    }
                )

        return results

    def _compare_fields_with_format(self, ai_data: Dict[str, Any], sql_data: Dict[str, Any]) -> str:
        """比较股东序号和持股比例字段。"""
        error_messages = []
        for field_name in ["股东序号", "持股比例"]:
            ai_value = self._preprocess_value(ai_data.get(field_name, ""))
            sql_value = self._preprocess_value(sql_data.get(field_name, ""))
            if not self._compare_values(ai_value, sql_value):
                error_messages.append(f"{field_name}错误【正式库: {sql_data.get(field_name, '')}，AI: {ai_data.get(field_name, '')}】")

        if not error_messages:
            return "数据一致"
        return "；".join(error_messages)

    def _preprocess_ai_shareholder_name(self, shareholder_name: str) -> str:
        """预处理 AI 返回的股东名称。"""
        if not shareholder_name:
            return ""

        processed_name = str(shareholder_name).strip()
        processed_name = convert(processed_name, "zh-hans")

        replacement_map = {
            "－": "-",
            "—": "-",
            "–": "-",
            "‒": "-",
            "（": "(",
            "）": ")",
            "*": "",
            "#": "",
        }
        for old_char, new_char in replacement_map.items():
            processed_name = processed_name.replace(old_char, new_char)

        processed_name = re.sub(r"([\u4e00-\u9fa5])\s+([\u4e00-\u9fa5])", r"\1\2", processed_name)
        processed_name = re.sub(r"([\u4e00-\u9fa5])\s+([()])", r"\1\2", processed_name)
        processed_name = re.sub(r"([()])\s+([\u4e00-\u9fa5])", r"\1\2", processed_name)
        processed_name = re.sub(r"([\u4e00-\u9fa5])\s+(\d)", r"\1\2", processed_name)
        processed_name = re.sub(r"(\d)\s+([\u4e00-\u9fa5])", r"\1\2", processed_name)
        processed_name = re.sub(r"注\s*\d*$", "", processed_name)
        processed_name = re.sub(r"\d+$", "", processed_name)
        return processed_name.strip()

    def _preprocess_db_shareholder_name(self, shareholder_name: str) -> str:
        """预处理数据库侧股东名称，用于主键比对。"""
        processed_name = str(shareholder_name).strip().lower()
        for old_char in [" ", ",", "."]:
            processed_name = processed_name.replace(old_char, "")
        return processed_name

    def _preprocess_value(self, value: Any) -> Any:
        """预处理数值与字符串字段。"""
        if value is None:
            return ""

        text = str(value).strip()
        if not text:
            return ""

        if self._is_numeric_value(text):
            text = text.replace(",", "")
            try:
                return float(text)
            except ValueError:
                return text
        return text

    def _is_numeric_value(self, value: str) -> bool:
        """判断字符串是否可视为数值。"""
        if not value:
            return False
        candidate = value.replace(",", "")
        has_digit = any(char.isdigit() for char in candidate)
        has_numeric_char = any(char in {".", "-", "+", "e", "E", "%"} for char in candidate)
        return has_digit and (has_numeric_char or candidate.isdigit())

    def load_prompt_from_md(self, md_file_path: str = "主要股东背景介绍一季报.md") -> str:
        """加载提示词文件内容。"""
        try:
            prompt_path = get_prompt_path(md_file_path)
            with open(prompt_path, "r", encoding="utf-8") as file:
                return file.read()
        except Exception as exc:
            logger.error("读取提示词文件时发生异常: %s", exc)
            return ""

    def generate_report(self, results: List[Dict[str, Any]], report_file: str = None) -> str:
        """生成 Excel 比对报告。"""
        if not results:
            print("没有可生成报告的数据")
            return ""

        reports_dir = get_reports_dir()
        if not report_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = os.path.join(reports_dir, f"主要股东背景介绍一季报比对报告_{timestamp}.xlsx")

        try:
            with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
                self._create_comparison_sheet(results, writer)
            print(f"报告已生成: {report_file}")
            return report_file
        except Exception as exc:
            print(f"生成报告失败: {exc}")
            return ""

    def _create_comparison_sheet(self, results: List[Dict[str, Any]], writer: pd.ExcelWriter):
        """创建比对结果工作表。"""
        comparison_data = []
        for result in results:
            for comparison in result.get("comparison_result", []):
                comparison_data.append(
                    {
                        "股票代码": comparison.get("股票代码", ""),
                        "公告发布日期": comparison.get("公告发布日期", ""),
                        "信息发布日期": comparison.get("信息发布日期", ""),
                        "截止日期": comparison.get("截止日期", ""),
                        "股东名称": comparison.get("股东名称", ""),
                        "股东类别(AI)": comparison.get("股东类别", ""),
                        "股东序号": comparison.get("股东序号", ""),
                        "持股比例": comparison.get("持股比例", ""),
                        "持股数量(AI)": comparison.get("持股数量", ""),
                        "比对结果": comparison.get("比对结果", ""),
                    }
                )

        df = pd.DataFrame(comparison_data)
        df.to_excel(writer, sheet_name="比对结果", index=False)

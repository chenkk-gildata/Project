"""
主要股东背景介绍一季报 PDF 切片处理模块。
逻辑按原“股东背景介绍一季报切片.py”封装为类接口。
"""
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import fitz
from PyPDF2 import PdfReader, PdfWriter


class PDFProcessor:
    """PDF 切片处理器。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.processed_count = 0
        self.success_count = 0
        self.completed_count = 0
        self.special_processed_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.is_processing = False

    def get_exchange_code(self, pdf_path: str) -> Optional[str]:
        """根据文件名前缀判断交易所类型。"""
        filename = os.path.basename(pdf_path)
        if len(filename) < 6:
            return None

        prefix = filename[:6]
        if prefix.startswith(("00", "30", "20")):
            return "szs"
        if prefix.startswith(("60", "90")):
            return "shs"
        if prefix.startswith("68"):
            return "kcb"
        if prefix.startswith(("92", "8", "4")):
            return "bjs"
        return None

    def find_keywords(self, pdf_path: str) -> Dict[str, Optional[Dict]]:
        """查找起止关键词及其位置。"""
        exchange_code = self.get_exchange_code(pdf_path)

        if exchange_code == "bjs":
            start_pattern = re.compile(
                r"^[（(]?[0-9一二三四五六七八九]*.*[）)]?[、.\-]?\s*报告期末的普通股股本结构、持股5%以上的股东或前十名股东情况\s*$",
                re.IGNORECASE | re.MULTILINE,
            )
            end_pattern = re.compile(
                r"^[（(]?[0-9一二三四五六七八九]*.*[）)]?[、.\-]?\s*存续至本期的优先股股票相关情况\s*$",
                re.IGNORECASE | re.MULTILINE,
            )
            start_keywords = ["前十名股东"]
            end_keywords = ["优先股"]
        else:
            start_pattern = re.compile(
                r"^[（(]?[0-9一二三四五六七八九]*.*[）)]?[、.\-]?\s*股东信息\s*$",
                re.IGNORECASE | re.MULTILINE,
            )
            end_pattern = re.compile(
                r"^[（(]?[0-9一二三四五六七八九]*.*[）)]?[、.\-]?\s*其他(?:重要|提醒)事项\s*$",
                re.IGNORECASE | re.MULTILINE,
            )
            start_keywords = ["股东信息"]
            end_keywords = ["事项"]

        def get_search_rect(page, inst, keyword_type: str):
            page_width = page.rect.width
            page_rotation = page.rotation
            if keyword_type in ["股东信息", "事项", "前十名股东", "优先股"]:
                if page_rotation == 90:
                    return fitz.Rect(inst.x0 - 30, 0, inst.x1 + 30, page_width)
                return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 30)
            return fitz.Rect(0, inst.y0 - 30, page_width, inst.y1 + 30)

        try:
            doc = fitz.open(pdf_path)
            start_info = None
            end_info = None

            total_pages = len(doc)
            actual_start = 1
            actual_end = min(20, total_pages)

            for page_num in range(actual_start - 1, actual_end):
                page = doc.load_page(page_num)
                page_rect = page.rect

                if not end_info:
                    end_candidates = []
                    for keyword in end_keywords:
                        for inst in page.search_for(keyword):
                            end_candidates.append((keyword, inst))
                    end_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    for keyword_type, inst in end_candidates:
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        if end_pattern.search(text):
                            end_info = {
                                "page_number": page_num + 1,
                                "keyword_box": (inst.x0, inst.y0, inst.x1, inst.y1),
                                "page_dimensions": (page_rect.width, page_rect.height),
                            }
                            break

                if not start_info:
                    start_candidates = []
                    for keyword in start_keywords:
                        for inst in page.search_for(keyword):
                            start_candidates.append((keyword, inst))
                    start_candidates.sort(key=lambda item: (item[1].y0, item[1].x0))

                    for keyword_type, inst in start_candidates:
                        rect = get_search_rect(page, inst, keyword_type)
                        text = page.get_text("text", clip=rect)
                        if start_pattern.search(text):
                            start_info = {
                                "page_number": page_num + 1,
                                "keyword_box": (inst.x0, inst.y0, inst.x1, inst.y1),
                                "page_dimensions": (page_rect.width, page_rect.height),
                            }
                            break

                if start_info and end_info:
                    break

            doc.close()
            return {"start": start_info, "end": end_info}
        except Exception:
            return {"start": None, "end": None}

    def crop_page_before_keyword(self, pdf_path: str, keyword_info: Dict, output_path: str) -> Optional[str]:
        """保留结束关键词所在页前部内容及前两页。"""
        try:
            reader = PdfReader(pdf_path)
            page = reader.pages[keyword_info["page_number"] - 1]

            page_rotation = page.rotation
            page_width, page_height = keyword_info["page_dimensions"]
            _, _, max_x, max_y = keyword_info["keyword_box"]

            if page_rotation == 90:
                pydf2_min_y = page_height - max_x
            else:
                pydf2_min_y = page_height - max_y

            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (max_x, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, pydf2_min_y)
                cropped_page.cropbox.upper_right = (page_width, page_height)

            writer = PdfWriter()
            start_index = max(keyword_info["page_number"] - 3, 0)
            for index in range(start_index, keyword_info["page_number"] - 1):
                writer.add_page(reader.pages[index])
            writer.add_page(cropped_page)

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            return output_path
        except Exception:
            return None

    def crop_page_after_keyword(self, pdf_path: str, keyword_info: Dict, output_path: str) -> Optional[str]:
        """保留开始关键词所在页后部内容及后两页。"""
        try:
            reader = PdfReader(pdf_path)
            page = reader.pages[keyword_info["page_number"] - 1]

            page_rotation = page.rotation
            page_width, page_height = keyword_info["page_dimensions"]
            min_x, min_y, _, _ = keyword_info["keyword_box"]
            pydf2_max_y = page_height - min_y

            cropped_page = page
            if page_rotation == 90:
                cropped_page.cropbox.lower_left = (min_x, 0)
                cropped_page.cropbox.upper_right = (page_height, page_width)
            else:
                cropped_page.cropbox.lower_left = (0, 0)
                cropped_page.cropbox.upper_right = (page_width, pydf2_max_y)

            writer = PdfWriter()
            writer.add_page(cropped_page)

            total_pages = len(reader.pages)
            for index in range(keyword_info["page_number"], min(keyword_info["page_number"] + 2, total_pages - 1) + 1):
                writer.add_page(reader.pages[index])

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            return output_path
        except Exception:
            return None

    def crop_same_page(self, pdf_path: str, start_info: Dict, end_info: Dict, output_path: str) -> Optional[str]:
        """在同一页中裁剪开始和结束关键词之间的内容。"""
        try:
            reader = PdfReader(pdf_path)
            page_num = start_info["page_number"] - 1
            page = reader.pages[page_num]

            page_width, page_height = start_info["page_dimensions"]
            _, start_min_y, _, _ = start_info["keyword_box"]
            _, _, _, end_max_y = end_info["keyword_box"]

            page_rotation = page.rotation
            start_pydf2_max_y = page_height - start_min_y
            end_pydf2_min_y = page_height - end_max_y

            cropped_page = page
            if page_rotation != 90:
                cropped_page.cropbox.lower_left = (0, end_pydf2_min_y)
                cropped_page.cropbox.upper_right = (page_width, start_pydf2_max_y)

            writer = PdfWriter()
            writer.add_page(cropped_page)
            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            return output_path
        except Exception:
            return None

    def process_standard_pdf(self, pdf_path: str, start_info: Dict, end_info: Dict, output_path: str) -> Optional[str]:
        """标准裁剪逻辑。"""
        try:
            reader = PdfReader(pdf_path)
            writer = PdfWriter()

            start_page = reader.pages[start_info["page_number"] - 1]
            start_rotation = start_page.rotation
            start_width, start_height = start_info["page_dimensions"]
            min_x, min_y, _, _ = start_info["keyword_box"]
            pydf2_max_y = start_height - min_y

            cropped_start_page = start_page
            if start_rotation == 90:
                cropped_start_page.cropbox.lower_left = (min_x, 0)
                cropped_start_page.cropbox.upper_right = (start_height, start_width)
            else:
                cropped_start_page.cropbox.lower_left = (0, 0)
                cropped_start_page.cropbox.upper_right = (start_width, pydf2_max_y)
            writer.add_page(cropped_start_page)

            for index in range(start_info["page_number"], end_info["page_number"] - 1):
                writer.add_page(reader.pages[index])

            end_page = reader.pages[end_info["page_number"] - 1]
            end_rotation = end_page.rotation
            end_width, end_height = end_info["page_dimensions"]
            _, _, max_x, max_y = end_info["keyword_box"]

            if end_rotation == 90:
                pydf2_min_y = end_height - max_x
            else:
                pydf2_min_y = end_height - max_y

            cropped_end_page = end_page
            if end_rotation == 90:
                cropped_end_page.cropbox.lower_left = (0, 0)
                cropped_end_page.cropbox.upper_right = (max_x, end_width)
            else:
                cropped_end_page.cropbox.lower_left = (0, pydf2_min_y)
                cropped_end_page.cropbox.upper_right = (end_width, end_height)
            writer.add_page(cropped_end_page)

            with open(output_path, "wb") as output_file:
                writer.write(output_file)

            return output_path
        except Exception:
            return None

    def process_gdbjjs_only(self, pdf_path: str, keyword_info: Dict, output_path: str) -> Tuple[str, Optional[str]]:
        """按原脚本逻辑处理股东背景介绍切片。"""
        start_info = keyword_info.get("start")
        end_info = keyword_info.get("end")

        if not start_info and not end_info:
            return "skipped", None
        if not start_info and end_info:
            result = self.crop_page_before_keyword(pdf_path, end_info, output_path)
            return ("special_processed", result) if result else ("failed", None)
        if start_info and not end_info:
            result = self.crop_page_after_keyword(pdf_path, start_info, output_path)
            return ("special_processed", result) if result else ("failed", None)

        if start_info["page_number"] > end_info["page_number"]:
            result = self.crop_page_before_keyword(pdf_path, end_info, output_path)
            return ("special_processed", result) if result else ("failed", None)

        if start_info["page_number"] == end_info["page_number"]:
            result = self.crop_same_page(pdf_path, start_info, end_info, output_path)
            return ("special_processed", result) if result else ("failed", None)

        result = self.process_standard_pdf(pdf_path, start_info, end_info, output_path)
        return ("completed", result) if result else ("failed", None)

    def process_single_pdf(self, pdf_path: str) -> str:
        """处理单个 PDF，成功时覆盖原文件。"""
        if not self.is_processing:
            return "failed"

        try:
            exchange_code = self.get_exchange_code(pdf_path)
            if exchange_code == "kcb":
                return "skipped"

            keywords = self.find_keywords(pdf_path)
            temp_output_path = pdf_path + ".temp"
            status, result_path = self.process_gdbjjs_only(pdf_path, keywords, temp_output_path)

            if status in {"completed", "special_processed"} and result_path and os.path.exists(temp_output_path):
                os.remove(pdf_path)
                os.rename(temp_output_path, pdf_path)
                return status

            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)
            return status
        except Exception:
            return "failed"

    def process_batch(self, pdf_files: List[str]) -> Tuple[int, int]:
        """批量处理 PDF 文件。"""
        if not pdf_files:
            return 0, 0

        self.is_processing = True
        self.processed_count = 0
        self.success_count = 0
        self.completed_count = 0
        self.special_processed_count = 0
        self.skipped_count = 0
        self.failed_count = 0

        total_count = len(pdf_files)
        workers = min(8, total_count) if total_count > 10 else min(4, total_count)
        print(f"\n开始处理 {total_count} 个 PDF 文件，使用 {workers} 个并发线程...")

        executor = ThreadPoolExecutor(max_workers=workers)
        try:
            futures = {executor.submit(self.process_single_pdf, pdf_file): pdf_file for pdf_file in pdf_files}
            for future in as_completed(futures):
                if not self.is_processing:
                    break
                try:
                    status = future.result()
                except Exception:
                    status = "failed"

                with self.lock:
                    self.processed_count += 1
                    if status == "completed":
                        self.completed_count += 1
                    elif status == "special_processed":
                        self.special_processed_count += 1
                    elif status == "skipped":
                        self.skipped_count += 1
                    else:
                        self.failed_count += 1
        finally:
            executor.shutdown(wait=True)
            self.is_processing = False

        self.success_count = self.completed_count + self.special_processed_count
        print("\nPDF 切片处理完成!")
        print(f"处理完成: {self.completed_count}")
        print(f"特殊处理: {self.special_processed_count}")
        print(f"跳过处理: {self.skipped_count}")
        print(f"处理失败: {self.failed_count}")
        return self.success_count, self.failed_count

    def stop_processing(self):
        """停止处理。"""
        self.is_processing = False
